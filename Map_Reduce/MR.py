# MapReduce of the Page Rank Algorithm

from mrjob.job import MRJob

# Data :
# a webpage = a node = {
# 'node_id' : id
# 'links' : list_of_links_in_the_webpage
# 'score' : initialized at 1 then PR calculated
# }

class CreateGraph(MRJob):
    
    def map_task(self, page_id, page):
        for link in page:
            yield(page_id, link)
    
    def reduce_task(self, page_id, links):
        node = {}
        links_list = []
        
        for link in links:
            links_list += [link]
            
        node['links'] = links_list
        node['node_id'] = page_id
        
        yield(page_id, node)
                


class MRPageRank(MRJob):

    def map_task(self, node_id, node):
        yield (node_id, ('node', node))
        if 'links' in node.keys():
            weight = 1 / len(node.get('links'))
            for link in node.get('links'):
            # for link, weight in node.get('links'):
                yield(link, ('score', node['score'] * weight))
                
                
    def reduce_task(self, node_id, type_and_value):
        
        node = {}
        total_score = 0
        previous_score_set = False
        
        for type_value, value in type_and_value:
            if type_value == 'node':
                node = value
                
                if previous_score_set == False:
                    node['prev_score'] = node['score']
                    previous_score_set = True
            
            elif type_value == 'score':
                total_score += value
        
            else:
                raise Exception('Issue')
                
            d = 0.8
            node['score'] = 1 - d + d * total_score
            # node['score'] = node['prev_score'] * d + 1 - d
            
            yield(node_id, node)
            
        
        