# MapReduce of the Page Rank Algorithm

from mrjob.job import MRJob

# Data :
# a webpage = a node = {
# 'node_id' : id
# 'links' : list_of_links_in_the_webpage
# 'score' : initialized at 1 then PR calculated
# }

# The node_id and the links in node['links'] can be seen as the URL of the page

class CreateGraph(MRJob):
    
    def map_task(self, page_id, page):
        # For each link in a page we yield the link with the key page_id
        for link in page:
            yield(page_id, link) # (key, value)
    
    def reduce_task(self, page_id, links):
        node = {}
        links_list = []
    # In the reduce task we feed the node object:
    # We will get a node object with an id (the initial page id) and all the 
    # the links inside this page
        for link in links:
            links_list += [link]
            
        node['links'] = links_list
        node['node_id'] = page_id
        
        yield(page_id, node)
                


class MRPageRank(MRJob):

    def map_task(self, node_id, node):
        # First we send the node object
        yield (node_id, ('node', node))
        if 'links' in node.keys():
            weight = 1 / len(node.get('links'))
            for link in node.get('links'):
                # Then we also send the score of link related to node_id
                yield(link, ('score', node['score'] * weight))
                
                
    def reduce_task(self, node_id, type_and_value):
        
        node = {}
        total_score = 0
        previous_score_set = False
        
        # After the shuffle and sort by key, we are supposed to get for each 
        # node (or link) one node object and a list of scores
        
        for type_value, value in type_and_value:
            if type_value == 'node':
                # Here we get the node object
                node = value
                
                if previous_score_set == False:
                    node['prev_score'] = node['score']
                    previous_score_set = True
            
            elif type_value == 'score':
                # Here we get the score values and compute the Page Range score
                total_score += value
        
            else:
                raise Exception('Issue')
                
            d = 0.8
            node['score'] = 1 - d + d * total_score
            # We update the score of the node(link)
            
            yield(node_id, node)
            
        
        