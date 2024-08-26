import json
import pandas as pd
import re

# load nodes data
nodes = pd.read_csv('nodes.csv')
nodes

with open('dict_blocks.json', 'r') as json_file:
    dict_blocks = json.load(json_file)

dict_blocks.keys()


# sort nodes
def order_df(nodes: pd.DataFrame) -> list[list]:
    
    start_node = nodes[~nodes['ID_block_out'].isin(nodes['ID_block_in'])]
    ordered_rows = []
    current_node = start_node.iloc[0]['ID_block_out']
    
    while True:
        current_row = nodes[nodes['ID_block_out'] == current_node]
        ordered_rows.append(current_row)
        if current_row.empty or not any(nodes['ID_block_out'].isin(current_row['ID_block_in'])):
            break
        current_node = current_row.iloc[0]['ID_block_in']
        
    ordered_df = pd.concat(ordered_rows).reset_index(drop=True)
    list_of_lists = ordered_df[['ID_block_out', 'ID_block_in']].values.tolist()
    return list_of_lists

            
sorted_nodes = order_df(nodes)


# add columns data to sorted nodes
def add_metadata(sorted_nodes:list) -> list:
    sorted_nodes_metadata = []

    for i in sorted_nodes:
        dictionary = {}

        dictionary[i[0]] = dict_blocks[i[0]]
        dictionary[i[1]] = dict_blocks[i[1]]

        sorted_nodes_metadata.append(dictionary)
        
    return sorted_nodes_metadata

sorted_nodes_metadata = add_metadata(sorted_nodes)


def extract_column(text):
    match = re.search(r'\[([^\]]*)\]', text)
    return match.group(1) if match else ''