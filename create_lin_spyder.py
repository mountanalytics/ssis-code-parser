import json
import pandas as pd
lineages = pd.read_csv('nodes.csv')

def order_df(lineages: pd.DataFrame) -> list[list]:
    
    start_node = lineages[~lineages['ID_block_out'].isin(lineages['ID_block_in'])]
    ordered_rows = []
    current_node = start_node.iloc[0]['ID_block_out']
    
    while True:
        current_row = lineages[lineages['ID_block_out'] == current_node]
        ordered_rows.append(current_row)
        if current_row.empty or not any(lineages['ID_block_out'].isin(current_row['ID_block_in'])):
            break
        current_node = current_row.iloc[0]['ID_block_in']
        
    ordered_df = pd.concat(ordered_rows).reset_index(drop=True)
    list_of_lists = ordered_df[['ID_block_out', 'ID_block_in']].values.tolist()
    return list_of_lists


with open('dict_blocks.json', 'r') as json_file:
    dict_blocks = json.load(json_file)
lineages = lineages.drop(columns=[col for col in lineages .columns if not col or col.startswith('Unnamed')])
"""
sorted_nodes = []
for i, node in enumerate(lineages['ID_block_out']):
    if node not in list(lineages['ID_block_in']):
        sorted_nodes.append([lineages['ID_block_out'][i], lineages['ID_block_in'][i]])



for _ in lineages['ID_block_out']:
    for i, node in enumerate(lineages['ID_block_out']):
        if sorted_nodes[-1][1] == node:
            sorted_nodes.append([lineages['ID_block_out'][i], lineages['ID_block_in'][i]])
"""
sorted_nodes = order_df(lineages)        
sorted_nodes_metadata = []

for i in sorted_nodes:

    dictionary = {}

    dictionary[i[0]] = dict_blocks[i[0]]
    dictionary[i[1]] = dict_blocks[i[1]]

    sorted_nodes_metadata.append(dictionary)