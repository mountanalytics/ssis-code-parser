import json
import pandas as pd
import re


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

            


# add columns data to sorted nodes
def add_metadata(sorted_nodes:list) -> list:
    sorted_nodes_metadata = []

    for i in sorted_nodes:
        dictionary = {}

        dictionary[i[0]] = dict_blocks[i[0]]
        dictionary[i[1]] = dict_blocks[i[1]]

        sorted_nodes_metadata.append(dictionary)
        
    return sorted_nodes_metadata



# extract column from between []
def extract_column(text:str)->str:
    match = re.search(r'\[([^\]]*)\]', text)
    return match.group(1) if match else ''


if __name__ == "__main__":

    # load nodes data
    nodes = pd.read_csv('nodes.csv') # nodes

    with open('dict_blocks.json', 'r') as json_file: # columns data
        dict_blocks = json.load(json_file)


    # order nodes and add metadata
    sorted_nodes = order_df(nodes)
    sorted_nodes_metadata = add_metadata(sorted_nodes)

    columns_input = []
    columns_output = []
    seen_nodes = []
    previous_node = None

    for nodes_pair in sorted_nodes_metadata:
        for i, node in enumerate(nodes_pair):

            # define previous node
            if i == 0:
                previous_node = node

            # define the type of node
            if i == 1:
                type = nodes.loc[nodes['ID_block_in'] == node, 'type_block_in'].iloc[0]#.item()
            if i == 0:
                type = nodes.loc[nodes['ID_block_out'] == node, 'type_block_out'].iloc[0]#.item()


            if previous_node:
                # if node pair was already seen then skip
                if [previous_node, node] in seen_nodes:
                    continue
                # if the previous node is the same as the current node and the node type is not source then skip
                if previous_node == node and type != 'Microsoft.SSISODBCSrc':
                    continue
                print([previous_node, node])
                seen_nodes.append([previous_node, node])

            print(type)


            # process source node
            if type == 'Microsoft.SSISODBCSrc':
                columns=[d['Column_name'] for d in nodes_pair[node]]

                columns_in = [d['Column_input'] for d in nodes_pair[node]]
                columns_out = [node +'[' + d['Column_name']  +']' for d in nodes_pair[node]]

                columns_input += columns_in
                columns_output += columns_out

            # process lookup node
            if type == 'Microsoft.Lookup':

                columns_in =[d['Column_lookup'] for d in nodes_pair[node]['merged_columns']] + [previous_node + '[' +column+']' for column in columns]
                columns_out=[node +'[' +d['Column_name']+']' for d in nodes_pair[node]['merged_columns']]+ [node + '[' +column+']' for column in columns]

                columns_input +=columns_in
                columns_output +=columns_out
                columns += [d['Column_name'] for d in nodes_pair[node]['merged_columns']]

            # process derivedcolumn node
            if type == 'Microsoft.DerivedColumn':
                
                expression = [d['expression'] for d in nodes_pair[node]]
                expression_columns = [d['Column_name'] for d in nodes_pair[node]]

                column_in =[]
                column_out =[]

                for column in columns:
                    for idx, expression_column in enumerate(expression_columns):
                        if column == expression_column:

                            columns_in.append(previous_node + '[' + column+']')
                            columns_out.append(node + '[' + column+']' + " {"+ expression[idx] +"}")
                        else:
                            columns_in.append(previous_node + '[' + column+']')
                            columns_out.append(node + '[' + column+']')


                #columns_in = [previous_node + '[' + column+']' for column in columns]
                #columns_out = [node +'[' +column+']' for column in columns]

                columns_input +=columns_in
                columns_output +=columns_out

            # process rowcount
            if type == 'Microsoft.RowCount':

                columns_in =[previous_node + '[' + column+']' for column in columns]
                columns_out=[node +'[' +column+']' for column in columns]

                variable = nodes_pair[node]


                columns_input +=columns_in
                columns_output +=columns_out
                
            # process conditionalsplit
            if type == 'Microsoft.ConditionalSplit':
                columns_in=[previous_node + '[' + column+']' for column in columns]
                columns_out=[node +'[' +column+']' for column in columns]

                columns_input +=columns_in
                columns_output +=columns_out          

            # process unionall
            if type == 'Microsoft.UnionAll':
                columns_in=[d['Column_input'] for d in nodes_pair[node] if previous_node in d['Column_input']] # add the column that are mentioned in the previous node, otherwise the same column is added two times
                columns_out=[d['Column_name'] for d in nodes_pair[node]]

                columns_out = [node +'[' +n  +']' for n in set(columns_out)]

                columns_in = sorted(columns_in, key=extract_column)
                columns_out = sorted(columns_out, key=extract_column)


                columns_input +=columns_in
                columns_output +=columns_out
                

            # process destination
            if type == 'Microsoft.SSISODBCDst':
                columns_in =[previous_node + '[' + d['Column_name']+']' for d in nodes_pair[node]]
                columns_out=[node +'[' +d['Column_name']+']' for d in nodes_pair[node]]

                columns_input +=columns_in
                columns_output +=columns_out

            print(previous_node)
            print()

    # create dataframe and save 
    lineages = {'column_in': columns_input, 'column_out': columns_output}

    lineages = pd.DataFrame(lineages)

    lineages['expression'] = lineages['column_out'].str.extract(r'\{([^}]*)\}')

    lineages['col_name_in'] = lineages['column_in'].str.extract(r'\[([^\]]*)\]')
    lineages['col_name_out'] = lineages['column_out'].str.extract(r'\[([^\]]*)\]')

    lineages['node_in'] =lineages['column_in'].apply(lambda x: x.split('[')[0])#.str.extract(r'^(.*?)\s*\[.*\]$')
    lineages['node_out'] =lineages['column_out'].apply(lambda x: x.split('[')[0])#.str.extract(r'^(.*?)\s*\[.*\]$')


    lineages.to_csv('lineages.csv')