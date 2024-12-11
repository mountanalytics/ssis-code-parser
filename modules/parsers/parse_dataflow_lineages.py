import json
import pandas as pd
import re
import numpy as np

# sort nodes
def order_df(nodes: pd.DataFrame) -> list[list]:
    def explore_paths(current_node, current_path):
        while True:
            current_row = nodes[nodes['ID_block_out'] == current_node]
            current_path.append(current_row)
            
            # If there are multiple branches, explore each one
            if len(current_row) > 1:
                paths = []
                for i in range(len(current_row)):
                    new_path = current_path.copy()
                    next_node = current_row.iloc[i]['ID_block_in']
                    
                    if not next_node or not any(nodes['ID_block_out'] == next_node):
                        paths.append(new_path)
                    else:
                        paths.extend(explore_paths(next_node, new_path))
                
                return paths
            
            # If there's only one row, continue normally
            elif len(current_row) == 1:
                next_node = current_row.iloc[0]['ID_block_in']
                
                if not next_node or not any(nodes['ID_block_out'] == next_node):
                    break
                
                current_node = next_node
            else:
                # No more rows to process, end the loop
                break
        
        return [current_path]

    # Initialize the list to collect all paths
    all_paths = []
    
    # Find the starting nodes (those that do not appear in ID_block_in)
    start_nodes = nodes[~nodes['ID_block_out'].isin(nodes['ID_block_in'])]['ID_block_out'].tolist()
    
    # Explore paths starting from each start node
    for start_node in start_nodes:
        all_paths.extend(explore_paths(start_node, []))
    
    return all_paths

            
# add columns data to sorted nodes
def add_metadata(sorted_nodes:list, dict_blocks: dict) -> list:
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


def order_nodes(nodes: pd.DataFrame) -> list[pd.DataFrame]:
    all_paths = order_df(nodes)
    for i,list1 in enumerate(all_paths):
        for j,list2 in enumerate(all_paths):
            filtered_list = [df2 for df2 in list2 if any(df2.equals(df1) for df1 in list1)]
            if len(filtered_list) == len(list1) and list1 != list2:
                all_paths.pop(i)
            elif len(filtered_list) == len(list2) and list1 != list2:
                all_paths.pop(j)
    return all_paths


def main_parser(nodes: pd.DataFrame, all_paths: list[pd.DataFrame], dict_blocks: dict, nodes_load: pd.DataFrame, df_name:str) -> pd.DataFrame:
    """
    Function that orchestrates the parsing of the lineages of a dataflow
    """

    final_lin = pd.DataFrame()
    cols_splits = {}
    for ordered_rows in all_paths:
        # Convert the specified columns into a list of lists and extend the final list
        ordered_df = pd.concat(ordered_rows).reset_index(drop=True)
        sorted_nodes = ordered_df[['ID_block_out', 'ID_block_in']].values.tolist()   
        sorted_nodes_metadata = add_metadata(sorted_nodes,dict_blocks)
        columns_input = []
        columns_output = []
        seen_nodes = []
        convert_marker= []
        previous_node = None
        exp_created = []
        
        for nodes_pair in sorted_nodes_metadata:
            for i, node in enumerate(nodes_pair):
                
                # define previous node
                if i == 0:
                    previous_node = node
    
                # define the type of node
                if i == 1:
                    type = nodes.loc[nodes['ID_block_in'] == node, 'type_block_in'].iloc[0]
                if i == 0:
                    type = nodes.loc[nodes['ID_block_out'] == node, 'type_block_out'].iloc[0]
    
    
                if previous_node:
                    # if node pair was already seen then skip
                    if [previous_node, node] in seen_nodes:
                        continue
                    # if the previous node is the same as the current node and the node type is not source then skip
                    if previous_node == node and type != 'Microsoft.SSISODBCSrc':
                        continue
                    
                    seen_nodes.append([previous_node, node])
                if previous_node in cols_splits.keys():
                    columns = cols_splits[previous_node]
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
                    expression_columns = [d['column'] for d in nodes_pair[node]]
    
                    columns_in =[]
                    columns_out =[]

                    for idx, expression_column in enumerate(expression_columns):
                        matched = False
                        for column in columns:
                            if column == expression_column:
                                derived_column = column
                                columns_in.append(previous_node + '[' + column+']')
                                columns_out.append(node + '[' + column+']' + " {"+ expression[idx] +"}")
                                matched = True
                        if not matched:
                            exp_created.append(node + '[' + expression_column + ']' + " {" + expression[idx] + "}")        
                              
                    columns_in += [previous_node + '[' + column+']' for column in columns if column != derived_column]
                    columns_out += [node +'[' +column+']'  for column in columns if column != derived_column]
        
                    columns_input +=columns_in
                    columns_output +=columns_out

                # process rowcount
                if type == 'Microsoft.RowCount':
    
                    columns_in =[previous_node + '[' + column+']' for column in columns]
                    columns_out=[node +'[' +column+']' for column in columns]
    
                    variable = nodes_pair[node]
    
                    columns_in.append(node +'['+"Row Count"+']')
                    columns_out.append(variable +'['+variable+']')
    
                    columns_input +=columns_in
                    columns_output +=columns_out
                    
                # process conditionalsplit
                if type == 'Microsoft.ConditionalSplit':
                    columns_in=[previous_node + '[' + column+']' for column in columns]
                    columns_out=[node +'[' +column+']' for column in columns]
    
                    columns_input +=columns_in
                    columns_output +=columns_out
                    if node not in cols_splits.keys():
                        cols_splits[node] = columns         
    
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
                    # second to last node unto last (destination)
                    columns_in =[previous_node + '[' + d['Column_name']+']' for d in nodes_pair[node]]
                    columns_out=[node +'[' +d['Column_name']+']' for d in nodes_pair[node]]
    
                    columns_input +=columns_in
                    columns_output +=columns_out
    
                    # add last node to destination table lineages
                    columns_in =[node + '[' + d['Column_name']+']' for d in nodes_pair[node]]
                    columns_out=[".".join(d['Column_ext'].split('.')[:-1]) +'['+ d['Column_ext'].split('.')[-1] +']' for d in nodes_pair[node]]
    
                    columns_input +=columns_in
                    columns_output +=columns_out

                if type == "Microsoft.DataConvert":
                    
                    columns_in=[previous_node + '[' + column+']' for column in columns]
                    columns_out=[node +'[' +column+']' for column in columns]
                    for cols in dict_blocks[node]:
                        columns.append(cols["output_column"])
                        arg = f'col: {cols["input_column"]}, from: {cols["input_type"]}, to: {cols["output_type"]}'
                        convert_marker.append({"col_name" :node + '['+cols["output_column"]+']', "arg" : arg})
                    columns_input +=columns_in
                    columns_output +=columns_out

                if type == "Microsoft.Sort":
                    columns_in=[previous_node + '[' + column+']' for column in columns]
                    columns_out=[node +'[' +column+']' for column in columns]
    
                    columns_input +=columns_in
                    columns_output +=columns_out

                if type == "Microsoft.MergeJoin":
                    columns_in = [previous_node + '[' + column+']' for column in columns]
                    columns_out = [node +'[' +column+']' for column in columns]
                    columns_input += columns_in
                    columns_output += columns_out
                    
                    columns = []
                    for col_map in dict_blocks[node]["col_mapping"]:
                        columns.append(col_map[1])

                if type == "Microsoft.Multicast":
                    columns_in=[previous_node + '[' + column+']' for column in columns]
                    columns_out=[node +'[' +column+']' for column in columns]
                    
                    columns_input +=columns_in
                    columns_output +=columns_out
                    if node not in cols_splits.keys():
                        cols_splits[node] = columns

                if type == "Microsoft.UnPivot":
                    columns_in=[previous_node + '[' + column+']' for column in columns]
                    columns_out = [node +'[' +column+']' for column in columns]
                    columns = []
                    
                    for cols in dict_blocks[node]['column_mapping']:
                        columns.append(cols[1])
                    columns_input +=columns_in
                    columns_output +=columns_out
                
                if type == "Microsoft.Aggregate":
                    columns_in=[previous_node + '[' + column+']' for column in columns]
                    columns_out = [node +'[' +column+']' for column in columns]
                    columns = []
                    for cols in dict_blocks[node].keys():
                        columns.append(cols)
                    columns_input +=columns_in
                    columns_output +=columns_out

        # create lineages dataframe and save csv
        lineages = {'SOURCE_COLUMNS': columns_input, 'TARGET_COLUMN': columns_output}
    
        lineages = pd.DataFrame(lineages)
        exp_dict = {exp.split(" {")[0]: exp for exp in exp_created}
        lineages['SOURCE_COLUMNS'] = lineages['SOURCE_COLUMNS'].replace(exp_dict)
        transformation_in = lineages['SOURCE_COLUMNS'].str.extract(r'\{([^}]*)\}')

        lineages['TRANSFORMATION'] = lineages['TARGET_COLUMN'].str.extract(r'\{([^}]*)\}')
        lineages['SOURCE_FIELD'] = lineages['SOURCE_COLUMNS'].str.extract(r'\[([^\]]*)\]')
        lineages['TARGET_FIELD'] = lineages['TARGET_COLUMN'].str.extract(r'\[([^\]]*)\]')
        lineages['SOURCE_NODE'] =lineages['SOURCE_COLUMNS'].apply(lambda x: "@".join(x.split('[')[0].split('\\')[1:]) if "\\" in x else x.split('[')[0])#.str.extract(r'^(.*?)\s*\[.*\]$')
        lineages['TARGET_NODE'] =lineages['TARGET_COLUMN'].apply(lambda x: "@".join(x.split('[')[0].split('\\')[1:]) if "\\" in x else x.split('[')[0])
        lineages['LINK_VALUE'] = 1
        lineages['ROW_ID'] = lineages.index
        lineages.fillna("", inplace=True)
        
        # move transformation to successive node
        name_node = None

        for i in range(len(lineages) - 1):
            if lineages.loc[i, 'TRANSFORMATION']:   
                transformation = lineages.loc[i, 'TRANSFORMATION']
                name_node = lineages.loc[i, 'TARGET_COLUMN']
                lineages.loc[i, 'TRANSFORMATION'] = ""
                continue
            if name_node:
                if lineages.loc[i, 'SOURCE_COLUMNS'] == name_node.split(" {")[0]:
                    lineages.loc[i, 'TRANSFORMATION'] = transformation

        lineages['TRANSFORMATION'] = transformation_in[0].combine_first(lineages['TRANSFORMATION'])

        # define color lineages
        lineages['COLOR'] = ["aliceblue" if i == ""  else "#ffb480" for i in lineages['TRANSFORMATION']]
        non_null_indices = transformation_in[0].notna().index[transformation_in[0].notna()]
        lineages.loc[non_null_indices, 'COLOR'] = '#ff6961'
        

        for cols in convert_marker:
            matching_indices = lineages.index[lineages["SOURCE_COLUMNS"] == cols["col_name"]].tolist()
            lineages.loc[matching_indices, "TRANSFORMATION"] = cols["arg"]
            lineages.loc[matching_indices, "COLOR"] = "#ffb480"


        # merge source node id
        lineages = pd.merge(lineages, nodes_load[['ID', 'LABEL_NODE']], left_on='SOURCE_NODE', right_on = 'LABEL_NODE', how='left')
        lineages['SOURCE_NODE'] = lineages['ID']
        lineages.drop(columns=['ID', 'LABEL_NODE'], inplace=True)
    
        # merge target node id
        lineages = pd.merge(lineages, nodes_load[['ID', 'LABEL_NODE']], left_on='TARGET_NODE', right_on = 'LABEL_NODE', how='left')
        lineages['TARGET_NODE'] = lineages['ID']
        lineages.drop(columns=['ID', 'LABEL_NODE'], inplace=True)
        
        # load nodes data
        final_lin = pd.concat([final_lin,lineages], ignore_index=True)

    df_no_duplicates = final_lin.drop_duplicates(subset=["SOURCE_COLUMNS","TARGET_COLUMN", "TRANSFORMATION", "SOURCE_FIELD", "TARGET_FIELD", "SOURCE_NODE", "TARGET_NODE"])
    df_no_duplicates.to_csv(f'output-data/lineages/lineage-{df_name}.csv')
    return df_no_duplicates


def parser_dataflow_lineages(df_name: str, nodes_df: pd.DataFrame, nodes_order_df: pd.DataFrame, metadata_df: dict) -> pd.DataFrame:

    all_paths = order_nodes(nodes_order_df) 
    lineages = main_parser(nodes_order_df, all_paths, metadata_df, nodes_df, df_name)
    return lineages


if __name__ == "__main__":
    parser_dataflow_lineages('Package@Merge and filter')
    

    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    