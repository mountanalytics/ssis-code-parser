import os
import pandas as pd


def replace_after_last_dot(s):
    if '.' in str(s):
        return str(s).split('.')[-1]
    return s


def update_datasets(nodes: pd.DataFrame, lineages:pd.DataFrame, file_name:str, node_final:pd.DataFrame) -> pd.DataFrame:
    """
    This function updates the lineages dataset for a control/data flow based on the new values of the concatenated nodes.
    It takes as input the nodes and lineages of a flow, its' name and the concatenated nodes dataframe.
    """

    mask = nodes['FUNCTION'].isin(['DataSources', 'DataDestinations']) # Apply the transformation conditionally
    
    nodes.loc[mask, 'LABEL_NODE'] = nodes.loc[mask, 'LABEL_NODE'].apply(replace_after_last_dot)
    nodes.loc[mask, 'NAME_NODE'] = nodes.loc[mask, 'NAME_NODE'].apply(replace_after_last_dot)

    merged = nodes.merge(node_final[['NAME_NODE', 'LABEL_NODE']], 
                    on=['NAME_NODE', 'LABEL_NODE'], 
                    how='left', 
                    indicator=True)
    
    merged_ind = node_final.merge(nodes[['NAME_NODE', 'LABEL_NODE']], 
                    on=['NAME_NODE', 'LABEL_NODE'], 
                    how='left', 
                    indicator='merge_status')
    
    # Filtering the rows that are present in both dataframes
    duplicates = merged[merged['_merge'] == 'both']
    dups = merged_ind[merged_ind['merge_status'] == 'both']
    dups = dups.rename(columns={'ID': 'NEW_ID'})
    duplicates = duplicates.merge(dups[['NEW_ID', 'LABEL_NODE']], 
                    on=['LABEL_NODE'], 
                    how='left')
    new_rows = merged[merged['_merge'] == 'left_only']

    node_final = pd.concat([node_final,new_rows],ignore_index=True)
    node_final['ID'] = node_final.index

    new_rows =  new_rows.assign(NEW_ID=range(len(node_final) - len(new_rows), len(node_final)))
    new_nodes = pd.concat([new_rows,duplicates],ignore_index=True)
    id_to_new_id = new_nodes.set_index('ID')['NEW_ID'].to_dict()

    lineages['SOURCE_NODE'] = lineages['SOURCE_NODE'].map(id_to_new_id)
    lineages['TARGET_NODE'] = lineages['TARGET_NODE'].map(id_to_new_id)
    csv_files = [f for f in os.listdir("output-data/lineages") if f.endswith('.csv')]

    for csv_file in csv_files:
    # Check if the csv_file name matches the file_name
        if file_name in csv_file:
            # Check if the file name ends with '_cf'
            if csv_file.endswith('_cf.csv'):
                lineages.to_csv(f"output-data/lineages/lineage-{file_name}_cf.csv", index=False)
            else:
                lineages.to_csv(f"output-data/lineages/lineage-{file_name}.csv", index=False)

    return node_final
    

def node_lin_pars(flows:dict) -> pd.DataFrame:
    """
    This function goes through the flows dictionary and updates the data of the nodes and lineages based on the values
    arose from the merging of the nodes
    """
    
    node_final = pd.DataFrame(columns=['LABEL_NODE', 'ID', 'FUNCTION', 'JOIN_ARG', 'SPLIT_ARG', 'NAME_NODE',
           'FILTER', 'COLOR'])

    for file_name in flows.keys():
        # upadate control flow 
        lineages = pd.DataFrame(flows[file_name]['control_flow']['lineages'])
        nodes = pd.DataFrame(flows[file_name]['control_flow']['nodes'])
        node_final = update_datasets(nodes, lineages, file_name, node_final)

        # update data flow
        for flow in flows[file_name]['data_flow']:                                                                                  
            lineages = pd.DataFrame(flows[file_name]['data_flow'][flow]['lineages'])
            nodes = pd.DataFrame(flows[file_name]['data_flow'][flow]['nodes'])
            node_final = update_datasets(nodes, lineages, flow, node_final)

    node_final.to_csv("output-data/nodes.csv", index = False)
    return node_final