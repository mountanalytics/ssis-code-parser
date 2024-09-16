import os
import pandas as pd

def replace_after_last_dot(s):
    if '.' in s:
        return s.split('.')[-1]
    return s

def node_lin_pars():
    node_final = pd.DataFrame(columns=['LABEL_NODE', 'ID', 'FUNCTION', 'JOIN_ARG', 'SPLIT_ARG', 'NAME_NODE',
           'FILTER', 'COLOR'])
    #lin_final = pd.DataFrame(columns=['SOURCE_COLUMNS', 'TARGET_COLUMN', 'TRANSFORMATION',
    #       'SOURCE_FIELD', 'TARGET_FIELD', 'SOURCE_NODE', 'TARGET_NODE',
    #       'LINK_VALUE', 'ROW_ID', 'COLOR'])
    for files in os.listdir("output-data/lineages"):
        if files.endswith(".csv"):
            name = files.replace(".csv", "").replace("lineage-","")
            node = pd.read_csv(f"output-data/nodes/nodes-{name}.csv")
            
            # Apply the transformation conditionally
            mask = node['FUNCTION'].isin(['DataSources', 'DataDestinations'])
            
            node.loc[mask, 'LABEL_NODE'] = node.loc[mask, 'LABEL_NODE'].apply(replace_after_last_dot)
            node.loc[mask, 'NAME_NODE'] = node.loc[mask, 'NAME_NODE'].apply(replace_after_last_dot)
            merged = node.merge(node_final[['NAME_NODE', 'LABEL_NODE']], 
                            on=['NAME_NODE', 'LABEL_NODE'], 
                            how='left', 
                            indicator=True)
            merged_ind = node_final.merge(node[['NAME_NODE', 'LABEL_NODE']], 
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
            lineage = pd.read_csv(f"output-data/lineages/lineage-{name}.csv")
            lineage['SOURCE_NODE'] = lineage['SOURCE_NODE'].map(id_to_new_id)
            lineage['TARGET_NODE'] = lineage['TARGET_NODE'].map(id_to_new_id)
            #lin_final = pd.concat([lin_final,lineage],ignore_index=True)
            lineage.to_csv(f"output-data/lineages/lineage-{name}.csv", index = False)
            #lin_final.to_csv("output-data/lineages/lineage-Complete.csv", index = False)
    node_final.to_csv("output-data/nodes.csv", index = False)
    return