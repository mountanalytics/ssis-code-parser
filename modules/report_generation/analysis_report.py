import json
import pandas as pd


def control_blocks(dict_blocks: dict) -> pd.Series:
    blocks_count = []
    for key in dict_blocks:
        blocks_count.append(dict_blocks[key]["Description"])
    count_control_flow = pd.Series(blocks_count).value_counts().rename_axis("FUNCTION")
    return count_control_flow


def dataflow_blocks(lineages: pd.DataFrame, nodes: pd.DataFrame) -> pd.Series:
    unique_nodes_df = pd.concat([lineages['SOURCE_NODE'],lineages['TARGET_NODE']],ignore_index=True).drop_duplicates()
    nodes = nodes[nodes['ID'].isin(unique_nodes_df)]
    
    blocks_df = nodes['FUNCTION'].value_counts() 
    return blocks_df


def source_target_tables(lineages: pd.DataFrame, nodes: pd.DataFrame) -> tuple[pd.DataFrame,pd.DataFrame]:
    unique_combinations = lineages[['SOURCE_NODE', 'TARGET_NODE']].drop_duplicates()
    
    # Merge to get SOURCE_FUNC
    unique_combinations = unique_combinations.merge(
        nodes[['ID', 'FUNCTION', "LABEL_NODE"]], 
        how='left', 
        left_on='SOURCE_NODE', 
        right_on='ID'
    ).rename(columns={'FUNCTION': 'SOURCE_FUNC', "LABEL_NODE" : "SOURCE_NAME"}).drop(columns='ID')
    
    # Merge to get TARGET_FUNC
    unique_combinations = unique_combinations.merge(
        nodes[['ID', 'FUNCTION', "LABEL_NODE"]], 
        how='left', 
        left_on='TARGET_NODE', 
        right_on='ID'
    ).rename(columns={'FUNCTION': 'TARGET_FUNC',"LABEL_NODE" : "TARGET_NAME"}).drop(columns='ID')
    
    source_tables = unique_combinations[unique_combinations["SOURCE_FUNC"] == "DataSources"]["SOURCE_NAME"].value_counts()
    target_tables = unique_combinations[unique_combinations["TARGET_FUNC"] == "DataDestinations"]["TARGET_NAME"].value_counts()
    return source_tables, target_tables


def transformations_dataflow(lineages: pd.DataFrame, nodes: pd.DataFrame) -> pd.DataFrame:
    transformations_df = lineages[lineages['TRANSFORMATION'].notna()][["SOURCE_NODE","SOURCE_FIELD","TRANSFORMATION"]]
    
    transformations_df = transformations_df.merge(
        nodes[['ID', "LABEL_NODE"]], 
        how='left', 
        left_on='SOURCE_NODE', 
        right_on='ID'
    ).rename(columns={"LABEL_NODE" : "SOURCE_NAME"}).drop(columns=['ID',"SOURCE_NODE"])
    columns_order = ['SOURCE_NAME'] + [col for col in transformations_df.columns if col != 'SOURCE_NAME']
    transformations_df = transformations_df[columns_order]
    return transformations_df


def split_join_argu(nodes: pd.DataFrame) -> tuple[pd.DataFrame,pd.DataFrame]:
    split_df = nodes[nodes['SPLIT_ARG'].notna()][["LABEL_NODE","FUNCTION","SPLIT_ARG"]]
    join_df = nodes[nodes['JOIN_ARG'].notna()][["LABEL_NODE","FUNCTION","JOIN_ARG"]]
    return split_df, join_df


def load_save_execute(path:str, lineages: pd.DataFrame, nodes: pd.DataFrame, dict_blocks: dict):
    count_control_flow = control_blocks(dict_blocks)
    blocks_df = dataflow_blocks(lineages, nodes)
    source_tables, target_tables = source_target_tables(lineages, nodes)
    transformations_df = transformations_dataflow(lineages, nodes)
    split_df, join_df = split_join_argu(nodes)

    source_tables.to_csv(f"{path}/source_df.csv")
    target_tables.to_csv(f"{path}/target_df.csv")
    count_control_flow.to_csv(f"{path}/blocks_control.csv")
    blocks_df.to_csv(f"{path}/blocks_dataflow.csv")
    transformations_df.to_csv(f"{path}/transformation_df.csv")
    split_df.to_csv(f"{path}/split_df.csv")
    join_df.to_csv(f"{path}/join_df.csv")
    return

def report_analysis(path:str): ### HARD DICRECTORES, ADD ARGS
    lineages = pd.read_csv('output-data/lineages/lineage-Package@Merge and filter.csv') #NOW THE CONTROL NODE WHICH YOU WANT TO ZOOM IN ON IS HARDCODED
    nodes = pd.read_csv('output-data/nodes.csv')
    with open('output-data/nodes/metadata_nodes_controlflow.json', 'r') as file:
        dict_blocks = json.load(file)
    load_save_execute(path, lineages, nodes, dict_blocks)
    return






