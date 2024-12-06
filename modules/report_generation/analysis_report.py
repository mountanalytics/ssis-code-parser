import json
import pandas as pd
import numpy as np
import os
import glob
import re

def sql_functions() -> list:
    sql_functions = [
        "CURRENT_DATETIME", "NOW", "CURDATE", "CURTIME", "DATE", "DAY", "DAYOFWEEK", "DAYOFYEAR", 
        "HOUR", "MINUTE", "MONTH", "SECOND", "WEEK", "YEAR", "TIMESTAMPADD", "TIMESTAMPDIFF", "CURRENT_TIMESTAMP",
        "CONCAT", "SUBSTRING", "CHAR_LENGTH", "LOWER", "UPPER", "TRIM", "REPLACE", "LPAD", "RPAD", 
        "INSTR", "LEFT", "RIGHT", "MD5", "SHA2", "TO_BASE64", "FROM_BASE64", "AES_ENCRYPT", 
        "AES_DECRYPT", "HEX", "UNHEX", "STRING_BYTES", "FORMAT",
        "ABS", "CEIL", "FLOOR", "ROUND", "MOD", "POWER", "RAND", "SQRT", "EXP", "LOG", 
        "LOG10", "SIGN", "TRUNCATE",
        "COUNT", "SUM", "AVG", "MIN", "MAX", "GROUP_CONCAT", "STDDEV", "VARIANCE",
        "IF", "CASE", "COALESCE", "NULLIF",
        "JSON_EXTRACT", "JSON_SET", "JSON_REMOVE", "JSON_CONTAINS", "JSON_UNQUOTE", "JSON_KEYS", 
        "JSON_LENGTH", "ST_DISTANCE", "ST_INTERSECTS", "ST_AREA", "ST_LENGTH", "ST_CONTAINS", "ST_WITHIN", 
        "ST_ASGEOJSON", "ST_GEOMFROMTEXT",
        "ROW_NUMBER", "RANK", "DENSE_RANK", "NTILE", "LEAD", "LAG", "FIRST_VALUE", "LAST_VALUE",
        "REGEXP", "REGEXP_LIKE", "REGEXP_REPLACE", "REGEXP_SUBSTR",
        "DATABASE", "USER", "VERSION", "CONNECTION_ID", "ROW_COUNT"
        ]
    return sql_functions



def control_blocks(dict_blocks: dict) -> pd.Series:
    blocks_count = []
    for key in dict_blocks:
        blocks_count.append(dict_blocks[key]["Description"])
        if dict_blocks[key]["Description"] == 'Foreach Loop Container':
            for itter in dict_blocks[key]["SQL"]:
                blocks_count.append(dict_blocks[key]["SQL"][itter]["Description"])
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

def hard_coded(nodes: pd.DataFrame) -> pd.DataFrame:
    block = list(nodes["NAME_NODE"])
    hard_coded_blocks = pd.DataFrame(columns=nodes.columns)
    for index, row in nodes.iterrows():
        # Convert SPLIT_ARG and FILTER to strings to handle NaNs safely
        split_arg_str = str(row["SPLIT_ARG"])
        filter_str = str(row["FILTER"])
        # Check conditions
        if (any(name in split_arg_str for name in block) or any(name in filter_str for name in block)) \
        or (pd.isna(row["SPLIT_ARG"]) and pd.isna(row["FILTER"])):
            nodes.at[index, "COLOR"] = "black"
        elif pd.notna(row["SPLIT_ARG"]) or pd.notna(row["FILTER"]):
            hard_coded_blocks = pd.concat([hard_coded_blocks,nodes.loc[[index]]], ignore_index=True)
    desired_columns = ["LABEL_NODE", "FUNCTION", "SPLIT_ARG", "FILTER"]
    hard_coded_blocks = hard_coded_blocks[desired_columns]
    return hard_coded_blocks

def hard_coded_transformations(folder_path:str) -> pd.DataFrame:
    csv_files = glob.glob(os.path.join(folder_path, '*.csv'))
    hard_coded = pd.DataFrame(columns=["SOURCE_COLUMNS","TARGET_COLUMN","TRANSFORMATION"])
    for file in csv_files:
        df = pd.read_csv(file)
        unique_columns = list(np.unique(df['SOURCE_FIELD']))
        df = df[df["TRANSFORMATION"].notna()]
        unique_columns.extend(sql_functions())
        hard_codes_file = pd.DataFrame(columns=[['SOURCE_COLUMNS', 'TARGET_COLUMN', 'TRANSFORMATION']])
        for _, row in df.iterrows():
            parts = re.split(r'\s*[+\-/*]\s*', row["TRANSFORMATION"])
            if len(parts) != 1:  # If there are multiple parts after splitting
                if False in [part.strip() in unique_columns for part in parts]:
                    hard_codes_file = pd.concat([hard_codes_file, pd.DataFrame([row])], ignore_index=True)
            else:
                if not any(col in row["TRANSFORMATION"] for col in unique_columns):
                    hard_codes_file = pd.concat([hard_codes_file, pd.DataFrame([row])], ignore_index=True)
        hard_codes_file = hard_codes_file[["SOURCE_COLUMNS","TARGET_COLUMN","TRANSFORMATION"]]
        hard_coded = pd.concat([hard_coded,hard_codes_file],ignore_index=True)
    return hard_coded

def load_save_execute(path:str, lineages: pd.DataFrame, nodes: pd.DataFrame, dict_blocks: dict, lineage_path: str):
    count_control_flow = control_blocks(dict_blocks)
    blocks_df = dataflow_blocks(lineages, nodes)
    source_tables, target_tables = source_target_tables(lineages, nodes)
    transformations_df = transformations_dataflow(lineages, nodes)
    split_df, join_df = split_join_argu(nodes)
    hard_coded_df = hard_coded(nodes)
    hard_coded_trans = hard_coded_transformations(lineage_path)

    source_tables.to_csv(f"{path}/source_df.csv")
    target_tables.to_csv(f"{path}/target_df.csv")
    count_control_flow.to_csv(f"{path}/blocks_control.csv")
    blocks_df.to_csv(f"{path}/blocks_dataflow.csv")
    transformations_df.to_csv(f"{path}/transformation_df.csv")
    split_df.to_csv(f"{path}/split_df.csv")
    join_df.to_csv(f"{path}/join_df.csv")
    hard_coded_df.to_csv(f"{path}/hard_coded_nodes.csv")
    hard_coded_trans.to_csv(f"{path}/hard_coded_transformations.csv")
    return

def report_analysis(path:str, lineage_path: str, node_path: str, control_block: str, metadata_path: str, file_name:str):
    lineages = pd.read_csv(f'{lineage_path}lineage-{control_block}.csv')
    nodes = pd.read_csv(node_path)
    with open(f'{metadata_path}metadata_nodes_{file_name}.json', 'r') as file:
        dict_blocks = json.load(file)
    load_save_execute(path, lineages, nodes, dict_blocks, lineage_path)
    return






