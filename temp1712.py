import pandas as pd
import re
import os

def format_hover_label_join(join_string: str) -> str:
    join_string = join_string.replace("['","").replace("']","").replace("', '"," ")
    join_string = re.sub(r'\b\s[Aa][Ss]\s+\w+', '', join_string)
    join_string = re.sub(r'\)\s*[Aa][Ss]\s+\w+', ')', join_string)
    sql_joins = [
        "JOIN",
        "INNER JOIN",
        "LEFT JOIN",
        "RIGHT JOIN",
        "FULL JOIN",
        "CROSS JOIN",
        "NATURAL JOIN",
        "SELF JOIN",
        "ON",
        "AND"
    ]
    join_pattern = r'(\b(?:' + '|'.join(re.escape(join) for join in sql_joins) + r')\b)'
    def normalize_match(match):
        return match.group(1).upper()
    normalized_string = re.sub(join_pattern, normalize_match, join_string, flags=re.IGNORECASE)
    split_result = re.split(join_pattern, normalized_string, flags=re.IGNORECASE)
    # Recombine into groups where each join is followed by its corresponding query part
    recombined_list = []
    current_part = split_result[0].strip()  # Start with the first part before any join
    
    for i in range(1, len(split_result), 2):  # Iterate over join statements and their following parts
        join = split_result[i].strip().upper()  # Get the join statement and normalize to uppercase
        next_part = split_result[i + 1].strip() if i + 1 < len(split_result) else ""  # Get the following part
        recombined_list.append(f"{join} {next_part}")  # Combine the join and its part
    
    # Add the initial part (before the first join) back to the beginning of the list
    if current_part:
        recombined_list.insert(0, current_part)
    form_string = " ".join(recombined_list).strip()
    return form_string
def extract_uasage_error_tables(folder_path: str) -> tuple[pd.DataFrame,pd.DataFrame]:
    csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]
    nodes = pd.read_csv("output-data/nodes.csv")
    nodes_no_error_table = pd.DataFrame()
    nodes_with_error_table = pd.DataFrame()
    for files in csv_files:
        lineages = pd.read_csv(f"{folder_path}/{files}")
        
        
        nodes = nodes[pd.notna(nodes["JOIN_ARG"])]
        lineages = lineages[lineages["SOURCE_NODE"].isin(nodes["ID"])].drop_duplicates(subset=["SOURCE_NODE", "TARGET_NODE"])
        source_node_counts = lineages["SOURCE_NODE"].value_counts()
        nodes_no_error_table_temp = nodes[nodes["ID"].isin(source_node_counts[source_node_counts == 1].index)][["LABEL_NODE","JOIN_ARG"]]
        nodes_with_error_table_temp = nodes[nodes["ID"].isin(source_node_counts[source_node_counts > 1].index)][["LABEL_NODE","JOIN_ARG"]]
        nodes_no_error_table_temp["JOIN_ARG"] = nodes_no_error_table_temp["JOIN_ARG"].apply(format_hover_label_join)
        nodes_with_error_table_temp["JOIN_ARG"] = nodes_with_error_table_temp["JOIN_ARG"].apply(format_hover_label_join)
        
        
        nodes_with_error_table = pd.concat([nodes_with_error_table,nodes_with_error_table_temp],ignore_index=True)
        nodes_no_error_table = pd.concat([nodes_no_error_table,nodes_no_error_table_temp],ignore_index=True)
    return nodes_no_error_table, nodes_with_error_table

folder_path = "output-data/lineages"
nodes_no_error_table, nodes_with_error_table = extract_uasage_error_tables(folder_path)













