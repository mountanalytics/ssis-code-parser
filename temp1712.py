import pandas as pd
import itertools
import re
import os
import pickle
"""
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
"""






def error_tables_flag(folder_path: str, label_node_dict: dict) -> dict:
    for first_key in label_node_dict.keys():  # Get the first key in the dictionary
        label_node_dict[first_key].update({"no_error_table": False, "error_table": False})
    csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]
    nodes = pd.read_csv("output-data/nodes.csv")
    for files in csv_files:
        lineages = pd.read_csv(f"{folder_path}{files}")
        nodes = nodes[pd.notna(nodes["JOIN_ARG"])]
        lineages = lineages[lineages["SOURCE_NODE"].isin(nodes["ID"])].drop_duplicates(subset=["SOURCE_NODE", "TARGET_NODE"])
        source_node_counts = lineages["SOURCE_NODE"].value_counts()
        nodes_no_error_table_temp = nodes[nodes["ID"].isin(source_node_counts[source_node_counts == 1].index)][["LABEL_NODE","JOIN_ARG"]]
        nodes_with_error_table_temp = nodes[nodes["ID"].isin(source_node_counts[source_node_counts > 1].index)][["LABEL_NODE","JOIN_ARG"]]
        for _, label in nodes_no_error_table_temp.iterrows():
            if label["LABEL_NODE"] in label_node_dict.keys():
                label_node_dict[label["LABEL_NODE"]]["no_error_table"] = True
            else:
                label_node_dict[label["LABEL_NODE"]] = {"no_error_table": True, "error_table": False}
        for _, label in nodes_with_error_table_temp.iterrows():
            if label["LABEL_NODE"] in label_node_dict.keys():
                label_node_dict[label["LABEL_NODE"]]["error_table"] = True
            else:
                label_node_dict[label["LABEL_NODE"]] = {"error_table": True, "no_error_table": False}
    return label_node_dict

folder_path = "output-data/lineages/"
nodes = pd.read_csv("output-data/nodes.csv")
with open('output-data/nodes_flag.pickle', 'rb') as handle:
    label_node_dict = pickle.load(handle)
label_node_dict = error_tables_flag(folder_path, label_node_dict)

















