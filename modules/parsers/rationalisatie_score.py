import pandas as pd
import os

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

