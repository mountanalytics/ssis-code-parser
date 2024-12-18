import pandas as pd

scoring = pd.read_excel("Rationalisation score.xlsx")
mapping_score = pd.read_excel("data/scoring_mapping.xlsx")
scoring = pd.merge(scoring, mapping_score, on="Rule", how="inner")
nodes = pd.read_csv("output-data/nodes.csv")
nodes["Rat_score"] = 0
flag_nodes = pd.read_csv("output-data/nodes_rat_score.csv")
points_nodes = pd.merge(scoring, flag_nodes, left_on="Flag", right_on="flag",how="inner")
points_nodes = points_nodes[["LABEL_NODE","Scoring"]]

scoring_sum = points_nodes.groupby("LABEL_NODE", as_index=False)["Scoring"].sum()
nodes = pd.merge(nodes, scoring_sum, on="LABEL_NODE", how="left")







