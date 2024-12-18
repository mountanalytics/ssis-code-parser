import pandas as pd
from gradpyent.gradient import Gradient

scoring = pd.read_excel("data/Rationalisation score.xlsx")
nodes = pd.read_csv("output-data/nodes.csv")
nodes["Rat_score"] = 0
flag_nodes = pd.read_csv("output-data/nodes_rat_score.csv")
points_nodes = pd.merge(scoring, flag_nodes, left_on="Flag", right_on="flag",how="inner")
points_nodes = points_nodes[["LABEL_NODE","Scoring"]]

scoring_sum = points_nodes.groupby("LABEL_NODE", as_index=False)["Scoring"].sum()
nodes = pd.merge(nodes, scoring_sum, on="LABEL_NODE", how="left")
nodes["Rat_score"] = nodes["Rat_score"] + nodes["Scoring"].fillna(0)
nodes = nodes.drop(columns=["Scoring"])
gg = Gradient(gradient_start='#FF6961', gradient_end='#FFFFFF', opacity=1.0)
nodes["COLOR"] = gg.get_gradient_series(series=nodes["Rat_score"], fmt='html')

nodes.to_csv("output-data/nodes_rat_score.csv")





