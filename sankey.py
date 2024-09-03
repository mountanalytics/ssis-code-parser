import pandas as pd
import plotly.graph_objects as go
from plotly.offline import plot

df = pd.read_csv('output-data/lineages/lineages.csv', sep = ',')
df_labels = pd.read_csv('output-data/nodes_sankey.csv')
#df = df.rename(columns = {"CALC_VIEW" : "TARGET_FIELD", 'SOURCE' : 'SOURCE_FIELD', "CALC_ID" : "TARGET_NODE", "SOURCE_ID" : "SOURCE_NODE"})
#df_labels = df_labels.rename(columns = {"Unnamed: 0" : "LABEL_NODE"})

df['source_to_target'] = df[['SOURCE_FIELD', 'TARGET_FIELD']].agg('=>'.join, axis=1)

    # df['source_to_target_transformation'] = df[['source_to_target', 'TRANSFORMATION']].agg('-'.join, axis=1)
df['source_to_target_transformation'] = df[['source_to_target','TRANSFORMATION']].apply(lambda x : '{}<br />Transformation: {}'.format(x[0],x[1]), axis=1)
df_labels['hover_label'] = df_labels[['LABEL_NODE','FILTER']].apply(lambda x : '{}<br />Filter: {}'.format(x[0],x[1]), axis=1)

print(df[['SOURCE_NODE', 'TARGET_NODE', 'LINK_VALUE', 'COLOR']])
print(df_labels[['LABEL_NODE', 'COLOR']])


# the nodes and lineage graphs are connected from the nodes' id (the id of the nodes dataframe is related to the source node and target node)

fig = go.Figure(data=[go.Sankey(
    node = dict(
      pad = 20,
      thickness = 20,
      line = dict(color = "black", width = 0.5),
      label = df_labels['LABEL_NODE'],
      color = df_labels['COLOR'],
      customdata = df_labels['hover_label'],
      hovertemplate='%{customdata}'
      #hovertemplate='Node %{customdata} has total value %{value}<extra></extra>',
      #color = "blue"

    ),
    link = dict(
      #arrowlen=15,
      line = dict(color = "blue", width = 0.05),
      hoverlabel = dict (font = dict(size=15) ),
      source = df['SOURCE_NODE'], 
      target = df['TARGET_NODE'],
      value = df['LINK_VALUE'],
      customdata = df['source_to_target_transformation'],
      hovertemplate='Details: %{customdata}',
      color = df['COLOR'],

      #hovertemplate='Link from node %{source.customdata}<br />'+'to node%{target.customdata}<br />has value %{value}'+ '<br />and data %{customdata}<extra></extra>',
  ),
  )])

fig.update_layout(font_size=10)
fig.update_layout(title = dict(text="Sankey of SSIS package", font_size=20))
fig.show()
plot(fig, validate=False)

