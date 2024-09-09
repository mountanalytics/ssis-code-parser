import json

from helpers.dtsx_opener import Load
from modules.parse_control_func import parse_control_flow
from modules.parse_sql import parse_sql_queries
from modules.parse_df_nodes import parse_dataflow_nodes
from modules.parse_df_lineage import parser_dataflow_lineage
from modules.nodes_lin_combine import node_lin_pars
from modules.analysis_report import report_analysis
from report.report_create_func import main_report_generation

open_dtsx = Load("data/Demo_rabo/Demo_rabo/Demo_SSIS.dtsx").run()
control_flow = parse_control_flow(open_dtsx)
with open('output-data/dict_blocks_controlflow.json', 'w') as json_file:
    json.dump(control_flow, json_file, indent=4)

parse_sql_queries(control_flow)
for key in control_flow.keys():
    if control_flow[key]["Description"] == "Data Flow Task":
        parse_dataflow_nodes(open_dtsx, control_flow[key]["Index"], control_flow[key]["Block_name"])
        parser_dataflow_lineage(control_flow[key]["Block_name"])
node_lin_pars() 
report_analysis()
main_report_generation()
    
    
    