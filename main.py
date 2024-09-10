import json
from sankeyapp.app import main 
from modules.dtsx_opener import Load
from modules.parsers.parse_controlflow import parse_control_flow
from modules.parsers.parse_sql import parse_sql_queries
from modules.parsers.parse_dataflow_nodes import parse_dataflow_nodes
from modules.parsers.parse_dataflow_lineages import parser_dataflow_lineage
from modules.merge_nodes_sets import node_lin_pars
from modules.report_generation.analysis_report import report_analysis
from modules.report_generation.create_report import main_report_generation



def run_ssis_parser(path_dtsx:str):

    # open dtsx
    open_dtsx = Load(path_dtsx).run() 

    # parse control flow and save results
    control_flow = parse_control_flow(open_dtsx)

    # parse sql queries in control flow
    parse_sql_queries(control_flow)

    # parse data flow
    for key in control_flow.keys():
        if control_flow[key]["Description"] == "Data Flow Task":
            parse_dataflow_nodes(open_dtsx, control_flow[key]["Index"], control_flow[key]["Block_name"])
            parser_dataflow_lineage(control_flow[key]["Block_name"])

    # parse nodes and lineages
    node_lin_pars() 

    # generate .docx report
    report_analysis("output-data/reports/tables")
    main_report_generation("output-data/reports/tables", "output-data/reports/MA_Rationalization_Model_Results.docx")

    # run sankeyapp dashboard locally
    main('output-data/lineages/', 'output-data/nodes.csv') 

if __name__ == "__main__":
    #run_ssis_parser("data/Demo_rabo/Demo_rabo/Demo_SSIS.dtsx")

    #run_ssis_parser("data/Demo_rabo_rep/Demo_rabo/Demo_SSIS.dtsx")

    run_ssis_parser("data/table_var/table_var/Package.dtsx")


    
    
    