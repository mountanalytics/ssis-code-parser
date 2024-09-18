import json
import os
import glob
from sankeyapp.app import main 

from modules.dtsx_opener import Load
from modules.parsers.parse_controlflow import parse_control_flow
from modules.parsers.parse_sql import parse_sql_queries
from modules.parsers.parse_dataflow_nodes import parse_dataflow_nodes
from modules.parsers.parse_dataflow_lineages import parser_dataflow_lineage
from modules.merge_nodes_sets import node_lin_pars
from modules.report_generation.analysis_report import report_analysis
from modules.report_generation.create_report import main_report_generation

def reset_folders():
    folders = ['output-data/lineages', 'output-data/nodes/', 'output-data/']

    for folder_path in folders:
        # Get all files in the folder (ignoring subdirectories)
        files = glob.glob(os.path.join(folder_path, '*'))
        # Loop through and delete each file
        for file in files:
            if os.path.isfile(file):  # Ensure it's a file (not a directory)
                os.remove(file)  # Delete the file
                

#def run_ssis_parser(path_dtsx:str):
#    # delete old output-data files
#    reset_folders()#

#    # open dtsx
#    open_dtsx = Load(path_dtsx).run() #

#    # parse control flow and save results
#    metadata_controlflow = control_flow = parse_control_flow(open_dtsx)#

#    # parse sql queries in control flow
#    nodes_controlflow, lineages_controlflow = parse_sql_queries(control_flow)#

#    # parse data flow
#    for key in control_flow.keys():
#        if control_flow[key]["Description"] == "Data Flow Task":
#            metadata_dataflow, nodes_dataflow = parse_dataflow_nodes(open_dtsx, control_flow[key]["Index"], control_flow[key]["Block_name"])
#            lineages_dataflow = parser_dataflow_lineage(control_flow[key]["Block_name"])#

#    # merge nodes
#    nodes = node_lin_pars() #

#    # generate .docx report
#    report_analysis("output-data/reports/tables",'output-data/lineages/', 'output-data/nodes.csv', "Package@Merge and filter", "output-data/nodes/")
#    main_report_generation("output-data/reports/tables", "output-data/reports/MA_Rationalization_Model_Results.docx")#

#    # run sankeyapp dashboard locally
#    main('output-data/lineages/', 'output-data/nodes.csv', 'output-data/nodes/', 'output-data/lineages/Delete_error/') 


def run_ssis_parser(folder:str):
    # delete old output-data files
    reset_folders()

    flow = {}

    for file in os.listdir(folder):


        if file.endswith('.dtsx'):
            path_dtsx = folder + file

            file_name = file.replace('.dtsx', '')

            # open dtsx
            open_dtsx = Load(path_dtsx).run() 

            # parse control flow and save results
            control_flow = parse_control_flow(open_dtsx)

            # parse sql queries in control flow
            nodes_controlflow, lineages_controlflow = parse_sql_queries(control_flow)



            dataflow = {}

            # parse data flow
            for key in control_flow.keys():
                if control_flow[key]["Description"] == "Data Flow Task":
                    dataflow_name = control_flow[key]["Block_name"]
                    metadata_dataflow, nodes_dataflow, nodes_order, marker = parse_dataflow_nodes(open_dtsx, control_flow[key]["Index"], dataflow_name)
                    lineages_dataflow = parser_dataflow_lineage(dataflow_name, nodes_dataflow, nodes_order, metadata_dataflow)
                    dataflow[dataflow_name] = {'nodes':[nodes_dataflow], 'lineages':[lineages_dataflow]}

            flow[file_name] = {'control_flow' : {'nodes': nodes_controlflow, 'lineages': lineages_controlflow}, 'data_flow' : dataflow}

    
    with open(f'output-data/flow.json', 'w') as json_file:
        json.dump(flow, json_file, indent=4)

    # merge nodes
    nodes = node_lin_pars(flow) 

    # generate .docx report
    report_analysis("output-data/reports/tables",'output-data/lineages/', 'output-data/nodes.csv', "Package@Merge and filter", "output-data/nodes/")
    main_report_generation("output-data/reports/tables", "output-data/reports/MA_Rationalization_Model_Results.docx")

    # run sankeyapp dashboard locally
    main('output-data/lineages/', 'output-data/nodes.csv', 'output-data/nodes/', 'output-data/lineages/Delete_error/') 


    




if __name__ == "__main__":
    #run_ssis_parser("data/Demo_rabo/Demo_rabo/Demo_SSIS.dtsx")
    #run_ssis_parser("data/Demo_rabo_rep/Demo_rabo/Demo_SSIS.dtsx")
    #run_ssis_parser("data/Demo_SSIS_final.dtsx")
    #run_ssis_parser("data/Demo_SSIS_3.dtsx")

    run_ssis_parser("data/")


    
# insert into parser
# columns in control flow lineages between []
# reset folders function
# everything in memory (apart from sankey and report)