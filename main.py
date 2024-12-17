import json
import os
import glob
from sankeyapp.app import main 
from modules.dtsx_opener import Load
from modules.parsers.extract_data_controlflow import parse_control_flow
from modules.parsers.parse_controlflow import parse_sql_queries
from modules.parsers.parse_dataflow_nodes import parser_dataflow_nodes
from modules.parsers.parse_dataflow_lineages import parser_dataflow_lineages
from modules.merge_nodes_sets import node_lin_pars
from modules.report_generation.analysis_report import report_analysis
from modules.report_generation.create_report import main_report_generation


def reset_folders(folders:list):
    """
    This function deletes everything inside the folders where the parsed data is saved.
    """
    for folder_path in folders:
        # Get all files in the folder (ignoring subdirectories)
        files = glob.glob(os.path.join(folder_path, '*'))
        # Loop through and delete each file
        for file in files:
            if os.path.isfile(file):  # Ensure it's a file (not a directory)
                os.remove(file)  # Delete the file
                

def run_ssis_parser(folder:str):
    """
    Main function of the project, orchestrates the data parsing, the report generation and the launch of the dashboard interface.
    """
    reset_folders(['output-data/lineages/', 'output-data/nodes/', 'output-data/'])  # delete old output-data files

    flow = {}

    # for package.dtsx in folder
    for file in os.listdir(folder):

        if file.endswith('.dtsx'):

            path_dtsx = folder + file
            file_name = file.replace('.dtsx', '')

            open_dtsx = Load(path_dtsx).run() # open dtsx

            control_flow = parse_control_flow(open_dtsx, file_name) # parse control flow (metadata)

            nodes_controlflow, lineages_controlflow = parse_sql_queries(control_flow, file_name) # parse sql queries in control flow (create nodes and lineages datasets)

            data_flow = {}

            # parse data flow (create nodes and lineages datasets)
            for key in control_flow.keys():
                if control_flow[key]["Description"] == "Data Flow Task":
                    dataflow_name = control_flow[key]["Block_name"]
                    metadata_dataflow, nodes_dataflow, nodes_order, marker = parser_dataflow_nodes(open_dtsx, control_flow[key]["Index"], dataflow_name)
                    lineages_dataflow = parser_dataflow_lineages(dataflow_name, nodes_dataflow, nodes_order, metadata_dataflow)
                    data_flow[dataflow_name] = {'nodes':nodes_dataflow.to_dict(), 'lineages':lineages_dataflow.to_dict()}

            flow[file_name] = {'control_flow' : {'nodes': nodes_controlflow.to_dict(), 'lineages': lineages_controlflow.to_dict()}, 'data_flow' : data_flow}

    with open(f'output-data/flow.json', 'w') as json_file: # save flow (metadata)
        json.dump(flow, json_file, indent=4, default=str)

    nodes = node_lin_pars(flow) # merge nodes

    # generate .docx report
    #report_analysis("output-data/reports/tables",'output-data/lineages/', 'output-data/nodes.csv', "Package 1_cf", "output-data/nodes/", file_name)
    #main_report_generation("output-data/reports/tables", "output-data/reports/MA_Rationalization_Model_Results.docx")

    main('output-data/lineages/', 'output-data/nodes.csv', 'output-data/nodes/', 'output-data/lineages/Delete_error/')     # run sankeyapp dashboard locally



if __name__ == "__main__":
    run_ssis_parser("data/Demo_rabo/Demo_rabo/")
    #run_ssis_parser("data/")
