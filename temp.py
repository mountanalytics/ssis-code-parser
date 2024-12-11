
import pandas as pd
import json
from modules.parsers.parse_controlflow import *
from sql_parser.main import main

with open('output-data/nodes/metadata_nodes_Package 1.json', 'r') as json_file: # columns data
    control_flow = json.load(json_file)

"""
for node in control_flow.keys():
    if control_flow[node]['Description'] == 'Execute SQL Task':

        print(control_flow[node]['SQL_state'])
        print(control_flow[node]['Variables'])

        try:

            print(control_flow[node]['Result_variable'])
        except KeyError:
            pass

        print('##############################')
        print()

        """


def executesql_parser(control_flow, nodes, lineages, variable_tables, node_name, foreach):
    """
    Function that extracts data from SQL queries and returns them
    """
    

    try: # try if it is part of for each loop
        for i in control_flow[node_name]['SQL']:
            variables = flatten_if_nested(control_flow[node_name]['SQL'][i]['Variables'])#[0]     
    except:
        try:
            variables = flatten_if_nested(control_flow[node_name]['Variables'])
        except:
            pass
        pass

    try: # try if it is part of for each loop
        for i in control_flow[node_name]['SQL']:
            sql_statement = control_flow[node_name]['SQL'][i]['SQL_state'] 
            previous_node_name = node_name
            node_name = i
    except:
        sql_statement = control_flow[node_name]['SQL_state'] 

    # if the sql statement contains a variable, change the ? with the variable name
    if '?' in sql_statement: 
        sql_statement = replace_variables(sql_statement, variables)

    else:
        try:
            # if the sql statement contains a variable, change the indeces with the variable name

            for variable in variables:
                
                for i, char in enumerate(sql_statement.replace(" ", "")): #!!!!!!!!!!!!! ADD MORE CONDITIONS
                    if char == variable[1]:# and ((sql_statement[i+1] == ')') and (sql_statement[i-1] == ',')) or ((sql_statement[i+1] == ',') and (sql_statement[i-1] == '('))  or ((sql_statement[i+1] == ',') and (sql_statement[i-1] == ')')):

                        sql_statement = sql_statement.replace(char, f"'{variable[0]}'") # + '?' + sql_statements[i + 1:]

            sql_statement = replace_variables(sql_statement, variables) 

        except:
            pass

    try:
        result_set = control_flow[node_name]['Result_variable']
        print(result_set)
        print()
    except:
        result_set = None


    print(sql_statement)
    print()

    nodes, lineages, _ = main(sql_statement, result_set, nodes, lineages, variable_tables, node_name)

    for i in nodes:
        print(i)
    print()
    for i in lineages:
        print(i)
    print('----------------')
    print() 

    return nodes, lineages


for node in control_flow.keys():
    nodes = []
    lineages = []
    variable_tables = {}
    if control_flow[node]['Description'] == 'Execute SQL Task':

        nodes = []
        lineages = []

        n, l = executesql_parser(control_flow, nodes, lineages, variable_tables, node, False)




