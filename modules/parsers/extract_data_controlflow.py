import json
import pandas as pd


def parse_sql_task(control_node: dict) -> dict:
    """
    This function taked as input an ExecuteSQLTask and return a dictionary containing the SQL statement, 
    the variables in the statement and the node type (description)
    """

    vars_list = []

    # try if there are variables
    try: 
        variables = control_node['DTS:ObjectData']['SQLTask:SqlTaskData']['SQLTask:ParameterBinding']

        if type(variables) == list:
            try:
                variables_list = [(i['SQLTask:DtsVariableName'], i['SQLTask:ParameterName']) for i in control_node['DTS:ObjectData']['SQLTask:SqlTaskData']['SQLTask:ParameterBinding']]
            except:
                variables_list = [i['SQLTask:DtsVariableName'] for i in control_node['DTS:ObjectData']['SQLTask:SqlTaskData']['SQLTask:ParameterBinding']]

            vars_list.append(variables_list)
            sql_state = control_node['DTS:ObjectData']['SQLTask:SqlTaskData']['SQLTask:SqlStatementSource']
        
        elif type(variables) == dict:
            try:
                variables_list = [(control_node['DTS:ObjectData']['SQLTask:SqlTaskData']['SQLTask:ParameterBinding']['SQLTask:DtsVariableName'], control_node['DTS:ObjectData']['SQLTask:SqlTaskData']['SQLTask:ParameterBinding']['SQLTask:ParameterName'])]
            except:
                variables_list = control_node['DTS:ObjectData']['SQLTask:SqlTaskData']['SQLTask:ParameterBinding']['SQLTask:DtsVariableName']
        
            vars_list.append(variables_list)
            sql_state = control_node['DTS:ObjectData']['SQLTask:SqlTaskData']['SQLTask:SqlStatementSource']

        dict_sql = {
            "Description": control_node['DTS:Description'],
            "SQL_state": sql_state,
            "Variables": vars_list
            }
            
    except: # if there are no variables
        sql_state = control_node['DTS:ObjectData']['SQLTask:SqlTaskData']['SQLTask:SqlStatementSource']
        dict_sql = {
            "Description": control_node['DTS:Description'],
            "SQL_state": sql_state,
            "Variables": None
            }
        
    try: # try and parse the destination variable
        dict_sql['Result_variable'] = control_node['DTS:ObjectData']['SQLTask:SqlTaskData']['SQLTask:ResultBinding']['SQLTask:DtsVariableName']
    except:
        pass

    return dict_sql


def parse_foreach_container(control_node:dict) -> dict:
    """
    This function taked as input an ForEachLoopCointer and return a dictionary containing the node type (description),
    the input table, the iterration variables and, if present within, a parsed ExecuteSQLTask,
    """

    dict_blocks_for = {}

    if control_node["DTS:ForEachEnumerator"]["DTS:CreationName"] == "Microsoft.ForEachADOEnumerator":
        input_table = control_node["DTS:ForEachEnumerator"]["DTS:ObjectData"]["FEEADO"]["VarName"]

    variables = control_node["DTS:ForEachVariableMappings"]["DTS:ForEachVariableMapping"]
    vars_list = []

    if type(variables) == list:
        vars_list = [(i['DTS:VariableName'],int(i["DTS:ValueIndex"])) for i in variables]
    elif type(variables) == dict:
        vars_list.append((variables['DTS:VariableName'], int(variables["DTS:ValueIndex"])))
        
    if type(control_node['DTS:Executables']['DTS:Executable']) == list:
        for i,control_for in enumerate(control_node['DTS:Executables']['DTS:Executable']):
            if control_for['DTS:CreationName'] =='Microsoft.ExecuteSQLTask':
                dict_blocks_for[control_for["DTS:refId"]] = parse_sql_task(control_for)
    else:
        control_for = control_node['DTS:Executables']['DTS:Executable']
        if control_for['DTS:CreationName'] =='Microsoft.ExecuteSQLTask': 
            dict_blocks_for[control_for["DTS:refId"]] = parse_sql_task(control_for)

    return {'Description': control_node['DTS:Description'], 
            'Input_variable': input_table, 
            'Iterr_variables': vars_list, 
            'SQL': dict_blocks_for} 


def parse_control_flow(open_dtsx: dict, file_name:str) -> dict:
    """
    This function iterated through the nodes (Executables) of a dtsx file and returns the parsed metadata as a dictionary
    containing one dictionary for every parsed nodes
    """
    dict_blocks = {}    

    # iterate through the control flow nodes and parse the data
    for idx,control_node in enumerate(open_dtsx['DTS:Executables']['DTS:Executable']):

        if control_node['DTS:CreationName'] =='Microsoft.ExecuteSQLTask': 
            dict_blocks[control_node["DTS:refId"]] = parse_sql_task(control_node)
        elif control_node['DTS:CreationName'] == 'Microsoft.Pipeline':
            dict_blocks[control_node["DTS:refId"]] = {'Description': control_node['DTS:Description'], 'Index': idx, "Block_name": control_node['DTS:refId'].replace("\\", "@")}

        elif control_node['DTS:CreationName'] =='Microsoft.ExpressionTask': 
            dict_blocks[control_node["DTS:refId"]] = {'Description': control_node['DTS:Description'], 'Expression' : control_node['DTS:ObjectData']['ExpressionTask']['Expression']}

        elif control_node['DTS:CreationName'] =='STOCK:FOREACHLOOP':
            dict_blocks[control_node["DTS:refId"]] = parse_foreach_container(control_node)

        if control_node["DTS:CreationName"] == "STOCK:SEQUENCE":
            dict_blocks[control_node["DTS:refId"]] = {'Description': control_node['DTS:Description'], "key": idx}
            for seq_comp in control_node["DTS:Executables"]["DTS:Executable"]:
                if seq_comp['DTS:CreationName'] =='Microsoft.ExecuteSQLTask': 
                    dict_blocks[seq_comp["DTS:refId"]] = parse_sql_task(seq_comp)
                elif seq_comp['DTS:CreationName'] == 'Microsoft.Pipeline':
                    dict_blocks[seq_comp["DTS:refId"]] = {'Description': seq_comp['DTS:Description'], 'Index': idx, "Block_name": seq_comp['DTS:refId'].replace("\\", "@")}

                elif seq_comp['DTS:CreationName'] =='Microsoft.ExpressionTask': 
                    dict_blocks[seq_comp["DTS:refId"]] = {'Description': seq_comp['DTS:Description'], 'Expression' : seq_comp['DTS:ObjectData']['ExpressionTask']['Expression']}

                elif seq_comp['DTS:CreationName'] =='STOCK:FOREACHLOOP':
                    dict_blocks[seq_comp["DTS:refId"]] = parse_foreach_container(seq_comp)
                
                
    # sort the nodes
    df_lineage = pd.DataFrame(columns=["ID_block_out","ID_block_in"])

    for const in open_dtsx["DTS:PrecedenceConstraints"]["DTS:PrecedenceConstraint"]:
        if dict_blocks[const["DTS:From"]]['Description'] != 'Sequence Container':
            df_lineage = pd.concat([df_lineage,pd.DataFrame({"ID_block_out" : [const["DTS:From"]], "ID_block_in": [const["DTS:To"]]})])
        if dict_blocks[const["DTS:To"]]['Description'] == 'Sequence Container':
            if type(open_dtsx["DTS:Executables"]["DTS:Executable"][dict_blocks[const["DTS:To"]]['key']]["DTS:PrecedenceConstraints"]["DTS:PrecedenceConstraint"]) == list:
                from_seq = open_dtsx["DTS:Executables"]["DTS:Executable"][dict_blocks[const["DTS:To"]]['key']]["DTS:PrecedenceConstraints"]["DTS:PrecedenceConstraint"][0]["DTS:From"]
                for seq_const in open_dtsx["DTS:Executables"]["DTS:Executable"][dict_blocks[const["DTS:To"]]['key']]["DTS:PrecedenceConstraints"]["DTS:PrecedenceConstraint"]:
                    df_lineage = pd.concat([df_lineage,pd.DataFrame({"ID_block_out" : [seq_const["DTS:From"]], "ID_block_in": [seq_const["DTS:To"]]})])
            else:
                from_int = open_dtsx["DTS:Executables"]["DTS:Executable"][dict_blocks[const["DTS:To"]]['key']]["DTS:PrecedenceConstraints"]["DTS:PrecedenceConstraint"]["DTS:From"]
                to_int = open_dtsx["DTS:Executables"]["DTS:Executable"][dict_blocks[const["DTS:To"]]['key']]["DTS:PrecedenceConstraints"]["DTS:PrecedenceConstraint"]["DTS:To"]
                df_lineage = pd.concat([df_lineage,pd.DataFrame({"ID_block_out" : [from_int], "ID_block_in": [to_int]})])
                from_seq = open_dtsx["DTS:Executables"]["DTS:Executable"][dict_blocks[const["DTS:To"]]['key']]["DTS:PrecedenceConstraints"]["DTS:PrecedenceConstraint"]["DTS:From"]
            df_lineage = pd.concat([df_lineage,pd.DataFrame({"ID_block_out" : [const["DTS:To"]], "ID_block_in": [from_seq]})])
            
        elif dict_blocks[const["DTS:From"]]['Description'] == 'Sequence Container':
            if type(open_dtsx["DTS:Executables"]["DTS:Executable"][dict_blocks[const["DTS:From"]]['key']]["DTS:PrecedenceConstraints"]["DTS:PrecedenceConstraint"]) == list:
                from_seq = open_dtsx["DTS:Executables"]["DTS:Executable"][dict_blocks[const["DTS:From"]]['key']]["DTS:PrecedenceConstraints"]["DTS:PrecedenceConstraint"][-1]["DTS:To"]
            else:
                from_seq = open_dtsx["DTS:Executables"]["DTS:Executable"][dict_blocks[const["DTS:From"]]['key']]["DTS:PrecedenceConstraints"]["DTS:PrecedenceConstraint"]["DTS:To"]
            df_lineage = pd.concat([df_lineage,pd.DataFrame({"ID_block_out" : [from_seq], "ID_block_in": [const["DTS:To"]]})])
            
    start_node = df_lineage['ID_block_out'][~df_lineage['ID_block_out'].isin(df_lineage['ID_block_in'])].values[0]
    end_node = df_lineage['ID_block_in'][~df_lineage['ID_block_in'].isin(df_lineage['ID_block_out'])].values[0]

    ordered_nodes = [start_node]
    current_node = start_node

    while current_node != end_node:
        next_node = df_lineage.loc[df_lineage['ID_block_out'] == current_node, 'ID_block_in'].values[0]
        ordered_nodes.append(next_node)
        current_node = next_node

    # merge the metadata with the sorted nodes
    dict_blocks = {node: dict_blocks[node] for node in ordered_nodes}

    # Save the converted dictionary as a JSON file
    with open(f'output-data/nodes/metadata_nodes_{file_name}.json', 'w') as json_file:
        json.dump(dict_blocks, json_file, indent=4)

    return dict_blocks

