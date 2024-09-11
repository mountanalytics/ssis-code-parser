import json

def pars_sql_task(control_node: dict) -> dict:
    vars_list = []#pd.DataFrame(columns=["Variable"])
    
    try: # try if there are variables
        variables = control_node['DTS:ObjectData']['SQLTask:SqlTaskData']['SQLTask:ParameterBinding']
        
        if type(variables) == list:

            variables_list = [i['SQLTask:DtsVariableName'] for i in control_node['DTS:ObjectData']['SQLTask:SqlTaskData']['SQLTask:ParameterBinding']]
            vars_list.append(variables_list)
            #new_vars_df = pd.DataFrame(variables_list, columns=["Variable"])
            #vars_df = pd.concat([vars_df,new_vars_df], ignore_index=True)
            sql_state = control_node['DTS:ObjectData']['SQLTask:SqlTaskData']['SQLTask:SqlStatementSource']
        
        
        elif type(variables) == dict:
            variables_list = control_node['DTS:ObjectData']['SQLTask:SqlTaskData']['SQLTask:ParameterBinding']['SQLTask:DtsVariableName']
            vars_list.append(variables_list)
            #new_vars_df = pd.DataFrame({"Variable": [variables_list]})
            #vars_df = pd.concat([vars_df,new_vars_df], ignore_index=True)
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


def parse_foreach_container(control_node):

    dict_blocks_for = {}
    if control_node["DTS:ForEachEnumerator"]["DTS:CreationName"] == "Microsoft.ForEachADOEnumerator":
        input_table = control_node["DTS:ForEachEnumerator"]["DTS:ObjectData"]["FEEADO"]["VarName"]
    variables = control_node["DTS:ForEachVariableMappings"]["DTS:ForEachVariableMapping"]
    vars_list = []
    if type(variables) == list:

        vars_list = [i['DTS:VariableName'] for i in variables]

    elif type(variables) == dict:
        vars_list.append(variables['DTS:VariableName'])
        
    if type(control_node['DTS:Executables']['DTS:Executable']) == list:
        for i,control_for in enumerate(control_node['DTS:Executables']['DTS:Executable']):
            if control_for['DTS:CreationName'] =='Microsoft.ExecuteSQLTask': # if the task is ExecuteSQL then print query and variables
                dict_blocks_for[control_for["DTS:refId"]] = pars_sql_task(control_for)
    else:
        control_for = control_node['DTS:Executables']['DTS:Executable']
        if control_for['DTS:CreationName'] =='Microsoft.ExecuteSQLTask': # if the task is ExecuteSQL then print query and variables
            dict_blocks_for[control_for["DTS:refId"]] = pars_sql_task(control_for)

    return {'Description': control_node['DTS:Description'], 
            'Input_variable': input_table, 
            'Iterr_variables': vars_list, 
            'SQL': dict_blocks_for} 
    


def parse_control_flow(open_dtsx: dict) -> dict:
    dict_blocks = {}    
    for idx,control_node in enumerate(open_dtsx['DTS:Executables']['DTS:Executable']):

        if control_node['DTS:CreationName'] =='Microsoft.ExecuteSQLTask': # if the task is ExecuteSQL then print query and variables
            dict_blocks[control_node["DTS:refId"]] = pars_sql_task(control_node)
            
        elif control_node['DTS:CreationName'] == 'Microsoft.Pipeline':
            dict_blocks[control_node["DTS:refId"]] = {'Description': control_node['DTS:Description'], 'Index': idx, "Block_name": control_node['DTS:refId'].replace("\\", "@")}

        elif control_node['DTS:CreationName'] =='Microsoft.ExpressionTask': 
            dict_blocks[control_node["DTS:refId"]] = {'Description': control_node['DTS:Description'], 'Expression' : control_node['DTS:ObjectData']['ExpressionTask']['Expression']}

        elif control_node['DTS:CreationName'] =='STOCK:FOREACHLOOP':
            dict_blocks[control_node["DTS:refId"]] = parse_foreach_container(control_node)
            
    # Save the converted dictionary as a JSON file
    with open('output-data/nodes/metadata_nodes_controlflow.json', 'w') as json_file:
        json.dump(dict_blocks, json_file, indent=4)
    return dict_blocks



if __name__=='__main__':
    from modules.dtsx_opener import Load
    import os
    print(os.getcwd())
    path_dtsx = "data/table_var/table_var/Package.dtsx"
    open_dtsx = Load(path_dtsx).run() 
    parse_control_flow(open_dtsx)