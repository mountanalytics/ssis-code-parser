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
            
    except: 
        sql_state = control_node['DTS:ObjectData']['SQLTask:SqlTaskData']['SQLTask:SqlStatementSource']
        dict_sql = {
            "Description": control_node['DTS:Description'],
            "SQL_state": sql_state,
            "Variables": None
            }
    return dict_sql

def parse_control_flow(open_dtsx: dict) -> dict:
    dict_blocks = {}    
    for idx,control_node in enumerate(open_dtsx['DTS:Executables']['DTS:Executable']):

        if control_node['DTS:CreationName'] =='Microsoft.ExecuteSQLTask': # if the task is ExecuteSQL then print query and variables
            dict_blocks[control_node["DTS:refId"]] = pars_sql_task(control_node)
            
        elif control_node['DTS:CreationName'] == 'Microsoft.Pipeline':
            dict_blocks[control_node["DTS:refId"]] = {'Description': control_node['DTS:Description'], 'Index': idx, "Block_name": control_node['DTS:refId'].replace("\\", "@")}

        elif control_node['DTS:CreationName'] =='Microsoft.ExpressionTask': 
            dict_blocks[control_node["DTS:refId"]] = {'Description': control_node['DTS:Description'], 'Expression' : control_node['DTS:ObjectData']['ExpressionTask']['Expression']}
    return dict_blocks