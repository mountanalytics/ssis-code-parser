 
from modules.dtsx_opener import Load

def pars_sql_task(control_node: dict) -> dict:
    vars_list = []#pd.DataFrame(columns=["Variable"])
    
    try: # try if there are variables
        variables = control_node['DTS:ObjectData']['SQLTask:SqlTaskData']['SQLTask:ParameterBinding']
        
        if type(variables) == list:

            vars_list = [i['SQLTask:DtsVariableName'] for i in control_node['DTS:ObjectData']['SQLTask:SqlTaskData']['SQLTask:ParameterBinding']]
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



    # open dtsx
open_dtsx = Load("data/table_var/table_var/Package.dtsx").run() 
dict_blocks_for = {} 
dict_blocks = {} 
for idx,control_node in enumerate(open_dtsx['DTS:Executables']['DTS:Executable']):
    if control_node['DTS:CreationName'] =='STOCK:FOREACHLOOP':
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
        
        dict_blocks[control_node["DTS:refId"]] = {'Description': control_node['DTS:Description'], 
                                                 'Input_variable': input_table, 
                                                 'Itter_variables': vars_list, 
                                                 'Blocks_in_for': dict_blocks_for} 
    
    
    
    
    
    
    
    