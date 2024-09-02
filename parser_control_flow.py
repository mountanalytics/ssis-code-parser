import xmltodict
import pandas as pd
import json

class Load():
    def __init__(self, path):
        self.path = path

    def remove_at_signs(self, obj):
        if isinstance(obj, dict):
            return {key.replace('@', ''): self.remove_at_signs(value) for key, value in obj.items()}
        elif isinstance(obj, list):
            return [self.remove_at_signs(item) for item in obj]
        else:
            return obj
        
    def remove_first_layer(self, json_dict):
        # Extract values from the first layer
        values = list(json_dict.values())
        return values

    def run(self):

        # Path to the XML file
        with open(self.path, 'rb') as f:
            self.xml = f.read()
   
        # open the xml file
        o = xmltodict.parse(self.xml)  # every time you reload the file in colab the key changes (file (1).xml becomes file (2).xml ...)

        json = self.remove_first_layer(self.remove_at_signs(o))[0]

        return json


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


    
if __name__ == "__main__":   
    
    kaggle = Load("data/Demo_rabo/Demo_rabo/Demo_SSIS.dtsx")
    open_dtsx = kaggle.run()
    df_lineage = pd.DataFrame(columns=["ID_block_out","ID_block_in"])
    for const in open_dtsx["DTS:PrecedenceConstraints"]["DTS:PrecedenceConstraint"]:
        df_lineage = pd.concat([df_lineage,pd.DataFrame({"ID_block_out" : [const["DTS:From"]], "ID_block_in": [const["DTS:To"]]})])
    dict_blocks = {}    
    for control_node in open_dtsx['DTS:Executables']['DTS:Executable']:

        if control_node['DTS:CreationName'] =='Microsoft.ExecuteSQLTask': # if the task is ExecuteSQL then print query and variables
            dict_blocks[control_node["DTS:refId"]] = pars_sql_task(control_node)
            
        elif control_node['DTS:CreationName'] == 'Microsoft.Pipeline':
            dict_blocks[control_node["DTS:refId"]] = {'Description': control_node['DTS:Description']}

        elif control_node['DTS:CreationName'] =='Microsoft.ExpressionTask': 
            dict_blocks[control_node["DTS:refId"]] = {'Description': control_node['DTS:Description'], 'Expression' : control_node['DTS:ObjectData']['ExpressionTask']['Expression']}


    # Save the converted dictionary as a JSON file
    with open('output-data/dict_blocks_control.json', 'w') as json_file:
        json.dump(dict_blocks, json_file, indent=4)
    