import xmltodict
import pandas as pd
import json
import numpy as np
import re

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
    
    
def excel_source(comp: dict) -> pd.DataFrame:
    mapping = pd.DataFrame(columns=["Column_ext","Column_name"])
    for part_comp in comp["outputs"]["output"]:
        try:
            for columns in part_comp["outputColumns"]["outputColumn"]:
                external_col = columns["externalMetadataColumnId"].split(".ExternalColumns[")[1].replace("]","")
                col_name = columns["name"]
                mapping = pd.concat([mapping, pd.DataFrame({"Column_ext": [external_col], "Column_name": [col_name]})], ignore_index=True)
        except:
            pass
    return mapping
 
def excel_dest(comp: dict) -> pd.DataFrame:
    mapping = pd.DataFrame(columns=["Column_ext","Column_name"])
    try:
        for columns in comp["inputs"]["input"]["inputColumns"]["inputColumn"]:
            external_col = columns["externalMetadataColumnId"].split(".ExternalColumns[")[1].replace("]","")
            col_name = columns["refId"].split(".Columns[")[1].replace("]","")
            mapping = pd.concat([mapping, pd.DataFrame({"Column_ext": [external_col], "Column_name": [col_name]})], ignore_index=True)
    except:
        pass
    return mapping



def derive_column(comp: dict) -> pd.DataFrame:
    df_save = pd.DataFrame(columns=["Column_input", "Column_name", "expression"])
    
    
    # when multiple input columns
    for der_comp in comp["inputs"]["input"]["inputColumns"]["inputColumn"]:

        try:
            for exp in der_comp["properties"]["property"]:
                
                if exp["description"] == "Derived Column Friendly Expression":
                    expression = exp["#text"]
                    df_save = pd.concat([df_save, pd.DataFrame({"Column_name": [der_comp["cachedName"]], "expression": [expression]})], ignore_index=True)
                   
        except:
           pass
        
    # when only one input column
    der_comp = comp["inputs"]["input"]["inputColumns"]["inputColumn"]
    try:
        for exp in der_comp["properties"]["property"]:
            
            if exp["description"] == "Derived Column Friendly Expression":
                expression = exp["#text"]
                df_save = pd.concat([df_save, pd.DataFrame({"Column_input":[der_comp["lineageId"]], "Column_name": [der_comp["cachedName"]], "expression": [expression]})], ignore_index=True)    
    
    except:
        pass
   # ????
    #for der_comp in comp["outputs"]["output"]:
    #    try:
    #        for exp in der_comp["outputColumns"]["outputColumn"]["properties"]["property"]:
    #            if exp["description"] == "Derived Column Friendly Expression":
    #                expression = exp["#text"]
    #                df_save = pd.concat([df_save, pd.DataFrame({"column": [der_comp["outputColumns"]["outputColumn"]["name"]], "expression": [expression]})], ignore_index=True)
    #    except:
    #        pass
        
    return df_save



def ODBC_source(comp: dict) -> pd.DataFrame:
    mapping = pd.DataFrame(columns=["Column_input", "Column_ext","Column_name"])
    
    for props in comp['properties']['property']:
        if props['name'] == 'TableName':
            table_name = props['#text'].replace('"', '')
            
    for part_comp in comp["outputs"]["output"]:
        if part_comp["name"] == "ODBC Source Output":
            try:
                
                for columns in part_comp["outputColumns"]["outputColumn"]:
                    col_in = table_name + '[' +columns["name"]+']'
                    external_col = columns["refId"]
                    col_name = columns["name"]
                    mapping = pd.concat([mapping, pd.DataFrame({"Column_input": [col_in], "Column_ext": [external_col], "Column_name": [col_name]})], ignore_index=True)
            except:
                pass
    return mapping, table_name

def ODBC_dest(comp: dict) -> pd.DataFrame:
    mapping = pd.DataFrame(columns=["Column_input", "Column_ext", "Column_name"])
    try:
        for names in comp["properties"]["property"]:
            if names["description"] == "The name of the table to be fetched.":
                external_table = names["#text"].replace('"', '')

        for columns in comp["inputs"]["input"]["inputColumns"]["inputColumn"]:
            external_col = columns["externalMetadataColumnId"].split(".ExternalColumns[")[1].replace("]","")
            col_name = columns["refId"].split(".Columns[")[1].replace("]","") # column name
            col_in = columns["lineageId"] # input col with lineage
            mapping = pd.concat([mapping, pd.DataFrame({"Column_input": [col_in], "Column_ext": [external_table+"."+external_col], "Column_name": [col_name]})], ignore_index=True)
    except:
        pass
    return mapping, external_table

def lookup(comp: dict) -> dict:
    
    for compon in comp["properties"]["property"]:
        if compon["description"] == "Specifies the SQL statement that generates the lookup table.":
            table = compon["#text"].split("from ")[1].replace("[","").replace("]","")
            
    columns_match = pd.DataFrame(columns=["Column_lookup","Column_name"])
    mapping = pd.DataFrame(columns=["Column_lookup","Column_name"])
    match_col = comp["inputs"]["input"]["inputColumns"]["inputColumn"]["cachedName"]
    
    for ext_match_col in comp["inputs"]["input"]["inputColumns"]["inputColumn"]["properties"]["property"]:
        try: 
            column = ext_match_col["#text"]
        except:
            pass
        
    mapping = pd.concat([mapping, pd.DataFrame({"Column_lookup": [table+"["+column+"]"], "Column_name": [match_col]})], ignore_index=True)
    output_poss = comp["outputs"]["output"]
    
    for outputs in output_poss:
        if outputs["name"] == "Lookup Match Output":
            output_col = outputs["outputColumns"]["outputColumn"]["name"]
            lookup_col = outputs["outputColumns"]["outputColumn"]["properties"]["property"]["#text"]
            columns_match = pd.concat([columns_match, pd.DataFrame({"Column_lookup": [table+"["+lookup_col+"]"], "Column_name": [output_col]})], ignore_index=True)
        elif outputs["name"] == "Lookup No Match Output":
            pass
            #here has to come sth when the output has no match
    
    data_dict = {
        "on": mapping,
        "merged_columns": columns_match
        }
    
    return data_dict, table

def split_cond(comp: dict) -> str:
    for comps in comp["outputs"]["output"]:
        try:
            for cases in comps["properties"]["property"]:
                if cases["description"] == "Specifies the friendly version of the expression. This expression version uses column names.":
                    split_exp = cases["#text"]
        except:
            pass
    return split_exp

def union_all(path_flow: list[dict], comp: dict) -> pd.DataFrame:
    df_lineage_union = pd.DataFrame(columns=["ID_block_out","ID_block_in"])
    mapping = pd.DataFrame(columns=["Column_input","Column_name"])
    for blocks in path_flow:
        if comp["refId"] in blocks["endId"]:
            from_block = blocks["startId"].split(".Outputs")[0]
            to_block = blocks["endId"].split(".Inputs[")[1].replace("]","")
            df_lineage_union = pd.concat([df_lineage_union, pd.DataFrame({"ID_block_out": [from_block], "ID_block_in": [to_block]})], ignore_index=True)
    union_list = list(df_lineage_union["ID_block_in"])
    for union_inp in comp["inputs"]["input"]:
        if union_inp["name"] in union_list:
            index = df_lineage_union[df_lineage_union['ID_block_in'] == union_inp["name"]].index[0]
            prefix = df_lineage_union.loc[index,'ID_block_out']

            for union_comp in union_inp["inputColumns"]["inputColumn"]:
                inp_col = union_comp["cachedName"]
                out_col = union_comp["properties"]["property"]["#text"].split(".Columns[")[1].rstrip("]}")
                
                mapping = pd.concat([mapping, pd.DataFrame({"Column_input": [prefix+"["+inp_col+"]"], "Column_name": [out_col]})], ignore_index=True)
    return mapping
   
def append_ext_tables(ext_table: str, df_nodes: pd.DataFrame) -> pd.DataFrame:
    ext_table = ext_table.replace('"','')
    input_df = pd.DataFrame({"LABEL_NODE": [ext_table], 
                             'ID': [np.nan],
                             'FUNCTION': ["DataSources"],
                             'JOIN_ARG': [np.nan],
                             'NAME_NODE': [ext_table],
                             'FILTER': [np.nan],
                             'COLOR': "black"
                             })
    df_nodes = pd.concat([df_nodes,input_df], ignore_index=True)
    return df_nodes

def append_normal_node(refid: str, func: str, df_nodes: pd.DataFrame) -> pd.DataFrame:
        split_name = refid.split("\\")
        input_df = pd.DataFrame({"LABEL_NODE": [split_name[1]+"@"+split_name[2]], 
                                 'ID': [np.nan],
                                 'FUNCTION': [func],
                                 'JOIN_ARG': [np.nan],
                                 'NAME_NODE': [split_name[2]],
                                 'FILTER': [np.nan],
                                 'COLOR': "black"
                                 })
        df_nodes = pd.concat([df_nodes,input_df], ignore_index=True)
        return df_nodes


def append_join_node(refid: str, func: str, join_argu: str, df_nodes: pd.DataFrame) -> pd.DataFrame:
        split_name = refid.split("\\")
        input_df = pd.DataFrame({"LABEL_NODE": [split_name[1]+"@"+split_name[2]], 
                                 'ID': [np.nan],
                                 'FUNCTION': [func],
                                 'JOIN_ARG': [join_argu],
                                 'NAME_NODE': [split_name[2]],
                                 'FILTER': [np.nan],
                                 'COLOR': "dodgerblue"
                                 })
        df_nodes = pd.concat([df_nodes,input_df], ignore_index=True)
        return df_nodes
    
def vars_df(open_dtsx: dict) -> pd.DataFrame:
    vars_df = pd.DataFrame(columns=["Variable"])
    if len(open_dtsx["DTS:Variables"]["DTS:Variable"]) != 0:
        for var in open_dtsx["DTS:Variables"]["DTS:Variable"]:
            vars_df = pd.concat([vars_df, pd.DataFrame({"Variable" : [var["DTS:Namespace"] + "::" + var["DTS:ObjectName"]]})])
    return vars_df

def append_var_node(var_df: pd.DataFrame, df_nodes: pd.DataFrame) -> pd.DataFrame:
    for var in var_df["Variable"]:
        input_df = pd.DataFrame({"LABEL_NODE": [var], 
                                 'ID': [np.nan],
                                 'FUNCTION': ["Variable"],
                                 'JOIN_ARG': [np.nan],
                                 'NAME_NODE': [var],
                                 'FILTER': [np.nan],
                                 'COLOR': "green"
                                 })
        df_nodes = pd.concat([df_nodes,input_df], ignore_index=True)
    return df_nodes



if __name__ == "__main__":   
    
    kaggle = Load("data/Demo_rabo/Demo_rabo/Demo_SSIS.dtsx")
    open_dtsx = kaggle.run()
    df_lineage = pd.DataFrame(columns=["ID_block_out","ID_block_in", "type_block_out", "type_block_in"])
    df_nodes = pd.DataFrame(columns=['LABEL_NODE', 'ID', 'FUNCTION', 'JOIN_ARG', 'NAME_NODE', 'FILTER', 'COLOR'])
    df_nodes = append_var_node(vars_df(open_dtsx), df_nodes)

    components = open_dtsx["DTS:Executables"]["DTS:Executable"][1]["DTS:ObjectData"]["pipeline"]["components"]["component"]
    path_flow = open_dtsx["DTS:Executables"]["DTS:Executable"][1]["DTS:ObjectData"]["pipeline"]["paths"]["path"]


    for blocks in path_flow:

        # add name blocks
        id_block_out = [blocks["startId"].split(".Outputs")[0]]
        id_block_in = [blocks["endId"].split(".Inputs")[0]]

        # add type blocks
        type_block_in = [component['componentClassID'] for component in components if component['refId'] == id_block_in[0]][0]
        type_block_out = [component['componentClassID'] for component in components if component['refId'] == id_block_out[0]][0]


        df_lineage = pd.concat([df_lineage, pd.DataFrame({"ID_block_out": id_block_out, "ID_block_in": id_block_in, 'type_block_in':type_block_in, 'type_block_out': type_block_out})], ignore_index=True)


    
    dict_blocks = {}
    
    for comp in components:
        if comp["componentClassID"] == "Microsoft.DerivedColumn":
            df_nodes = append_normal_node(comp["refId"], "DerivedColumn", df_nodes)
            dict_blocks[comp["refId"]] = derive_column(comp)
        if comp["componentClassID"] == "Microsoft.RowCount":
            dict_blocks[comp["refId"]] = comp["properties"]["property"]["#text"]
            df_nodes = append_normal_node(comp["refId"], "RowCount", df_nodes)
        if comp["componentClassID"] == "Microsoft.SSISODBCSrc":
            dict_blocks[comp["refId"]],ext_table = ODBC_source(comp)
            df_nodes = append_normal_node(comp["refId"], "SSISODBCSrc", df_nodes)
            df_nodes = append_ext_tables(ext_table, df_nodes)
        if comp["componentClassID"] == "Microsoft.SSISODBCDst":
            dict_blocks[comp["refId"]],ext_table = ODBC_dest(comp)
            df_nodes = append_normal_node(comp["refId"], "SSISODBCDst", df_nodes)
            df_nodes = append_ext_tables(ext_table, df_nodes)
        if comp["componentClassID"] == "Microsoft.Lookup":
            dict_blocks[comp["refId"]], lookup_table = lookup(comp)
            df_nodes = append_ext_tables(lookup_table, df_nodes)
            joinargu = dict_blocks[comp["refId"]]['on'].loc[0,"Column_lookup"]
            joinargu = re.search(r'\[(.*?)\]', joinargu).group(1) + " = " + dict_blocks[comp["refId"]]['on'].loc[0,"Column_name"]
            df_nodes = append_join_node(comp["refId"], "Lookup", joinargu, df_nodes)
        if comp["componentClassID"] == "Microsoft.ConditionalSplit":
            df_nodes = append_join_node(comp["refId"], "ConditionalSplit", split_cond(comp), df_nodes)
            dict_blocks[comp["refId"]]  = split_cond(comp)
 
        if comp["componentClassID"] == "Microsoft.UnionAll":
            dict_blocks[comp["refId"]]  = union_all(path_flow, comp)
            df_nodes = append_normal_node(comp["refId"], "UnionAll", df_nodes)
        
        
        
        if comp["componentClassID"] == "Microsoft.ExcelSource":
            dict_blocks[comp["refId"]]  = excel_source(comp)
        if comp["componentClassID"] == "Microsoft.ExcelDestination":
            dict_blocks[comp["refId"]] = excel_dest(comp) 

            
    df_nodes["ID"] = df_nodes.index        
    df_nodes.to_csv('output-data/nodes_sankey.csv',index=False)   
    ## save data
    """
    def sort_precedeneces(precedences_list, precedences_list_sorted):
    
        # for element in precedence list, recursively add all the node one by one in order
        for i in precedences_list:
            # if the last element of the sorted list is the same as the first element of the list of lists
            if precedences_list_sorted[-1] == i[0]:
    
                precedences_list_sorted.append(i[1]) # append to sorted list
                sort_precedeneces(precedences_list, precedences_list_sorted) # recursively recall the function
    
        return precedences_list_sorted
    
    precedences_list = df_lineage[['ID_block_out', 'ID_block_in']].values.tolist()
    
    list_1 = [i[1] for i in precedences_list] # list with second elements of all pairs

    precedences_list_sorted = []

    # for element in precedence list, 
    for i in precedences_list:
        # if the first node is never a second node then the node is the first one in the sequence container, therefore append both elements to list, as first and second, then keep adding until done
        if i[0] not in list_1:
            precedences_list_sorted.append(i[0])
            precedences_list_sorted.append(i[1])


    print(sort_precedeneces(precedences_list, precedences_list_sorted))
    print()
    """
       
    df_lineage.to_csv('output-data/nodes-order.csv')
    
    ##########################################
    def convert_dataframes(obj):
        """
        Recursively traverse the dictionary and convert DataFrames to JSON-serializable format.
        """
        if isinstance(obj, dict):
            # If the current object is a dictionary, iterate over its keys and values
            return {key: convert_dataframes(value) for key, value in obj.items()}
        elif isinstance(obj, pd.DataFrame):
            # If the current object is a DataFrame, convert it to a list of dictionaries
            return obj.to_dict(orient='records')
        else:
            # If it's neither a dictionary nor a DataFrame, return the object as is
            return obj
    
    # Convert the entire nested structure
    serializable_data = convert_dataframes(dict_blocks)
    
    # Save the converted dictionary as a JSON file
    with open('output-data/dict_blocks.json', 'w') as json_file:
        json.dump(serializable_data, json_file, indent=4)
    