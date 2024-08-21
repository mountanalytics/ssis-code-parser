import xmltodict
import pandas as pd

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



def dervide_column(comp: dict) -> pd.DataFrame:
    df_save = pd.DataFrame(columns=["column_input", "column", "expression"])
    
    
    # when multiple input columns
    for der_comp in comp["inputs"]["input"]["inputColumns"]["inputColumn"]:

        try:
            for exp in der_comp["properties"]["property"]:
                
                if exp["description"] == "Derived Column Friendly Expression":
                    expression = exp["#text"]
                    df_save = pd.concat([df_save, pd.DataFrame({"column": [der_comp["cachedName"]], "expression": [expression]})], ignore_index=True)
                   
        except:
           pass
        
    # when only one input column
    der_comp = comp["inputs"]["input"]["inputColumns"]["inputColumn"]
    try:
        for exp in der_comp["properties"]["property"]:
            
            if exp["description"] == "Derived Column Friendly Expression":
                expression = exp["#text"]
                df_save = pd.concat([df_save, pd.DataFrame({"column_input":[der_comp["lineageId"]], "column": [der_comp["cachedName"]], "expression": [expression]})], ignore_index=True)    
    
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
    
    for property in comp['properties']['property']:
        if property['name'] == 'TableName':
            table_name = property['#text']
            
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
    return mapping

def ODBC_dest(comp: dict) -> pd.DataFrame:
    mapping = pd.DataFrame(columns=["Column_in", "Column_ext", "Column_name"])
    try:
        for columns in comp["inputs"]["input"]["inputColumns"]["inputColumn"]:
            external_col = columns["externalMetadataColumnId"]#.split(".ExternalColumns[")[1].replace("]","")
            col_name = columns["refId"].split(".Columns[")[1].replace("]","") # column name
            col_in = columns["lineageId"] # input col with lineage
            mapping = pd.concat([mapping, pd.DataFrame({"Column_in": [col_in], "Column_ext": [external_col], "Column_name": [col_name]})], ignore_index=True)
    except:
        pass
    return mapping

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
        "mapping": mapping,
        "columns": columns_match
        }
    
    return data_dict

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
    mapping = pd.DataFrame(columns=["Column_in","Column_out"])
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
                
                mapping = pd.concat([mapping, pd.DataFrame({"Column_in": [prefix+"."+inp_col], "Column_out": [out_col]})], ignore_index=True)
    return mapping
   
    
if __name__ == "__main__":   
    kaggle = Load("Demo_rabo/Demo_rabo/Demo_SSIS.dtsx")
    open_dtsx = kaggle.run()
    df_lineage = pd.DataFrame(columns=["ID_block_out","ID_block_in"])
    
    components = open_dtsx["DTS:Executables"]["DTS:Executable"][1]["DTS:ObjectData"]["pipeline"]["components"]["component"]
    path_flow = open_dtsx["DTS:Executables"]["DTS:Executable"][1]["DTS:ObjectData"]["pipeline"]["paths"]["path"]
    for blocks in path_flow:
        df_lineage = pd.concat([df_lineage, pd.DataFrame({"ID_block_out": [blocks["startId"].split(".Outputs")[0]], "ID_block_in": [blocks["endId"].split(".Inputs")[0]]})], ignore_index=True)
    dict_blocks = {}
    for comp in components:
        if comp["componentClassID"] == "Microsoft.DerivedColumn":
            dict_blocks[comp["refId"]] = dervide_column(comp)
        if comp["componentClassID"] == "Microsoft.RowCount":
            dict_blocks[comp["refId"]] = comp["properties"]["property"]["#text"]
        if comp["componentClassID"] == "Microsoft.SSISODBCSrc":
            dict_blocks[comp["refId"]] = ODBC_source(comp)
        if comp["componentClassID"] == "Microsoft.SSISODBCDst":
            dict_blocks[comp["refId"]] = ODBC_dest(comp)
        if comp["componentClassID"] == "Microsoft.Lookup":
            dict_blocks[comp["refId"]] = lookup(comp)
        if comp["componentClassID"] == "Microsoft.ConditionalSplit":
            dict_blocks[comp["refId"]]  = split_cond(comp)
        if comp["componentClassID"] == "Microsoft.UnionAll":
            dict_blocks[comp["refId"]]  = union_all(path_flow, comp)
                        
        
        
        
        if comp["componentClassID"] == "Microsoft.ExcelSource":
            dict_blocks[comp["refId"]]  = excel_source(comp)
        if comp["componentClassID"] == "Microsoft.ExcelDestination":
            dict_blocks[comp["refId"]] = excel_dest(comp) 
       
    