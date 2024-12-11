import xmltodict
import pandas as pd
import json
import numpy as np
import re

    
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
    df_save = pd.DataFrame(columns=["column", "expression"])
    try:
        for der_comp in comp["inputs"]["input"]["inputColumns"]["inputColumn"]:

        
            for exp in der_comp["properties"]["property"]:
                
                if exp["description"] == "Derived Column Friendly Expression":
                    expression = exp["#text"]
                    df_save = pd.concat([df_save, pd.DataFrame({"column": [der_comp["cachedName"]], "expression": [expression]})], ignore_index=True)
                    
    except:
        pass
    try:
        der_comp = comp["inputs"]["input"]["inputColumns"]["inputColumn"]
        for exp in der_comp["properties"]["property"]:
            
            if exp["description"] == "Derived Column Friendly Expression":
                expression = exp["#text"]
                df_save = pd.concat([df_save, pd.DataFrame({"column": [der_comp["cachedName"]], "expression": [expression]})], ignore_index=True)    
    
    except:
        pass
    for der_comp in comp["outputs"]["output"]:
        try:
            for exp in der_comp["outputColumns"]["outputColumn"]["properties"]["property"]:
                if exp["description"] == "Derived Column Friendly Expression":
                    expression = exp["#text"]
                    df_save = pd.concat([df_save, pd.DataFrame({"column": [der_comp["outputColumns"]["outputColumn"]["name"]], "expression": [expression]})], ignore_index=True)
        except:
            pass
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


def data_conversion_parser(comp: dict) -> list[dict]:
    conversions = []
    if type(comp["inputs"]["input"]["inputColumns"]["inputColumn"]) == list:
        for i in range(len(comp["outputs"]["output"])):
            input_col = comp["inputs"]["input"]["inputColumns"]["inputColumn"][i]["cachedName"]
            input_type = comp["inputs"]["input"]["inputColumns"]["inputColumn"][i]["cachedDataType"]
            out_col = comp["outputs"]["output"][0]["outputColumns"]["outputColumn"][i]["name"]
            out_type = comp["outputs"]["output"][0]["outputColumns"]["outputColumn"][i]["dataType"]
            conversions.append({
                            "input_column": input_col,
                            "input_type": input_type,
                            "output_column": out_col,
                            "output_type": out_type
                            })
    else:
        input_col = comp["inputs"]["input"]["inputColumns"]["inputColumn"]["cachedName"]
        input_type = comp["inputs"]["input"]["inputColumns"]["inputColumn"]["cachedDataType"]
        out_col = comp["outputs"]["output"][0]["outputColumns"]["outputColumn"]["name"]
        out_type = comp["outputs"]["output"][0]["outputColumns"]["outputColumn"]["dataType"]
        conversions.append({
                        "input_column": input_col,
                        "input_type": input_type,
                        "output_column": out_col,
                        "output_type": out_type
                        })
    return conversions

def merge_join_parser(comp: dict) -> dict:
    mapping = []
    match int(comp["properties"]["property"][0]["#text"]):
        case 0:
            join_type = "Full outer join"
        case 1:
            join_type = "Left outer join"
        case 2:
            join_type = "Inner join"
    for cols in comp["outputs"]["output"]["outputColumns"]["outputColumn"]:
        output_col = cols["name"]
        match = re.search(r'Columns\[(.*?)\]', cols["properties"]["property"]["#text"])
        if match:
            input_col = match.group(1)
        mapping.append((input_col,output_col))
    dict_join = {
                "join_type": join_type,
                "col_mapping": mapping}
    return dict_join

def sort_parser(comp: dict) -> dict:
    output_cols = []
    delete_dup_rows = comp["properties"]["property"][0]["#text"]
    for cols in comp["outputs"]["output"]["outputColumns"]["outputColumn"]:
        output_cols.append(cols["name"])
    sort_list = []
    for sort_col in comp["inputs"]["input"]["inputColumns"]["inputColumn"]:
        if int(sort_col["properties"]["property"][1]["#text"]) != 0:
            sort_order = abs(int(sort_col["properties"]["property"][1]["#text"]))
            match int(sort_col["properties"]["property"][1]["#text"]):
                case value if value > 0:
                    sort_type = "ascending"
                case value if value < 0:
                    sort_type = "descending"
            sort_list.append({
                        "column": sort_col["cachedName"],
                        "sorting_order": sort_order,
                        "sorting_type" : sort_type})         
    dict_sort = {
                "sort_arguments": sort_list,
                "output_columns": output_cols,
                "drop_duplicate": delete_dup_rows}
    
    return dict_sort

def unpivot_parser(comp: dict) -> dict:
    mapping = []
    pivot_cols = []
    for cols in comp["inputs"]["input"]["inputColumns"]["inputColumn"]:
        input_col = cols["cachedName"]
        match = re.search(r'Columns\[(.*?)\]', cols["properties"]["property"][0]["#text"])
        if match:
            output_col = match.group(1)
        mapping.append((input_col,output_col))
        if input_col != output_col:
            pivot_cols.append(input_col)
    for cols in comp["outputs"]["output"]["outputColumns"]["outputColumn"]:
        if cols["properties"]["property"]["#text"] == "true":
            pivot_col = cols["name"]
            break
    dict_pivot = {"pivot_attributes": {"pivot_column": pivot_col,
                                       "pivot_entries": pivot_cols},  
                  "column_mapping": mapping
                  }
    return dict_pivot


def aggregate_parser(comp: dict) -> dict:
    dict_blocks = {}
    for comps in comp["outputs"]["output"]["outputColumns"]["outputColumn"]:
        for props in comps["properties"]["property"]:
            if props["name"] == "AggregationType":
                match int(props["#text"]):
                    case 0:
                        agg_type = "Group by"
                    case 1:
                        agg_type = "Count"
                    case 3:
                        agg_type = "Count distinct"
                dict_blocks[comps["name"]] = agg_type
                break
    return dict_blocks


def append_ext_tables(ext_table: str, df_nodes: pd.DataFrame, func = 'DataSources') -> pd.DataFrame:
    ext_table = ext_table.replace('"','')
    input_df = pd.DataFrame({"LABEL_NODE": [ext_table], 
                             'ID': [np.nan],
                             'FUNCTION': [func],
                             'JOIN_ARG': [np.nan],
                             'SPLIT_ARG': [np.nan],
                             'NAME_NODE': [ext_table],
                             'FILTER': [np.nan],
                             'SORT': [np.nan],
                             'PIVOT': [np.nan],
                             'AGGREGATE': [np.nan],
                             'COLOR': "#42d6a4"
                             })
    df_nodes = pd.concat([df_nodes,input_df], ignore_index=True)
    return df_nodes

def append_normal_node(refid: str, func: str, df_nodes: pd.DataFrame) -> pd.DataFrame:
    split_name = refid.split("\\")
    input_df = pd.DataFrame({"LABEL_NODE": [split_name[1]+"@"+split_name[2]], 
                                'ID': [np.nan],
                                'FUNCTION': [func],
                                'JOIN_ARG': [np.nan],
                                'SPLIT_ARG': [np.nan],
                                'NAME_NODE': [split_name[2]],
                                'FILTER': [np.nan],
                                'SORT': [np.nan],
                                'PIVOT': [np.nan],
                                'AGGREGATE': [np.nan],
                                'COLOR': "#9d94ff" if func == "UnionAll"  else "#d0d3d3"
                                })
    df_nodes = pd.concat([df_nodes,input_df], ignore_index=True)
    return df_nodes


def append_join_node(refid: str, func: str, join_argu: str, df_nodes: pd.DataFrame) -> pd.DataFrame:
    split_name = refid.split("\\")
    input_df = pd.DataFrame({"LABEL_NODE": [split_name[1]+"@"+split_name[2]], 
                                'ID': [np.nan],
                                'FUNCTION': [func],
                                'JOIN_ARG': [join_argu],
                                'SPLIT_ARG': [np.nan],
                                'NAME_NODE': [split_name[2]],
                                'FILTER': [np.nan],
                                'SORT': [np.nan],
                                'PIVOT': [np.nan],
                                'AGGREGATE': [np.nan],
                                'COLOR': "#9d94ff"
                                })
    df_nodes = pd.concat([df_nodes,input_df], ignore_index=True)
    return df_nodes


def append_split_node(refid: str, func: str, split_argu: str, df_nodes: pd.DataFrame) -> pd.DataFrame:
    split_name = refid.split("\\")
    input_df = pd.DataFrame({"LABEL_NODE": [split_name[1]+"@"+split_name[2]], 
                                'ID': [np.nan],
                                'FUNCTION': [func],
                                'JOIN_ARG': [np.nan],
                                'SPLIT_ARG': [split_argu],
                                'NAME_NODE': [split_name[2]],
                                'FILTER': [np.nan],
                                'SORT': [np.nan],
                                'PIVOT': [np.nan],
                                'AGGREGATE': [np.nan],
                                'COLOR': "#9d94ff"
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
                                 'SPLIT_ARG': [np.nan],
                                 'NAME_NODE': [var],
                                 'FILTER': [np.nan],
                                 'SORT': [np.nan],
                                 'PIVOT': [np.nan],
                                 'AGGREGATE': [np.nan],
                                 'COLOR': "#cdd408"
                                 })
        df_nodes = pd.concat([df_nodes,input_df], ignore_index=True)
    return df_nodes

def append_sort_node(refid: str, func: str, sort_argu: str, df_nodes: pd.DataFrame) -> pd.DataFrame:
    split_name = refid.split("\\")
    input_df = pd.DataFrame({"LABEL_NODE": [split_name[1]+"@"+split_name[2]], 
                                'ID': [np.nan],
                                'FUNCTION': [func],
                                'JOIN_ARG': [np.nan],
                                'SPLIT_ARG': [np.nan],
                                'NAME_NODE': [split_name[2]],
                                'FILTER': [np.nan],
                                'SORT': sort_argu,
                                'PIVOT': [np.nan],
                                'AGGREGATE': [np.nan],
                                'COLOR': "#4fbdb9"
                                })
    df_nodes = pd.concat([df_nodes,input_df], ignore_index=True)
    return df_nodes

def append_pivot_node(refid: str, func: str, pivot_argu: str, df_nodes: pd.DataFrame) -> pd.DataFrame:
    split_name = refid.split("\\")
    input_df = pd.DataFrame({"LABEL_NODE": [split_name[1]+"@"+split_name[2]], 
                                'ID': [np.nan],
                                'FUNCTION': [func],
                                'JOIN_ARG': [np.nan],
                                'SPLIT_ARG': [np.nan],
                                'NAME_NODE': [split_name[2]],
                                'FILTER': [np.nan],
                                'SORT': [np.nan],
                                'PIVOT': pivot_argu,
                                'AGGREGATE': [np.nan],
                                'COLOR': "#8b6fae"
                                })
    df_nodes = pd.concat([df_nodes,input_df], ignore_index=True)
    return df_nodes

def append_aggregate_node(refid: str, func: str, arg_argu: str, df_nodes: pd.DataFrame) -> pd.DataFrame:
    split_name = refid.split("\\")
    input_df = pd.DataFrame({"LABEL_NODE": [split_name[1]+"@"+split_name[2]], 
                                'ID': [np.nan],
                                'FUNCTION': [func],
                                'JOIN_ARG': [np.nan],
                                'SPLIT_ARG': [np.nan],
                                'NAME_NODE': [split_name[2]],
                                'FILTER': [np.nan],
                                'SORT': [np.nan],
                                'PIVOT': [np.nan],
                                'AGGREGATE': arg_argu,
                                'COLOR': "#d6f6ff"
                                })
    df_nodes = pd.concat([df_nodes,input_df], ignore_index=True)
    return df_nodes

def lineage_path_flow(path_flow: list, components: list, df_name: str):
    """
    Function that extracts nodes order and branches of flow
    """
    df_lineage = pd.DataFrame(columns=["ID_block_out","ID_block_in", "type_block_out", "type_block_in"])
    marker_block = []
    for blocks in path_flow:
        # add name blocks
        id_block_out = [blocks["startId"].split(".Outputs")[0]]
        id_block_in = [blocks["endId"].split(".Inputs")[0]]

        # add type blocks
        type_block_in = [component['componentClassID'] for component in components if component['refId'] == id_block_in[0]][0]
        type_block_out = [component['componentClassID'] for component in components if component['refId'] == id_block_out[0]][0]
        df_lineage = pd.concat([df_lineage, pd.DataFrame({"ID_block_out": id_block_out, "ID_block_in": id_block_in, 'type_block_in':type_block_in, 'type_block_out': type_block_out})], ignore_index=True)
        if blocks["name"] == "Lookup No Match Output":
            marker_block.append(blocks["endId"].split(".Inputs")[0])
        
    marker_block = pd.DataFrame(marker_block, columns=["NAME"]).to_csv(f"output-data/nodes/marker_nodes-{df_name}.csv", index=False)
    df_lineage.to_csv(f'output-data/nodes/order_nodes-{df_name}.csv')
    return marker_block, df_lineage

def main_parser(components: list, df_nodes: pd.DataFrame, path_flow: list, df_name: str) -> tuple[dict, pd.DataFrame]:
    """
    Function that orchestrates the parsing of the nodes of a dataflow
    """

    dict_blocks = {}
    
    for comp in components:
        # parse derived column nodes
        if comp["componentClassID"] == "Microsoft.DerivedColumn":
            df_nodes = append_normal_node(comp["refId"], "DerivedColumn", df_nodes)
            dict_blocks[comp["refId"]] = derive_column(comp)
        # parse row count nodes
        if comp["componentClassID"] == "Microsoft.RowCount":
            dict_blocks[comp["refId"]] = comp["properties"]["property"]["#text"]
            df_nodes = append_normal_node(comp["refId"], "RowCount", df_nodes)
        # parse source nodes
        if comp["componentClassID"] == "Microsoft.SSISODBCSrc":
            dict_blocks[comp["refId"]],ext_table = ODBC_source(comp)
            df_nodes = append_normal_node(comp["refId"], "SSISODBCSrc", df_nodes)
            df_nodes = append_ext_tables(ext_table, df_nodes)
        # parse destination nodes
        if comp["componentClassID"] == "Microsoft.SSISODBCDst":
            dict_blocks[comp["refId"]],ext_table = ODBC_dest(comp)
            df_nodes = append_normal_node(comp["refId"], "SSISODBCDst", df_nodes)
            df_nodes = append_ext_tables(ext_table, df_nodes, 'DataDestinations')
        # parse lookup nodes
        if comp["componentClassID"] == "Microsoft.Lookup":
            dict_blocks[comp["refId"]], lookup_table = lookup(comp)
            df_nodes = append_ext_tables(lookup_table, df_nodes)
            joinargu = dict_blocks[comp["refId"]]['on'].loc[0,"Column_lookup"]
            joinargu = re.search(r'\[(.*?)\]', joinargu).group(1) + " = " + dict_blocks[comp["refId"]]['on'].loc[0,"Column_name"]
            df_nodes = append_join_node(comp["refId"], "Lookup", joinargu, df_nodes)
        # parse conditional split nodes
        if comp["componentClassID"] == "Microsoft.ConditionalSplit":
            df_nodes = append_split_node(comp["refId"], "ConditionalSplit", split_cond(comp), df_nodes)
            dict_blocks[comp["refId"]]  = split_cond(comp)
        # parse union nodes
        if comp["componentClassID"] == "Microsoft.UnionAll":
            dict_blocks[comp["refId"]]  = union_all(path_flow, comp)
            df_nodes = append_normal_node(comp["refId"], "UnionAll", df_nodes)
        # parse excel source nodes
        if comp["componentClassID"] == "Microsoft.ExcelSource":
            dict_blocks[comp["refId"]]  = excel_source(comp)
        # parse excel source destinations
        if comp["componentClassID"] == "Microsoft.ExcelDestination":
            dict_blocks[comp["refId"]] = excel_dest(comp) 
        if comp["componentClassID"] == "Microsoft.DataConvert":
            dict_blocks[comp["refId"]] = data_conversion_parser(comp)
            df_nodes = append_normal_node(comp["refId"], "DataConvert", df_nodes)
        elif comp["componentClassID"] == "Microsoft.MergeJoin":
            dict_blocks[comp["refId"]] = merge_join_parser(comp)
            df_nodes = append_join_node(comp["refId"], "Lookup", dict_blocks[comp["refId"]]["join_type"], df_nodes)
        elif comp["componentClassID"] == "Microsoft.Sort":
            dict_blocks[comp["refId"]] = sort_parser(comp)
            sort_argu = "<br />".join(
            f"{row['sorting_order']}. {row['column']} {'ASC' if row['sorting_type'] == 'ascending' else 'DESC'}"
            for _, row in pd.DataFrame(dict_blocks[comp["refId"]]["sort_arguments"]).sort_values(by='sorting_order').iterrows()
            )
            sort_argu += "<br />Drop duplicate rows: " + dict_blocks[comp["refId"]]["drop_duplicate"] 
            df_nodes = append_sort_node(comp["refId"], "Sort", sort_argu, df_nodes)
        elif comp["componentClassID"] == "Microsoft.UnPivot":
            dict_blocks[comp["refId"]] = unpivot_parser(comp)
            df_nodes = append_pivot_node(comp["refId"], "Unpivot",f'Pivot column: {dict_blocks[comp["refId"]]["pivot_attributes"]["pivot_column"]}', df_nodes)
        elif comp["componentClassID"] == "Microsoft.Multicast":
            dict_blocks[comp["refId"]] = "Multicast"
            df_nodes = append_normal_node(comp["refId"], "Multicast", df_nodes)
        elif comp["componentClassID"] == "Microsoft.Aggregate":
            dict_blocks[comp["refId"]] = aggregate_parser(comp)
            df_nodes = append_aggregate_node(comp["refId"], "Aggregate", "<br />".join([f"Column: {key}, Aggregation type: {value}" for key, value in dict_blocks[comp["refId"]].items()]), df_nodes)
    df_nodes["ID"] = df_nodes.index        
    df_nodes.to_csv(f'output-data/nodes/nodes-{df_name}.csv',index=False)
    return dict_blocks, df_nodes


def convert_dataframes(obj: dict) -> dict:
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
    

def parser_dataflow_nodes(open_dtsx: dict, index: int, df_name: str):
    df_nodes = pd.DataFrame(columns=['LABEL_NODE', 'ID', 'FUNCTION', 'JOIN_ARG', 'SPLIT_ARG', 'NAME_NODE', 'FILTER', 'SORT', 'PIVOT', 'COLOR'])
    df_nodes = append_var_node(vars_df(open_dtsx), df_nodes)


    components = open_dtsx["DTS:Executables"]["DTS:Executable"][index]["DTS:ObjectData"]["pipeline"]["components"]["component"]

    path_flow = open_dtsx["DTS:Executables"]["DTS:Executable"][index]["DTS:ObjectData"]["pipeline"]["paths"]["path"]

    marker, order_nodes = lineage_path_flow(path_flow, components, df_name) # 
    dict_blocks, nodes_df = main_parser(components, df_nodes, path_flow, df_name) # parse nodes
    dict_blocks = convert_dataframes(dict_blocks) 
    
    # Save the converted dictionary as a JSON file
    with open(f'output-data/nodes/metadata_nodes_dataflow_{df_name}.json', 'w') as json_file:
        json.dump(dict_blocks, json_file, indent=4)

    return dict_blocks, nodes_df, order_nodes, marker

    
    
    
    