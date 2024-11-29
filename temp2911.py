import os 
from modules.dtsx_opener import Load
import re
def run_ssis_parser(folder:str):
   # for package.dtsx in folder
    for file in os.listdir(folder):

        if file.endswith('.dtsx'):

            path_dtsx = folder + file


            open_dtsx = Load(path_dtsx).run() # open dtsx
    return open_dtsx


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




if __name__ == "__main__":
    open_dtsx = run_ssis_parser("data/Demo_rabo/Demo_rabo/")
    df = open_dtsx["DTS:Executables"]["DTS:Executable"][5]["DTS:ObjectData"]["pipeline"]["components"]["component"]
    data_parsed = {}
    for comp in df:
        if comp["componentClassID"] == "Microsoft.DataConvert":
            data_parsed[comp["name"]] = data_conversion_parser(comp)
        elif comp["componentClassID"] == "Microsoft.MergeJoin":
            data_parsed[comp["name"]] = merge_join_parser(comp)
        elif comp["componentClassID"] == "Microsoft.Sort":
            data_parsed[comp["name"]] = sort_parser(comp)
        elif comp["componentClassID"] == "Microsoft.UnPivot":
            data_parsed[comp["name"]] = unpivot_parser(comp)
        elif comp["componentClassID"] == "Microsoft.Multicast":
            data_parsed[comp["name"]] = "Multicast"
            
            
            
            
            
    
    
    
    
    
    