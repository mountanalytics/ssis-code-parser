import configparser
import copy
from collections import defaultdict
from collections import OrderedDict 
import pandas as pd
import configparser
import os
import json
import re
import sqlglot
from sqlglot import parse_one, exp
from sqlglot.dialects.tsql import TSQL
from modules.parsers.parse_controlflow import *
from sql_parser.main import main


def find_table_w_spaces(tree: sqlglot.expressions):
    """
    Find all table names which have an empty space in them and storing them without the " " for later use, as sqlglot cannot parse them otherwise.
    """
    table_names = list(tree.find_all(exp.Table))
    space_table = []
    for element in table_names:
        if " " in element.name:
            space_table.append((element.name.replace(" ",""),element.name))
    return space_table


def extract_target_columns(tree: sqlglot.expressions.Select):
    """
    From the query in input, get all the columns from the select statement
    """
    # extract target columns
    select_statement_big = tree.find_all(exp.Select) # find all select statements

    select_statement = []
    for select in list(select_statement_big): # for every select statements, extract the columns
        select_statement += select.expressions 

    target_columns = []
    for select in select_statement: # for every select statement, find all the target columns and add them to list
        columns = list(select.find_all(exp.Column))
        target_columns.append([i for i in columns])

    return select_statement, target_columns


# replace columns aliases
def transformer_functions(node):
    """
    Replaces column objects within the functions with simple column names
    """
    if isinstance(node, exp.Column):
        return parse_one(node.name)
    return node


def extract_transformation(tree: sqlglot.expressions.Select):
    """
    Function to extract possible transformation from columns
    """
    # add possible transformation to columns
    transformations = []

    for col in tree:
        if list(col.find_all(exp.Alias)) == []: # if there are no functions
            transformations.append("")
        else: # else add the function
            transformations.append(col.sql(dialect = "tsql"))

    return transformations

def transformer_functions(node):
    """
    Replaces column objects within the functions with simple column names
    """
    if isinstance(node, exp.Column):
        return parse_one(node.name)
    return node


def replace_variables(query:str, variables: list) -> str: 
    """
    This function replaces the question marks "?" in a query with the corresponding variable, returns the updates query
    """   
    # Convert the string to a list so that we can modify it
    result = list(query)
    
    # Keep track of the replacement index
    replace_index = 0
    
    # Loop through the characters in the string
    for i, char in enumerate(result):
        if char == '?':
            # Replace '?' with the current element in replacements
            if replace_index < len(variables):
                result[i] = "'" + str(variables[replace_index][0]) +"'"
                replace_index += 1
    
    # Convert list back to a string
    return ''.join(result)

def parse_query(query: str):
    """
    Function to convert query string to a sqlglot parsed tree.
    """
    ast = parse_one(query, read="tsql")
    trial1 = repr(ast)
    return ast

def get_statements(transformed_tree):
    """
    Function to extract from expression, join expression and where expression from query
    """

    source_tables = []

    # from expression
    from_exp = list(transformed_tree.find_all(exp.From))
    from_table =str(from_exp[0].this).split(' AS')[0] # table
    source_tables.append(from_table)

    # join expression
    join_exp = list(transformed_tree.find_all(exp.Join))
    if join_exp != []:
        join_table = str(join_exp[0].this).split(' AS')[0] # table
        source_tables.append(join_table)
    else:
        join_exp = None

    # where expression
    where_exp = list(transformed_tree.find_all(exp.Where))
    if where_exp != []:
        where_exp = str(where_exp[0].this).split(' AS')[0]# table
    else:
        where_exp = None

    return source_tables, where_exp


def on_statement(select_statement: sqlglot.expressions.Select):
    """
    Function to extract the on condition from the join statements, (on column = column)
    """

    # from expression
    joins = list(select_statement.find_all(exp.Join))
    on_conditions = []
    for join in joins:
        try:
            on_conditions.append(f"{list(join.find_all(exp.EQ))[0].this.table}.{list(join.find_all(exp.EQ))[0].this.this} = {list(join.find_all(exp.EQ))[0].expression.table}.{list(join.find_all(exp.EQ))[0].expression.this}")
        except:
            return []
        
    if joins != []:
        return on_conditions
    else:
        return None
    

def remove_duplicate_dicts(dict_list:list) -> list:
    """
    This function converts list of dictionaries to a list of frozensets (which are hashable)
    """

    seen = set()
    unique_dicts = []
    
    for d in dict_list:
        t = tuple(sorted(d.items()))  # convert dictionary to a tuple of sorted key-value pairs to make it hashable
        if t not in seen:
            seen.add(t)
            unique_dicts.append(d)
    
    return unique_dicts
    

def extract_source_target_transformation(target_columns :list, lineages: list, space_table:list, source_node_name:str, target_node_name:str):
    """
    Function that returns a list of dictionaries, in which each dictionary contains the list of source columns, the target column and the possible transformation
    """
    for target_column in target_columns:
        source_columns = []

        for source_column in target_column[0]:

            #parse the table and column info
            table = source_column.table
            catalog = source_column.catalog
            db = source_column.db
            column = source_column.name

            for w in space_table:
                if table == w[0]:
                    table = w[1]

            if catalog !="" and db !="":
                source_column_complete = catalog + "." +  db +"." + table +"." +column

            elif catalog == "" and db == "":
                source_column_complete = table +"." +column
            elif catalog == "":    
                source_column_complete = db + "." + table + "."+column
            elif db == "":
                source_column_complete = catalog + "." + table +"." +column

            source_columns.append(source_column_complete)
                
        if source_columns != []:
            lineages.append({'SOURCE_COLUMNS':f'{source_node_name}[{source_columns[0].split(".")[-1]}]', 'TARGET_COLUMN':f'{target_node_name}[{source_columns[0].split(".")[-1]}]', 'TRANSFORMATION': target_column[1]})
    
    return remove_duplicate_dicts(lineages)


def flatten_if_nested(lst:list) -> list:
    """
    Function that flattens nested lists
    """
    if len(lst) == 1 and isinstance(lst[0], list):
        return lst[0]  # Flatten the list
    return lst  # Return the original if not a nested list


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

                        sql_statement = sql_statement.replace(char, f"'{variable[0]}'")# + '?' + sql_statements[i + 1:]

            sql_statement = replace_variables(sql_statement, variables) 

        except:
            pass
    
    try:
        result_set = control_flow[node_name]['Result_variable']
        #print(result_set)
        #print()
    except:
        result_set = None


    #print(sql_statement)
    #print()

    nodes, lineages, variable_tables = main(sql_statement, result_set, nodes, lineages, variable_tables, node_name)

    for i in nodes:
        pass
        #print(i)
    #print()
    for i in lineages:
        pass
        #print(i)
    #print('----------------')
    #print() 

    return nodes, lineages, variable_tables, node_name
    
    
    
    
    """
    # get sqlglot tree from query
    tree = parse_query(sql_statement)

    # only parse if there is a select statement
    if 'select' in sql_statement.lower():

        select = list(tree.find_all(exp.Select))[0]  # find main select statement


        # EXTRACT NODES
        source_tables, where_exp = get_statements(select) # parse source tables and where condition 
        source_tables = [table.replace('"', "") for table in source_tables]

        on_condition = on_statement(select) # parse on condition

        # parse destination table
        insert_tables = [table for table in select.find_all(exp.Insert)]
        insert_tables += [table.this.this.this for table in select.find_all(exp.Into)]
        
        try:
            insert_tables.append(control_flow[node_name]['Result_variable'])
        except:
            pass

        # add source tables to nodes
        for table in source_tables:              
            nodes.append({'NAME_NODE': table,'LABEL_NODE': table, 'FILTER': None, 'FUNCTION': 'DataSources', 'JOIN_ARG': None, 'COLOR': "#42d6a4"})

        # add variables to nodes
        for variable in variables:
            nodes.append({'NAME_NODE': variable[0],'LABEL_NODE': variable[0], 'FILTER': None, 'FUNCTION': 'Variable', 'JOIN_ARG': None, 'COLOR': "#cdd408"})
                
        # add query node
        nodes.append({'NAME_NODE': node_name,'LABEL_NODE': node_name, 'FILTER': where_exp, 'FUNCTION': 'Query', 'JOIN_ARG': on_condition, 'COLOR': '#d0d3d3'})
                
        # add destination table to nodes
        for table in insert_tables:     
            if '::' in table: # if result table is variable
                nodes.append({'NAME_NODE': table,'LABEL_NODE': table, 'FILTER': None, 'FUNCTION': 'Variable', 'JOIN_ARG': None, 'COLOR': "#cdd408"})
            else:
                nodes.append({'NAME_NODE': table,'LABEL_NODE': table, 'FILTER': None, 'FUNCTION': 'DataDestinations', 'JOIN_ARG': None, 'COLOR': "#42d6a4"})

        # EXTRACT LINEAGES
        target_node = insert_tables[0]
    
        source_node = list(source_tables)[0] # CHANGE THIS IN CASE THERE ARE MORE SOURCE TABLES!!!
        query = node_name

        space_table = find_table_w_spaces(select) # clean tables
        space_table = list(set(space_table)) # a list of tuples with table names paired (space removed original - original ) Eg. (OrderDetails, Order Details)

        target_columns = []
        select_statement, target_columns = extract_target_columns(tree) # extract target columns
        
        for table in insert_tables:     
            if '::' in table: # if result table is a variable
                variable_tables[table] = [(col[0].this.this, i) for i, col in enumerate(target_columns)]

        replaced_trees = [x.transform(transformer_functions) for x in select_statement] # replace columns aliases

        transformations = extract_transformation(replaced_trees) # add possible transformation to columns

        target_columns = list(zip(target_columns, transformations)) 

        lineages += extract_source_target_transformation(target_columns, lineages, space_table, source_node, query) # append lineages of node to lineages list
        lineages += extract_source_target_transformation(target_columns, lineages, space_table, query, target_node) # append lineages of node to lineage list                
        
        # append variables to lineages
        for variable in variables: 
            lineages.append({'SOURCE_COLUMNS':f'{variable[0]}[{variable[0]}]', 'TARGET_COLUMN':f"{query}[{variable[0]}]", 'TRANSFORMATION':""})

    elif "insert into" in sql_statement.lower():

        insert = list(tree.find_all(exp.Insert))[0] # find insert statement

        dest_table = [i.this.this for i in insert.find_all(exp.Table)][0]

        schema = [i for i in insert.find_all(exp.Schema)][0]

        columns = []

        for i in tree.args["this"].args["expressions"]:                  
            try:
                columns.append(i.args["this"])
            except:
                pass

        transformations = []

        for i in tree.args["expression"].args["expressions"][0].args["expressions"]:
            try:
                transformations.append(i.args["this"])
            except:
                transformations.append(i)
      
        nodes.append({'NAME_NODE': node_name,'LABEL_NODE': node_name, 'FILTER': None, 'FUNCTION': 'Query', 'JOIN_ARG': None, 'COLOR': "#d0d3d3"})
        nodes.append({'NAME_NODE': dest_table,'LABEL_NODE': dest_table, 'FILTER': None, 'FUNCTION': 'DataDestinations', 'JOIN_ARG': None, 'COLOR': "#42d6a4"})

        for i, column in enumerate(columns):
            lineages.append({'SOURCE_COLUMNS':f'{node_name}[{column}]', 'TARGET_COLUMN':f"{dest_table}[{column}]", 'TRANSFORMATION':transformations[i]})

        

        for transformation in transformations:
            transformation = str(transformation)
            if "::" in transformation and foreach==False:
                nodes.append({'NAME_NODE': transformation,'LABEL_NODE': transformation, 'FILTER': None, 'FUNCTION': 'Variable', 'JOIN_ARG': None, 'COLOR': "#cdd408"})

                lineages.append({'SOURCE_COLUMNS':f'{transformation}[{transformation}]', 'TARGET_COLUMN':f"{node_name}[{column}]", 'TRANSFORMATION':""})
    
    return nodes, lineages, variable_tables, node_name
    """

def foreachloop_parser(control_flow, nodes, lineages, variable_tables, node_name):

    nodes = pd.concat([nodes, pd.DataFrame([{'NAME_NODE': node_name,'LABEL_NODE': node_name, 'FILTER': None, 'FUNCTION': 'ForEachLoopContainer', 'JOIN_ARG': None, 'COLOR': "#d0d3d3"}])], ignore_index=True)

    _, _, _, sql_node_name = executesql_parser(control_flow, nodes, lineages, variable_tables, node_name, True)
    #_, sql_node_name = executesql_parser(control_flow, nodes, lineages, variable_tables, node_name, True)
    variables = control_flow[node_name]['Iterr_variables']
    input_table = control_flow[node_name]['Input_variable']

    for variable_table in variable_tables:
        if input_table == variable_table: # if the variable table correspond to the input table
            for column in variable_tables[variable_table]:
                for variable in variables:
                    if column[1] == variable[1]: # if the indeces of the column and the variable correspond

                        nodes = pd.concat([nodes, pd.DataFrame([{'NAME_NODE': variable[0],'LABEL_NODE': variable[0], 'FILTER': None, 'FUNCTION': 'Variable', 'JOIN_ARG': None, 'COLOR': "#cdd408"}])], ignore_index=True)

                        lineages = pd.concat([lineages, pd.DataFrame([{'SOURCE_COLUMNS':f'{input_table}[{column[0]}]', 'TARGET_COLUMN':f"{node_name}[{column[0]}]", 'TRANSFORMATION':""}])], ignore_index=True) # from the input the table to the foreachloop
                        lineages = pd.concat([lineages, pd.DataFrame([{'SOURCE_COLUMNS':f'{node_name}[{variable[0]}]', 'TARGET_COLUMN':f"{sql_node_name}[{variable[0]}]", 'TRANSFORMATION':""}])], ignore_index=True) # from the foreachloop to the sql
                        
                        lineages = pd.concat([lineages, pd.DataFrame([{'SOURCE_COLUMNS':f'{sql_node_name}[{variable[0]}]', 'TARGET_COLUMN':f"{variable[0]}[{variable[0]}]", 'TRANSFORMATION':""}])], ignore_index=True) # from the foreachloop to the to the variable node
                        lineages = pd.concat([lineages, pd.DataFrame([{'SOURCE_COLUMNS':f'{variable[0]}[{variable[0]}]', 'TARGET_COLUMN':f"{node_name}[{variable[0]}]", 'TRANSFORMATION':""}])], ignore_index=True) # from the variable node back to the foreachloop

    return nodes, lineages

def parse_sql_queries(control_flow:dict, file_name:str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Function that orchestrates the extraction of data from SQL queries in the control flow
    """

    nodes = pd.DataFrame()
    lineages = pd.DataFrame()
    variable_tables = {} 

    # iterate through the control flow nodes and extract and parse SQL statement
    for node in control_flow.keys():
        if control_flow[node]['Description'] == 'Execute SQL Task':
            #nodes, lineages, variable_tables, _ = executesql_parser(control_flow, nodes, lineages, variable_tables, node, False)
            nodes_sql, lin_sql, variable_tables, node_name = executesql_parser(control_flow, nodes, lineages, variable_tables, node, False)
            lineages = pd.concat([lineages,pd.DataFrame(lin_sql)])
            nodes = pd.concat([nodes,pd.DataFrame(nodes_sql)])
        elif control_flow[node]['Description'] == 'Foreach Loop Container':
            nodes, lineages = foreachloop_parser(control_flow, nodes, lineages, variable_tables, node)

    # create nodes and lineages dataframes                   
    nodes_df = pd.DataFrame(nodes).reset_index(drop=True)
    nodes_df['ID'] = nodes_df.index
    nodes_df['COLOR'] = nodes_df.apply(
        lambda row: row['COLOR'] if row['FILTER'] is None or all(pd.isna(x) for x in row['FILTER']) else '#db59a5', 
        axis=1
    )
    nodes_df["NAME_NODE"] = nodes_df["NAME_NODE"].apply(lambda x: str(x).replace("_doublecolumns_","::").replace("'",""))
    nodes_df["LABEL_NODE"] = nodes_df["LABEL_NODE"].apply(lambda x: str(x).replace("_doublecolumns_","::").replace("'",""))
    nodes_df.to_csv(f'output-data/nodes/nodes-{file_name}.csv',index=False) # save nodes file
    lineages_df = pd.DataFrame(lineages)
    lineages_df['SOURCE_COLUMNS'] = lineages_df['SOURCE_COLUMNS'].apply(lambda x: str(x).replace("_doublecolumns_","::").replace("@","").replace("'",""))
    lineages_df['TARGET_COLUMN'] = lineages_df['TARGET_COLUMN'].apply(lambda x: str(x).replace("_doublecolumns_","::").replace("@","").replace("'",""))
    lineages_df['SOURCE_FIELD'] = lineages_df['SOURCE_COLUMNS'].str.extract(r'\[([^\]]*)\]')
    lineages_df['TARGET_FIELD'] = lineages_df['TARGET_COLUMN'].str.extract(r'\[([^\]]*)\]')

    lineages_df['SOURCE_NODE'] = lineages_df['SOURCE_COLUMNS'].str.split('[', expand=True)[0]
    lineages_df['TARGET_NODE'] = lineages_df['TARGET_COLUMN'].str.split('[', expand=True)[0]
    lineages_df['LINK_VALUE'] = 1
    lineages_df['ROW_ID'] = lineages_df.index
    

    # merge source id
    lineages_df = pd.merge(lineages_df, nodes_df[['ID', 'NAME_NODE']], left_on='SOURCE_NODE', right_on = 'NAME_NODE', how='left')
    lineages_df['SOURCE_NODE'] = lineages_df['ID']
    lineages_df.drop(columns=['ID', 'NAME_NODE'], inplace=True)
    # merge target id
    lineages_df = pd.merge(lineages_df, nodes_df[['ID', 'NAME_NODE']], left_on='TARGET_NODE', right_on = 'NAME_NODE', how='left')
    lineages_df['TARGET_NODE'] = lineages_df['ID']
    lineages_df.drop(columns=['ID', 'NAME_NODE'], inplace=True)
    lineages_df = lineages_df.drop_duplicates(subset =['SOURCE_COLUMNS', 'TARGET_COLUMN', 'TRANSFORMATION']).reset_index(drop=True)
    lineages_df['COLOR'] = 'aliceblue'

    lineages_df['COLOR'] = lineages_df.apply(

        lambda row: row['COLOR'] if row['TRANSFORMATION'] == '' or row['TRANSFORMATION'] in nodes_df['NAME_NODE'].values else '#ff6961',

        #lambda row: row['COLOR'] if row['TRANSFORMATION'] == '' or row['TRANSFORMATION'] in nodes_df['NAME_NODE'].values else '#ff6961', # ffb480
        #lambda row: row['COLOR'] if row['TRANSFORMATION'] == '' else ('#ffb480' if row['TRANSFORMATION'] in nodes_df['NAME_NODE'].values else '#ff6961'),
        axis=1
    )
    lineages_df.to_csv(f'output-data/lineages/lineage-{file_name}_cf.csv',index=False) # save lineages file
    return nodes_df, lineages_df


if __name__ == '__main__':
    
    with open('./output-data/nodes/metadata_nodes_controlflow.json', 'r') as json_file: # columns data
        control_flow = json.load(json_file)

    parse_sql_queries(control_flow)

