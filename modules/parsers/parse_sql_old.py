import pypyodbc as odbc
import configparser
import copy
from collections import defaultdict
from collections import OrderedDict 
import pandas as pd
import pypyodbc as odbc
import configparser
import os
import json
import re
import sqlglot
from sqlglot import parse_one, exp
from sqlglot.dialects.tsql import TSQL
#from modules.sql_parser.parse_lineages import *
#from modules.sql_parser.parse_nodes import *


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

    target_columns =[]
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


def split_at_last_as(input_string: str):  
    """
    Function to split transformation string at last " AS ", as everything after the last " AS " is the alias, not the transformation
    """
    split_point = input_string.rfind(' AS ')
    if split_point == -1:
        return input_string, ''
    return input_string[:split_point], input_string[split_point + 4:]


# replace columns aliases
def transformer_functions(node):
    """
    Replaces column objects within the functions with simple column names
    """
    if isinstance(node, exp.Column):
        return parse_one(node.name)
    return node


def replace_variables(query:str, variables: list):

    
    # Convert the string to a list so that we can modify it
    result = list(query)
    
    # Keep track of the replacement index
    replace_index = 0
    
    # Loop through the characters in the string
    for i, char in enumerate(result):
        if char == '?':
            # Replace '?' with the current element in replacements
            if replace_index < len(variables):
                result[i] = "'" + str(variables[replace_index]) +"'"
                replace_index += 1
    
    # Convert list back to a string
    return ''.join(result)

def parse_query(query: str):
    """
    Function to convert query string to a sqlglot parsed tree
    """
    ast = parse_one(query, read="tsql")
    trial1 = repr(ast)
    return ast

def find_select(ast):
    selects = list(ast.find_all(exp.Select))   
    return selects

# parse table name + table alias
def parse_tables(table, table_alias_list, subquery=True):    
    """
    Function to parse all table information available (db, catalog...)
    """ 

    if subquery == False:
        table_alias =  table.alias.strip()
        table_name = table.name.strip()
        table_db = table.db.strip()
        table_catalog = table.catalog.strip()

    else:
        table_alias = table.alias.strip()
        source = table.this.args["from"].strip()
        table_name= source.this.name.strip()
        table_catalog =  source.this.catalog.strip()
        table_db = source.this.db.strip()
        
 
    if " " in table_name:
        table_name = table_name.replace(" ", "")
        

    if table_catalog != "" and table_db != "":
        result = (table_catalog+"."+ table_db+"."+table_name, table_alias)

    elif table_db == "" and table_catalog == "":

        result = (table_name, table_alias)

    elif table_catalog == "": 
        result = (table_db+"."+table_name, table_alias)

    elif table_db == "":
        result = (table_catalog+"."+table_name, table_alias)
        

    table_alias_list.append(result)
    return result

def get_tables(ast: sqlglot.expressions.Select):
    """
    Function to extract the table names and their aliases, used to reconstruct a tuple with structure (database+schema+name, alias )
    """
    # find all tables
    table_alias = list(ast.find_all(exp.Table))
    alias_table = []

    # extract information from each table
    for table in table_alias:
        parse_tables(table, alias_table, False)

    return alias_table


def replace_aliases(query:str):
    # replace aliases
    ast = parse_query(query)

    alias_table = get_tables(ast)
    
    def transformer_table(node):
        for element in alias_table:
            if isinstance(node, exp.Column) and node.table == element[1]:
                return parse_one(element[0] + "." + node.name)
        return node

    transformed_tree = ast.transform(transformer_table)

    return transformed_tree


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
    


def remove_duplicate_dicts(dict_list):
    # Convert list of dictionaries to a list of frozensets (which are hashable)
    seen = set()
    unique_dicts = []
    
    for d in dict_list:
        # Convert dictionary to a tuple of sorted key-value pairs to make it hashable
        t = tuple(sorted(d.items()))
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
            if 'AS' in target_column[1]: # if there is an alias, append formula and alias
                for col in source_columns:
                    if split_at_last_as(target_column[1])[0].strip() not in col:

                        lineages.append({'SOURCE_COLUMNS':source_columns, 'TARGET_COLUMN':f"{target_node_name}.{split_at_last_as(target_column[1])[1].strip()}", 'TRANSFORMATION':split_at_last_as(target_column[1])[0].strip()})
                    else:
                        lineages.append({'SOURCE_COLUMNS':source_columns, 'TARGET_COLUMN':f"{target_node_name}.{split_at_last_as(target_column[1])[1].strip()}", 'TRANSFORMATION': ""})
            else:

                lineages.append({'SOURCE_COLUMNS':f'{source_node_name}.{source_columns[0].split(".")[-1]}', 'TARGET_COLUMN':f'{target_node_name}.{source_columns[0].split(".")[-1]}', 'TRANSFORMATION': target_column[1]})
    return remove_duplicate_dicts(lineages)

def flatten_if_nested(lst):
    if len(lst) == 1 and isinstance(lst[0], list):
        return lst[0]  # Flatten the list
    return lst  # Return the original if not a nested list


def parse_sql_queries(control_flow:dict):

    nodes = []
    lineages = []

    variable_tables = {} # dictionaries with temporary tables and their columns

    for node in control_flow.keys():
        if control_flow[node]['Description'] == 'Execute SQL Task':

            try:
                variables = flatten_if_nested(control_flow[node]['Variables'])
            except:
                pass
    

            sql_statement = control_flow[node]['SQL_state'] 

            # if the sql statement contains a variable, change the ? with the variable name
            if '?' in sql_statement: 
                sql_statement = replace_variables(sql_statement, variables)
            
            # get sqlglot tree from query
            tree = parse_query(sql_statement)

            # only parse if there is a select statement
            if 'select' in sql_statement.lower():

                # find main select statement
                select = list(tree.find_all(exp.Select))[0]

                
                # EXTRACT NODES

                # parse source tables and where condition
                source_tables, where_exp = get_statements(select) 
                #print(source_tables)
                
                # parse on condition
                on_condition = on_statement(select)

                # parse destination table
                insert_tables = [table for table in select.find_all(exp.Insert)]
                insert_tables += [table.this.this.this for table in select.find_all(exp.Into)]
                try:
                    insert_tables.append(control_flow[node]['Result_variable'])
                except:
                    pass

                # add source tables to nodes
                for table in source_tables:              
                        nodes.append({'NAME_NODE': table.replace(".", "/"),'LABEL_NODE': table.replace(".", "/"), 'FILTER': None, 'FUNCTION': 'DataSources', 'JOIN_ARG': None, 'COLOR': "gold"})

                # add variables to nodes
                for variable in variables:
                    nodes.append({'NAME_NODE': variable,'LABEL_NODE': variable, 'FILTER': None, 'FUNCTION': 'Variable', 'JOIN_ARG': None, 'COLOR': "green"})
                        
                # add query node
                nodes.append({'NAME_NODE': node,'LABEL_NODE': node, 'FILTER': where_exp, 'FUNCTION': 'Query', 'JOIN_ARG': on_condition, 'COLOR': 'black'})
                        
                # add destination table to nodes
                for table in insert_tables:     
                    if '::' in table: # if result table is variable
                        nodes.append({'NAME_NODE': table,'LABEL_NODE': table, 'FILTER': None, 'FUNCTION': 'Variable', 'JOIN_ARG': None, 'COLOR': "green"})
                    else:
                        nodes.append({'NAME_NODE': table,'LABEL_NODE': table, 'FILTER': None, 'FUNCTION': 'DataDestinations', 'JOIN_ARG': None, 'COLOR': "gold"})
                        


                # EXTRACT LINEAGES
                target_node = insert_tables[0].replace(".", "/")
          
                source_node = list(source_tables)[0].replace(".", "/") # CHANGE THIS IN CASE THERE ARE MORE SOURCE TABLES!!!
                query = node

                space_table = find_table_w_spaces(select) # clean tables

                space_table = list(set(space_table)) # a list of tuples with table names paired (space removed original - original ) Eg. (OrderDetails, Order Details)

                target_columns = []
                select_statement, target_columns = extract_target_columns(tree) # extract target columns
             
                for table in insert_tables:     
                    if '::' in table: # if result table is variable
                        variable_tables[table] = [(col[0].this.this, i) for i, col in enumerate(target_columns)]

                replaced_trees = [x.transform(transformer_functions) for x in select_statement] # replace columns aliases

                # add possible transformation to columns
                transformations = extract_transformation(replaced_trees)
                target_columns = list(zip(target_columns, transformations)) 

                #lineages = []


                lineages += extract_source_target_transformation(target_columns, lineages, space_table, source_node, query) # append lineages of node to lineages list
                lineages += extract_source_target_transformation(target_columns, lineages, space_table, query, target_node) # append lineages of node to lineage list                
                
                for variable in variables: # append variables to lineages
                    lineages.append({'SOURCE_COLUMNS':f'{variable}.{variable}', 'TARGET_COLUMN':f"{query}.{variable}", 'TRANSFORMATION':""})



        elif control_flow[node]['Description'] == 'Foreach Loop Container':
            # nodes
            print(variable_tables)

            nodes.append({'NAME_NODE': node,'LABEL_NODE': node, 'FILTER': None, 'FUNCTION': 'ForEachLoopContainer', 'JOIN_ARG': None, 'COLOR': "gold"})
            
            variables = control_flow[node]['Iterr_variables']
            print(variables)
            input_table = control_flow[node]['Input_variable']


            for i, variable in enumerate(variables):
                for variable_table in variable_tables:
                    print(variable_tables[variable_table][i], variable)
                    

                    if input_table in variable_table and variable_tables[variable_table][i][1] == variable[1]:
                        #print({'SOURCE_COLUMNS':f'{input_table}.{variable_tables[variable_table][i][0]}', 'TARGET_COLUMN':f"{node}.{variable_tables[variable_table][i]}", 'TRANSFORMATION':""})
                        lineages.append({'SOURCE_COLUMNS':f'{input_table}.{variable_tables[variable_table][i][0]}', 'TARGET_COLUMN':f"{node}.{variable_tables[variable_table][i][0]}", 'TRANSFORMATION':""}) # CORRESPONDING COLUMN

                    lineages.append({'SOURCE_COLUMNS':f'{node}.{variable[0]}', 'TARGET_COLUMN':f"{node}.{variable[0]}", 'TRANSFORMATION':""})


                    
    nodes_df = pd.DataFrame(nodes)
    nodes_df['ID'] = nodes_df.index
    nodes_df.to_csv(f'output-data/nodes/nodes-control_flow.csv',index=False)



    lineages_df = pd.DataFrame(lineages)

    lineages_df['SOURCE_FIELD'] = lineages_df['SOURCE_COLUMNS'].str.split('.', expand=True)[1]
    lineages_df['TARGET_FIELD'] = lineages_df['TARGET_COLUMN'].str.split('.', expand=True)[1]

    lineages_df['SOURCE_NODE'] = lineages_df['SOURCE_COLUMNS'].str.split('.', expand=True)[0]
    lineages_df['TARGET_NODE'] = lineages_df['TARGET_COLUMN'].str.split('.', expand=True)[0]

    lineages_df['LINK_VALUE'] = 1
    lineages_df['ROW_ID'] = lineages_df.index
    lineages_df['COLOR'] = 'aliceblue'

    # merge source id
    lineages_df = pd.merge(lineages_df, nodes_df[['ID', 'LABEL_NODE']], left_on='SOURCE_NODE', right_on = 'LABEL_NODE', how='left')
    lineages_df['SOURCE_NODE'] = lineages_df['ID']
    lineages_df.drop(columns=['ID', 'LABEL_NODE'], inplace=True)

    # merge target id
    lineages_df = pd.merge(lineages_df, nodes_df[['ID', 'LABEL_NODE']], left_on='TARGET_NODE', right_on = 'LABEL_NODE', how='left')
    lineages_df['TARGET_NODE'] = lineages_df['ID']
    lineages_df.drop(columns=['ID', 'LABEL_NODE'], inplace=True)

    lineages_df = lineages_df.drop_duplicates(subset =['SOURCE_COLUMNS', 'TARGET_COLUMN', 'TRANSFORMATION']).reset_index(drop=True)

    lineages_df.to_csv(f'output-data/lineages/lineage-control_flow.csv',index=False)



if __name__ == '__main__':
    
    with open('./output-data/nodes/metadata_nodes_controlflow.json', 'r') as json_file: # columns data
        control_flow = json.load(json_file)
    
    parse_sql_queries(control_flow)

