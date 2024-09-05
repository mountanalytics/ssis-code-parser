import json

from helpers.dtsx_opener import Load
from modules.parse_control_func import parse_control_flow
from modules.parse_sql import parse_sql_queries

open_dtsx = Load("data/Demo_rabo/Demo_rabo/Demo_SSIS.dtsx").run()
control_flow = parse_control_flow(open_dtsx)
with open('output-data/dict_blocks_control.json', 'w') as json_file:
    json.dump(control_flow, json_file, indent=4)

parse_sql_queries(control_flow)

