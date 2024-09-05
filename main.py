import json

from helpers.dtsx_opener import Load
from modules.parse_control_func import parse_control_flow

open_dtsx = Load("data/Demo_rabo/Demo_rabo/Demo_SSIS.dtsx").run()
dict_blocks = parse_control_flow(open_dtsx)
with open('output-data/dict_blocks_control.json', 'w') as json_file:
    json.dump(dict_blocks, json_file, indent=4)