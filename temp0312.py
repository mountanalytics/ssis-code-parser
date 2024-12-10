import pandas as pd
import numpy as np
import glob
import os
import re

def sql_functions() -> list:
    sql_functions = [
        "CURRENT_DATETIME", "NOW", "CURDATE", "CURTIME", "DATE", "DAY", "DAYOFWEEK", "DAYOFYEAR", 
        "HOUR", "MINUTE", "MONTH", "SECOND", "WEEK", "YEAR", "TIMESTAMPADD", "TIMESTAMPDIFF", "CURRENT_TIMESTAMP",
        "CONCAT", "SUBSTRING", "CHAR_LENGTH", "LOWER", "UPPER", "TRIM", "REPLACE", "LPAD", "RPAD", 
        "INSTR", "LEFT", "RIGHT", "MD5", "SHA2", "TO_BASE64", "FROM_BASE64", "AES_ENCRYPT", 
        "AES_DECRYPT", "HEX", "UNHEX", "STRING_BYTES", "FORMAT",
        "ABS", "CEIL", "FLOOR", "ROUND", "MOD", "POWER", "RAND", "SQRT", "EXP", "LOG", 
        "LOG10", "SIGN", "TRUNCATE",
        "COUNT", "SUM", "AVG", "MIN", "MAX", "GROUP_CONCAT", "STDDEV", "VARIANCE",
        "IF", "CASE", "COALESCE", "NULLIF",
        "JSON_EXTRACT", "JSON_SET", "JSON_REMOVE", "JSON_CONTAINS", "JSON_UNQUOTE", "JSON_KEYS", 
        "JSON_LENGTH", "ST_DISTANCE", "ST_INTERSECTS", "ST_AREA", "ST_LENGTH", "ST_CONTAINS", "ST_WITHIN", 
        "ST_ASGEOJSON", "ST_GEOMFROMTEXT",
        "ROW_NUMBER", "RANK", "DENSE_RANK", "NTILE", "LEAD", "LAG", "FIRST_VALUE", "LAST_VALUE",
        "REGEXP", "REGEXP_LIKE", "REGEXP_REPLACE", "REGEXP_SUBSTR",
        "DATABASE", "USER", "VERSION", "CONNECTION_ID", "ROW_COUNT"
        ]
    return sql_functions
# Define the folder path
def hard_coded_transformations(folder_path:str) -> pd.DataFrame:
    csv_files = glob.glob(os.path.join(folder_path, '*.csv'))
    hard_coded = pd.DataFrame(columns=["SOURCE_COLUMNS","TARGET_COLUMN","TRANSFORMATION"])
    for file in csv_files:
        df = pd.read_csv(file)
        unique_columns = list(np.unique(df['SOURCE_FIELD']))
        df = df[df["TRANSFORMATION"].notna()]
        unique_columns.extend(sql_functions())
        hard_codes_file = pd.DataFrame(columns=[['SOURCE_COLUMNS', 'TARGET_COLUMN', 'TRANSFORMATION']])
        for idx, row in df.iterrows():
            parts = re.split(r'\s*[+\-/*]\s*', row["TRANSFORMATION"])
            if len(parts) != 1:  # If there are multiple parts after splitting
                if False in [part.strip() in unique_columns for part in parts]:
                    hard_codes_file = pd.concat([hard_codes_file, pd.DataFrame([row])], ignore_index=True)
            else:
                if not any(col in row["TRANSFORMATION"] for col in unique_columns):
                    hard_codes_file = pd.concat([hard_codes_file, pd.DataFrame([row])], ignore_index=True)
        hard_codes_file = hard_codes_file[["SOURCE_COLUMNS","TARGET_COLUMN","TRANSFORMATION"]]
        hard_coded = pd.concat([hard_coded,hard_codes_file],ignore_index=True)
    return hard_coded


hard_coded_trans = hard_coded_transformations('output-data/lineages')




