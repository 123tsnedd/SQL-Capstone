import psycopg2
import json
from sqlalchemy import create_engine
import pandas as pd
# Connect to the PostgreSQL database


def create_table(name):
    return f" CREATE TABLE IF NOT EXISTS {name}();"


def insert_data(table, params):
    """define and insert data into table"""
    return f""" INSERT INTO {table} {params}"""


def select_data(table):
    """selects data from selected table"""
    return f""" SELECT * FROM {table}"""

def add_column(table, column, data_type):
    """adds column to table"""
    return f""" ALTER TABLE {table} ADD COLUMN {column} {data_type};"""

def not_null_column(table, column):
    '''add column with NOT NULL constraint to a table that already has data. This won't allow any NULL values in the column.'''


def find_transition():
    pass


def open_file(file):
    data = []
    with open(file, "r") as file:
        for line in file:
            line_data = json.loads(line)
            data.append(line_data)
    return data


def extract_msg():
    import json
    #initialize empty list to store processed msg
    extracted_msg = []

    file = "tcsi_dump_2023.json"

    with open(file, 'r') as file:
        for line in file:
            data = json.loads(line)

            #remove 'msg' and merge contents
            if 'msg' in data:
                msg_data = data.pop('msg')
                data.update(msg_data)

            #convert modified dict back to a json
            processed_entry = json.dumps(data)

            #add processed entry to list
            extracted_msg.append(processed_entry)

    # for entry in processed_msg:
    #     print(entry)
    return extracted_msg


# import psycopg2
# from psycopg2 import sql
# import json

# data = extract_msg()
# # PostgreSQL connection parameters
# db_params = {
#     "host": "localhost",
#     "dbname": "data",
#     "user": "postgres",
#     "password": "AstroLab",
#     "port": "5432",
# }

# # Establish a connection to the PostgreSQL database
# conn = psycopg2.connect(**db_params)

# # Create a cursor
# cur = conn.cursor()

# # Iterate through the data and insert it into the database
# #table and columns created in postgresql 
# #ALTER TABLE telemery ADD COLUMN ts TIMESTAMP...;



import psycopg2
from psycopg2 import sql
import json

data = extract_msg()
# PostgreSQL connection parameters
db_params = {
    "host": "localhost",
    "dbname": "data",
    "user": "postgres",
    "password": "AstroLab",
    "port": "5432",
}

# Establish a connection to the PostgreSQL database
conn = psycopg2.connect(**db_params)

# Create a cursor
cur = conn.cursor()

try:
    for item_json in data:
        if 'telpos' in item_json:

            item = json.loads(item_json)  # Parse the JSON string into a dictionary
            insert_query = sql.SQL("INSERT INTO telpos (ts, prio, ec, epoch, ra, dec, el, ha, am, rotoff)\
                VALUES (to_timestamp{}, 'YYYY-MM-DDTHH24:MI:SS.US999999999'), {}, {}, {},{}, {}, {}, {}, {}) ON CONFLICT (prio) DO NOTHING;").format(
                sql.Literal(item['ts']),
                sql.Literal(item['prio']),
                sql.Literal(item['ec']),
                sql.Literal(item['epoch']),
                sql.Literal(item['ra']),
                sql.Literal(item['dec']),
                sql.Literal(item['el']),
                sql.Literal(item['ha']),
                sql.Literal(item['am']),
                sql.Literal(item['rotoff'])
            )
            cur.execute(insert_query)

        elif 'teldata' in item_json:
            item = json.loads(item_json)  # Parse the JSON string into a dictionary
            insert_query = sql.SQL("INSERT INTO teldata (ts, prio, ec, roi, tracking, guiding, slewing, guiderMoving, az, zd, pa, domeAz, domeStat) \
                                   VALUES (to_timestamp({}, 'YYYY-MM-DDTHH24:MI:SS.US999999999'), {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}) ON CONFLICT (ts) DO NOTHING;").format(
                sql.Literal(item['ts']),
                sql.Literal(item['prio']),
                sql.Literal(item['ec']),
                sql.Literal(item['roi']),
                sql.Literal(item['tracking']),
                sql.Literal(item['guiding']),
                sql.Literal(item['slewing']),
                sql.Literal(item['guiderMoving']),
                sql.Literal(item['az']),
                sql.Literal(item['zd']),
                sql.Literal(item['pa']),
                sql.Literal(item['domeAz']),
                sql.Literal(item['domeStat']),
                                   )
            cur.execute(insert_query)

            
        elif 'telvane' in item_json:
            item = json.loads(item_json)  # Parse the JSON string into a dictionary
            insert_query = sql.SQL("INSERT INTO telvane (ts, prio, ec, secz, encz, secx, encx, secy, ency, sech, ench, secv, encv)\
                                   VALUES (to_timestamp({}, 'YYYY-MM-DDTHH24:MI:SS.US999999999'), {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}) ON CONFLICT (ts) DO NOTHING;").format(
                sql.Literal(item['ts']),
                sql.Literal(item['prio']),
                sql.Literal(item['ec']),
                sql.Literal(item['secz']),
                sql.Literal(item['encz']),
                sql.Literal(item['secx']),
                sql.Literal(item['encx']),
                sql.Literal(item['secy']),
                sql.Literal(item['ency']),
                sql.Literal(item['sech']),
                sql.Literal(item['ench']),
                sql.Literal(item['secv']),
                sql.Literal(item['encv']),
            )
            cur.execute(insert_query)

        elif 'telenv' in item_json:
            item = json.loads(item_json)  # Parse the JSON string into a dictionary
            insert_query = sql.SQL("INSERT INTO telenv (ts, prio, ec, tempout, pressure, humidity, wind, winddir, temptruss, tempcell, tempseccell, tempamb, dewpoint)\
                                   VALUES (to_timestamp({}, 'YYYY-MM-DDTHH24:MI:SS.US999999999'), {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}) ON CONFLICT (ts) DO NOTHING;").format(
                sql.Literal(item['ts']),
                sql.Literal(item['prio']),
                sql.Literal(item['ec']),
                sql.Literal(item['tempout']),
                sql.Literal(item['pressure']),
                sql.Literal(item['humidity']),
                sql.Literal(item['wind']),
                sql.Literal(item['winddir']),
                sql.Literal(item['temptruss']),
                sql.Literal(item['tempcell']),
                sql.Literal(item['tempseccell']),
                sql.Literal(item['tempamb']),
                sql.Literal(item['dewpoint']),
            )
            cur.execute(insert_query)

        elif 'telsee' in item_json:
            item = json.loads(item_json)  # Parse the JSON string into a dictionary
            insert_query = sql.SQL("INSERT INTO telsee (ts, prio, ec, dimm_time, dimm_el, dimm_fwhm, dimm_fwhm_corr, mag1_time, mag1_el, mag1_fwhm, mag1_fwhm_corr, mag2_time, mag2_el, mag2_fwhm, mag2_fwhm_corr)\
                                    VALUES (to_timestamp({}, 'YYYY-MM-DDTHH24:MI:SS.US999999999'), {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}) ON CONFLICT (ts) DO NOTHING;").format(
                sql.Literal(item['ts']),
                sql.Literal(item['prio']),
                sql.Literal(item['ec']),
                sql.Literal(item['dimm_time']),
                sql.Literal(item['dimm_el']),
                sql.Literal(item['dimm_fwhm']),
                sql.Literal(item['dimm_fwhm_corr']),
                sql.Literal(item['mag1_time']),
                sql.Literal(item['mag1_el']),
                sql.Literal(item['mag1_fwhm']),
                sql.Literal(item['mag1_fwhm_corr']),
                sql.Literal(item['mag2_time']),
                sql.Literal(item['mag2_el']),
                sql.Literal(item['mag2_fwhm']),
                sql.Literal(item['mag2_fwhm_corr']),
            )
            cur.execute(insert_query)

        elif 'telcat' in item_json:
            item = json.loads(item_json)  # Parse the JSON string into a dictionary
            insert_query = sql.SQL("INSERT INTO telcat (ts, prio, ec, catObj, catRm, catRa, catDec, catEP, catRO)\
                                    VALUES (to_timestamp({}, 'YYYY-MM-DDTHH24:MI:SS.US999999999'), {}, {}, {}, {}, {}, {}, {}, {}) ON CONFLICT (ts) DO NOTHING;").format(
                sql.Literal(item['ts']),
                sql.Literal(item['prio']),
                sql.Literal(item['ec']),
                sql.Literal(item['catObj']),
                sql.Literal(item['catRm']),
                sql.Literal(item['catRa']),
                sql.Literal(item['catDec']),
                sql.Literal(item['catEp']),
                sql.Literal(item['catRo']),
            )
            cur.execute(insert_query)
        conn.commit()
except Exception as e:
    print("error:", str(e), "on line:", item_json)

# Commit the changes and close the cursor and connection
finally:
    cur.close()
    conn.close()
# for data bases creating strategy of WAL_LOG for smaller. FILE_copy might be better for larger. 
#\dt show tables \dt+ show tables with details
# pd.read_sql_querry. read sql querry into a pandas dataframe







