import psycopg2
import json
from sqlalchemy import create_engine
import pandas as pd
# Connect to the PostgreSQL database

db_params = {
        "host": "localhost",
        "dbname": "data",
        "user": "postgres",
        "password": "AstroLab", #this will probably change to be required as an input for security
        "port": "5432",
    }

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
    return f"""ALTER TABLE {table} ADD COLUMN {column} integer NOT NULL;"""

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

def fetch_one_or_all(table, one=None, all=None):
    '''fetch one row from table or all rows from table'''
    connection = psycopg2.connect(db_params)
    cursor = connection.cursor()
    cursor.execute(f"SELECT * FROM {table};")
    try:
        if one:
            result = cursor.fetchone()
            return result
        elif all:
            result = cursor.fetchone()
            return result
    except Exception as err:
        print(err)

def add_search_param():
    pass

def where_clause():
    clause = input("Enter where clause param:")
    return clause

def fetch_data_from_table(selection, table, where= None, limit= None):
    '''retrieve data from table. selection is the column name. where is the where clause. limit is the limit clause.      ex_query = """
    SELECT * FROM {table}
    WHERE ts = %s
    AND EXTRACT(DAY FROM ts) = %s
    AND EXTRACT(MICROSECONDS FROM ts) = %s
    """
    or
    WHERE EXTRACT(DAY FROM ts) = %s 
    AND EXTRACT(MICROSECONDS FROM ts) = %s'''

    connection = psycopg2.connect(**db_params) #** unpacks the dictionary
    cursor = connection.cursor()
    result = None
    try:
        if where:
            if limit:
                cursor.execute(f"SELECT {selection} FROM {table} WHERE {where} LIMIT {limit};")
                result = cursor.fetchall()
                #print(result)
            else: 
                cursor.execute(f"SELECT {selection} FROM {table} WHERE {where};")
                result = cursor.fetchall()
                #print(result)
        if limit:
            cursor.execute(f"SELECT {selection} FROM {table} LIMIT {limit};")
            result = cursor.fetchall()
            #print(result)
        else:
            cursor.execute(f"SELECT {selection} FROM {table};")
            result = cursor.fetchall()
            #print(result)
    except Exception as e:
        print(e)

    finally:
        cursor.close()
        connection.close()
        return result

def custom_query(query):
    '''execute custom query. Don't forget ';' at the end of the query'''
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print(cursor.fetchall())
        return None
    except (Exception, psycopg2.Error) as e:
        print(e)
    
    finally:
        cursor.close()
        connection.close()

def add_file_to_db():
    '''add file to database'''
    import psycopg2
    from psycopg2 import sql
    import json
    
    data = extract_msg()
    # PostgreSQL connection parameters
    # db_params = {
    #     "host": "localhost",
    #     "dbname": "data",
    #     "user": "postgres",
    #     "password": "AstroLab",
    #     "port": "5432",
    # }
    
    # Establish a connection to the PostgreSQL database
    conn = psycopg2.connect(**db_params)
    conn.autocommit = False
    # Create a cursor
    cur = conn.cursor()
    
    try:
        for item_json in data:
            if 'telpos' in item_json:
            
                item = json.loads(item_json)  # Parse the JSON string into a dictionary
                insert_query = sql.SQL("INSERT INTO telpos (ts, prio, ec, epoch, ra, dec, el, ha,   am, rotoff)\
                    VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {}) ON CONFLICT (ts) DO NOTHING;").format(
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
                insert_query = sql.SQL("INSERT INTO teldata (ts, prio, ec, roi, tracking,   guiding, slewing, guiderMoving, az, zd, pa, domeAz, domeStat) \
                                       VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}) ON CONFLICT (ts) DO NOTHING;").format(
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
                insert_query = sql.SQL("INSERT INTO telvane (ts, prio, ec, secz, encz, secx,    encx, secy, ency, sech, ench, secv, encv)\
                                       VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}) ON CONFLICT (ts) DO NOTHING;").format(
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
                insert_query = sql.SQL("INSERT INTO telenv (ts, prio, ec, tempout, pressure,    humidity, wind, winddir, temptruss, tempcell, tempseccell, tempamb, dewpoint)\
                                       VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {},   {}) ON CONFLICT (ts) DO NOTHING;").format(
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
                insert_query = sql.SQL("INSERT INTO telsee (ts, prio, ec, dimm_time, dimm_el,   dimm_fwhm, dimm_fwhm_corr, mag1_time, mag1_el, mag1_fwhm, mag1_fwhm_corr,     mag2_time, mag2_el, mag2_fwhm, mag2_fwhm_corr)\
                                        VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {},  {}, {}, {}, {}) ON CONFLICT (ts) DO NOTHING;").format(
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
                                        VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}) ON  CONFLICT (ts) DO NOTHING;").format(
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
        print(e)
        print(insert_query)
        conn.rollback()
    else:
        conn.commit()
    finally:
        cur.close()
        conn.close()


    # for data bases creating strategy of WAL_LOG for smaller. FILE_copy might be better for    larger. 
    #\dt show tables \dt+ show tables with details
    # pd.read_sql_querry. read sql querry into a pandas dataframe
    
def manual_backup():
    '''
    Perform manual backup of database. Recommended before creating or changing database/tables.
    Database will be backed up to the current working directory unless otherwise specified.
    Recommended to save backup in different location of primary database. 
    '''
    import os
    import datetime
    try:
        #define backup
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        backUp_path = os.getcwd()+f"/backup_{timestamp}.sql"
        # run pg_dump to backup database
        #backup_command = f"C:\\Program_Files\\PostgreSQL\\16\\bin\\pg_dump.exe -U postgres -F t data > {backUp_path}"
        backup_command = f"C:/ProgramFiles/PostgreSQL/16/bin/pg_dump -U {db_params['user']} -h {db_params['host']} -p {db_params['port']} {db_params['dbname']} -F t data > {backUp_path}"
        os.system(backup_command)
        #print(f"Backup created at {backUp_path}")

    except:
        print("Backup failed")


if __name__ == "__main__":
#######################################
    print("Welcome to the kickass database")
    print("Please select an option:")


    run = True
    while run:
        try:
            print("1. Add file to database")
            print("2. Fetch data from database")
            print("3. Custom type query")
            print("4. Manual Backup")
            print("5. Exit")
            option = int(input("Enter option number: "))
            if option == 2:
                limit = input("Enter limit; leave blank if None: ")
                where = input("Enter where clause; leave blank if None: ")
                data = fetch_data_from_table('*', 'telsee', limit= limit, where= where)
                #print(data)
                if data is None:
                    print("No data found")
                    continue
                else:
                    print(data)
                    cont = input('New search? (y/n): ')
                    if cont.lower() == 'y':
                        continue
                    else:
                        print('Goodbye bitches')
                        run = False
            elif option == 3:
                query = input("Please enter custom query: ")
                custom_query(query)

            elif option == 4:
                manual_backup()
            elif option == 5:
                print("Goodbye")
                exit()
                break
            else:
                print("Invalid input, select from numbers given above")

        except ValueError as val_err:
            print(val_err)
            print("Invalid Input, Please enter a number from the options above")

