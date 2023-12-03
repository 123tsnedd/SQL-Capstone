import psycopg2
import json
import os
import table_file as tf
import logging
import logging.handlers

#config log
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO) #logs all messages with level INFO and above

#handlers
console_handler = logging.StreamHandler() #logs to console
file_handler = logging.FileHandler('sqlFile.log') #logs to file
console_handler.setLevel(logging.WARNING) #logs all messages with level WARNING and above
file_handler.setLevel(logging.INFO) #logs all messages with level INFO and above

#create formatters and add to handlers
console_format = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
file_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(console_format)
file_handler.setFormatter(file_format)

#add handlers to logger
logger.addHandler(console_handler)
logger.addHandler(file_handler)


# Connect to the PostgreSQL database

db_params = {
        "host": "localhost",
        "dbname": "data",
        "user": "postgres",
        "password": "AstroLab", #this will probably change to be required as an input for security
        "port": "5432",
    }

tables = ['telpos', 'teldata', 'telenv', 'telcat', 'telsee', 'telvane', 'observer']

def delete_all_tables():
    try:
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        for table in tables:
            cur.execute(f"DROP TABLE IF EXISTS {table};")
            conn.commit()
    except Exception as e:
        print(e)
        logger.info(e)
        conn.rollback()
        print('Tables not deleted')
    finally:
        print("All Tables deleted")
        logger.info("All Tables deleted")
        conn.close()
        cur.close()

def insert_specific_data(table, params):
    """insert data into specific table. """
    return f""" INSERT INTO {table} {params}"""

def query_data(select, table, subquery=None, where=None, limit=None):
    """selects data from selected table"""
    return f""" SELECT {select} FROM {table}"""

def add_column(table, column, data_type):
    """adds column to table"""
    return f""" ALTER TABLE {table} ADD COLUMN {column} {data_type};"""

def not_null_column(table, column):
    '''add column with NOT NULL constraint to a table that already has data. This won't allow any NULL values in the column.'''
    return f"""ALTER TABLE {table} ADD COLUMN {column} integer NOT NULL;"""

def get_schemas():
    try:
        with psycopg2.connect(**db_params) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT schema_name FROM information_schema.schemata;")
                schemas = cur.fetchall()
        return schemas
    except Exception as e:
        print(e)
    finally:
        cur.close()
        conn.close()

def open_file(file= all):
    data = []
    if file == all:
        print(file)
        return file
    else:
        import os
        files = os.listdir('./data_files')
        return files

def extract_msg():
    import json
    #initialize empty list to store processed msg
    extracted_msg = []

    #will be a steam of data from dump file
    #files = "tcsi_dump_2023.json"
    usr_input = input("Enter file name or press enter to select all files: ")
    files = open_file(usr_input)
    print(f'files: {files}')
    for file in files:
        print('file', file)
        file = os.path.join('./data_files', file)
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
    print(f'length of extracted_msg: {len(extracted_msg)}')
    return extracted_msg

def where_clause():
    clause = input("Enter where clause param:")
    return clause

def get_columns(table):
    '''get column names from table'''
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()
    cursor.execute(f"SELECT * FROM {table};")
    try:
        result = [desc[0] for desc in cursor.description]
        return result
    except Exception as err:
        print(err)
    finally:
        cursor.close()
        connection.close()

def fetch_data(select, table, where= None, limit= None, As= None):
    '''retrieve data from table. select is the column name. where is the where clause. limit is the limit clause.      ex_query = . Can add subquaries in the WhHERE clause. ex: WHERE ts IN (SELECT ts FROM telpos WHERE EXTRACT(DAY FROM ts) = 1 AND EXTRACT(MICROSECONDS FROM ts) = 0)
    SELECT {select} FROM {table}
    WHERE {where} = %s
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
                cursor.execute(f"SELECT {select} FROM {table} WHERE {where} LIMIT {limit};")
                result = cursor.fetchall()
                #print(result)
            else: 
                cursor.execute(f"SELECT {select} FROM {table} WHERE {where};")
                result = cursor.fetchall()
                #print(result)
        if limit:
            cursor.execute(f"SELECT {select} FROM {table} LIMIT {limit};")
            result = cursor.fetchall()
            #print(result)
        else:
            cursor.execute(f"SELECT {select} FROM {table};")
            result = cursor.fetchall()
            #print(result)
    except Exception as e:
        print(e)

    finally:
        columns = get_columns(table)
        cursor.close()
        connection.close()
        return columns, result

def custom_query(query):
    '''execute custom query. Don't forget ';' at the end of the query'''
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        if cursor.description: #if cursor has results
            #print(cursor.fetchall())
            result = cursor.fetchall()
                
    except (Exception, psycopg2.Error) as e:
        logger.exception(e)
        print(e)
    
    finally:
        cursor.close()
        connection.close()
    print('results: ', result)
    return result

def add_file_to_db():
    '''add file to database'''
    import psycopg2
    from psycopg2 import sql
    import json
    
    data = extract_msg()
    print(f'data length: {len(data)}')
    # Establish a connection to the PostgreSQL database
    conn = psycopg2.connect(**db_params)
    conn.autocommit = False
    # Create a cursor
    cur = conn.cursor()
    
    try:
        for item_json in data:
            if 'telpos' in item_json:    
                item = json.loads(item_json)  # Parse the JSON string into a dictionary
                insert_query = sql.SQL("INSERT INTO telpos (ts, prio, ec, epoch, ra, dec, el, ha, am, rotoff)\
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
                print('Added to telpos')

            elif 'teldata' in item_json:
                item = json.loads(item_json)  # Parse the JSON string into a dictionary
                insert_query = sql.SQL("INSERT INTO teldata (ts, prio, ec, roi, tracking, guiding, slewing, guiderMoving, az, zd, pa, domeAz, domeStat) \
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
                logger.info(insert_query)
                print(item)
                print('Added to teldata')

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
                print('Added to telvane')
    
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
                print('Added to telenv')

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
                print('Added to telsee')

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
                print('Added to telcat')

            elif 'observer' in item_json:
                    item = json.loads(item_json)  # Parse the JSON string into a dictionary
                    insert_query = sql.SQL("INSERT INTO observer (ts, prio, ec, email, obsName, observing) VALUES ({}, {}, {}, {}, {}, {}) ON CONFLICT (ts) DO NOTHING;")
                    query_args = (
                        sql.Literal(item['ts']),
                        sql.Literal(item['prio']),
                        sql.Literal(item['ec']),
                        sql.Literal(item['email']),
                        sql.Literal(item['obsName']),
                        sql.Literal(item['observing']),
                   )
                    # Only execute the insert if the necessary fields are present
                    if item['email'] is not None or item['obsName'] is not None: # or item['observing']:
                        cur.execute(insert_query.format(*query_args))
                    else:
                        continue
                    print('Added to observer')
            conn.commit()
    except Exception as e:
        print(e)
        logger.exception(e)
        print(insert_query)
        conn.rollback()

    finally:
        
        cur.close()
        conn.close()
        logger.info("Data added to database")
        print("Data added to database\nComplete")

def truncate_single_table(table):
    '''Truncate a single table from database'''
    try:
        with psycopg2.connect(**db_params) as conn:
            with conn.cursor() as cur:
                cur.execute(f"TRUNCATE TABLE {table};")
                conn.commit()
    except Exception as e:
        print(e)
        conn.rollback()
    finally:
        cur.close()
        conn.close()

def truncate_all_tables():
    '''TRUNCATE quickly removes all rows from a set of tables. It has the same effect as an unqualified DELETE on each table, but since it does not actually scan the tables it is faster. This is most useful on large tables.'''

    try:
        with psycopg2.connect(**db_params) as conn: #** unpacks the dictionary
            with conn.cursor() as cur:
                cur.execute(f"""
                            DO $$
                            DECLARE
                                table_name text;
                            BEGIN
                                FOREACH table_name IN ARRAY %s
                                LOOP
                                    IF EXISTS (
                                        SELECT 1
                                        FROM PG_TABLES
                                        WHERE schemaname = 'public' AND tablename = table_name 
                                    )
                                    THEN
                                        EXECUTE 'TRUNCATE TABLE ' ||  quote_ident(table_name); 
                                    END IF;
                                END LOOP;
                            END$$;
                            """, (tables,))
                                        #quote_ident() adds double quotes to table name if it contains special characters
                conn.commit()
    except Exception as e:
        print(e)
        conn.rollback()

    finally:
        print("Tables truncated, maybe but most likey not")
        #cur.close() unneeded cuz the with open, duhhhh
        #conn.close()

def create_all_tables():
    '''create and build all tables if they don't already exist. Tables that were truncated still exist; running this function will not recreate them.'''
    try:
        with psycopg2.connect(**db_params) as conn:
            with conn.cursor() as cur:
                imported_tables = tf.build_all()
                for ind, table in enumerate(imported_tables):
                    cur.execute(table)
                    print(f'{tables[ind]} created')
                conn.commit()
    except Exception as e:
        print(e)
        logger.exception(e)
        conn.rollback()
        print("Rollback, Tables not created")

def drop_n_build_all():
    '''drop all tables and rebuild them'''
    delete_all_tables()
    #truncate_all_tables()
    create_all_tables() #tables arn't deleted but truncated
    add_file_to_db()
    logger.info("Tables dropped and rebuilt")

def mainn():
    logger.info("Program started")
    print("Welcome to the MAGAO-X  database")
    print("Please select an option:")

    run = True
    while run:
        try:
            print("1. Add file to database")
            print("2. Fetch data from database")
            print("3. Custom type query")
            print("4. Drop and ReBuild entire Database")
            print("5. Truncate single table")
            print("6. Exit")
            option = int(input("Enter option number: "))
            if option == 2:
                limit = input("Enter limit; leave blank if None: ")
                where = input("Enter where clause; leave blank if None: ")
                data = fetch_data('*', 'telsee', limit= limit, where= where)
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
                        logger.info("User exited program")
                        print('Goodbye Human')
                        run = False
            elif option == 3:
                query = input("Please enter custom query: ")
                custom_query(query)
                logger.info("Custom query executed")

            elif option == 4:
                drop_n_build_all()
                logger.info("Database dropped and rebuilt")

            elif option == 5:
                try:
                    trun_table = (f'Enter table name you wish to truncate: ')
                    truncate_single_table(trun_table)
                except Exception as e:
                    logger.exception(e)
                    print(e)
                    print("Table not found")
                    continue    

            elif option == 6:
                print("Goodbye")
                exit()
                break
            else:
                print("Invalid input, select from numbers given above")

        except ValueError as val_err:
            print(val_err)
            print("Invalid Input, Please enter a number from the options above")

if __name__ == "__main__":
#######################################
    mainn()
    #truncate_all_tables()
    #add_file_to_db()
    #delete_all_tables()
    #drop_n_build_all()

