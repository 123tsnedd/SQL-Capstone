import psycopg2
import json
import table_file as tf

# Connect to the PostgreSQL database

db_params = {
        "host": "localhost",
        "dbname": "data",
        "user": "postgres",
        "password": "AstroLab", #this will probably change to be required as an input for security
        "port": "5432",
    }

tables = ['telpos', 'telsee', 'telvane', 'telenv', 'telcat', 'teldata', 'observer']

def insert_specific_data(table, params):
    """insert data into specific table. """
    return f""" INSERT INTO {table} {params}"""

def query_data(table):
    """selects data from selected table"""
    return f""" SELECT * FROM {table}"""

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

def open_file(file):
    data = []
    with open(file, "r") as file:
        for line in file:
            line_data = json.loads(line)
            data.append(line_data)
    return data

def extract_msg(files):
    import json
    #initialize empty list to store processed msg
    extracted_msg = []

    #will be a steam of data from dump file
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
    connection = psycopg2.connect(**db_params)
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
        columns = get_columns(table)
        cursor.close()
        connection.close()
        return columns, result

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
            
            elif 'observer' in item_json:
                item = json.loads(item_json) #dict_keys(['ts', 'prio', 'ec', 'email', 'obsName', 'observing'])
                insert_query = sql.SQL("INSERT INTO observer (ts, prio, ec, email, obsName, observing)\ VALUES({}, {}, {}, {}, {}, {}) ON CONFLICT (ts) DO NOTHING;").format(
                    sql.Literal(item['ts']),
                    sql.Literal(item['prio']),
                    sql.Literal(item['ec']),
                    sql.Literal(item['email']),
                    sql.Literal(item['obsName']),
                    sql.Literal(item['observing']),
                )
                if item['email'] == None and item['obsName'] == None:
                    if item['observing'] == True:
                        item['email'] = 'None'
                        item['obsName'] = 'None'
                        cur.execute(insert_query)
                    else:
                        pass
                else:
                    cur.execute(insert_query)
            #conn.commit()
    except Exception as e:
        print(e)
        print(insert_query)
        conn.rollback()
    else:
        conn.commit()
    finally:
        cur.close()
        conn.close()
        print("Data added to database\nCompleted")

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
                            BEGIN
                                IF EXISTS (
                                    SELECT 1
                                    FROM PG_TABLES
                                    WHERE schemaname = 'public' AND tablename = %s 
                                )
                                THEN
                                    EXECUTE 'TRUNCATE TABLE' ||  %S;
                                END IF;
                            END$$;
                            """, ('telpos', 'telsee', 'telvane', 'telenv', 'telcat', 'teldata', 'observer'))
                conn.commit()
    except Exception as e:
        print(e)
        conn.rollback()

    finally:
        cur.close()
        conn.close()

def create_all_tables():
    '''create and build all tables if they don't already exist. Tables that were truncated still exist; running this function will not recreate them.'''
    try:
        with psycopg2.connect(**db_params) as conn:
            with conn.cursor() as cur:
                for table in tables:
                    cur.execute(tf.build_all())
                conn.commit()
    except Exception as e:
        print(e)
        conn.rollback()
        print("Tables not created")
    finally:
        
        cur.close()
        conn.close()

def drop_n_build_all():
    '''drop all tables and rebuild them'''
    truncate_all_tables()
    create_all_tables()
    add_file_to_db()

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
            print("4. Drop and ReBuild entire Database")
            print("5. Truncate single table")
            print("6. Exit")
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
                drop_n_build()

            elif option == 5:
                try:
                    trun_table = (f'Enter table name you wish to truncate: ')
                    truncate_single_table(trun_table)
                except Exception as e:
                    print(e)
                    print("Table not truncated")
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

