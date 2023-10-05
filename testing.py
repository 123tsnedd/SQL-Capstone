import psycopg2
from psycopg2 import sql
import json
from sql import extract_msg
data = extract_msg()
with open('binaryDataFile.bin', 'wb') as bfile:
    for item in data:
        bfile.write(item.encode('utf-8'))
# PostgreSQL connection parameters
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

# try:
#     for item_json in data:
#         if 'telpos' in item_json:
#             item = json.loads(item_json)  # Parse the JSON string into a dictionary
#             insert_query = sql.SQL("INSERT INTO testtelpos (ts, prio, ec, epoch, ra, dec, el, ha, am, rotoff) \
#                 VALUES (to_timestamp({}, 'YYYY-MM-DDTHH24:MI:SS.US999999999'), {}, {}, {}, {}, {}, {}, {}, {}, {}) ON CONFLICT DO NOTHING;").format(
#                 sql.Literal(item['ts']),
#                 sql.Literal(item['prio']),
#                 sql.Literal(item['ec']),
#                 sql.Literal(item['epoch']),
#                 sql.Literal(item['ra']),
#                 sql.Literal(item['dec']),
#                 sql.Literal(item['el']),
#                 sql.Literal(item['ha']),
#                 sql.Literal(item['am']),
#                 sql.Literal(item['rotoff'])
#             )
#             cur.execute(insert_query)
# except Exception as e:
#     print(e)
#     conn.rollback()
# else:
#     conn.commit()
# finally:
#     cur.close()
#     conn.close()