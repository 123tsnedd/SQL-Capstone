# Bachelor in Applied Computing Senior Capstone Project
# PostgreSQL Database Interaction Project 

## Overview
This project provides a Python-based interface for seamless interaction with a PostgreSQL database while utilizing programs such as Jupyter Notebooks. It includes functions such as the `custom_query` function to execute SQL queries and handle database operations, 'drop and build' to truncate all tables and rebuild the database, and other helper functions. The Python file table_file.py contains the functions to create tables with their respective parameters, the primary key being the timestamp column.

### Prerequisites
- Python 3.10+
- PostgreSQL
- `psycopg2` imported library
- sqlFile.py
- table_file.py

### Installation
Once PostgreSQL is installed on the supported station, the user can run 'sqlFile.py' *note. Before running the script, the user must update 'db_params' in 'sqlFile.py'* Once started, the user will be prompted with several options; the user will select 'drop_and_build_all() (default #4). The function will create and build all the tables regardless of whether the tables exist already or not. This function will be used for future database rebuilds as well.  
