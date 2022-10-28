import psycopg2 
import cx_Oracle
import sys
cx_Oracle.init_oracle_client(lib_dir=r"C:\instantclient_21_6")
from configparser import ConfigParser

#read the database.ini file  
def config (filename, section):
    try:
        
        parser = ConfigParser()    
        parser.read(filename)
        db = {}
        
        if parser.has_section(section):
            params = parser.items(section)
            for param in params:
                db[param[0]] = param[1]
        else:
            raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    except (Exception, psycopg2.DatabaseError) as error:
        print("Error de conexión a BD", error)        
        input('Presione una tecla para salir ')
        sys.exit()
        
    return db

#connecting to Postgresql
def connect_postgres():    
    
    try: 
        
        params = config('database.ini', 'postgresql') 
        conn = psycopg2.connect(**params)        
        cur = conn.cursor()
   
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error de conexión a BD", error)        
        input('Presione una tecla para salir ')
        sys.exit()
    return conn, cur
    
#connecting to Oracle
def connect_oracle():
    
    try:
        
        conn_info = config('database.ini', 'oracle')  
        conn_str = '{user}/{password}@{host}:{port}/{service}'.format(**conn_info)

        con = cx_Oracle.connect(conn_str)
        cursor = con.cursor()              
    
    except cx_Oracle.DatabaseError as e:
        print("Error de conexión a BD", e)        
        input('Presione una tecla para salir ')
        sys.exit()
    return con, cursor
