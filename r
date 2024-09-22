import sqlite3
import pandas as pd

conn=sqlite3.connect('database.db')
cursor=conn.cursor()

def csv_sql(filePath, table_name):
    df=pd.read_csv(filePath)#читаем чз пандас cvs указываем путь к файлу
    df.to_sql(table_name, con=conn, if_exists='replace') #узываем соединенеи и что делать если такая таблица существует

def sql_csv(table_name, filePath):
    df=pd.read_sql_query(con=conn, sql=f'SELECT * FROM {table_name}')
    df.to_csv(filePath)
        
def init():
    cursor.execute("""CREATE TABLE if not exists hist_auto(
                    id integer primary key,
                    model varchar(128),
                    transmission varchar(128),
                    body_type varchar(128),
                    drive_type varchar(128),
                    color varchar(128),
                    production_year integer,
                    auto_key integer,
                    engine_capacity numper(2, 1),
                    horsepower integer,
                    engine_type varchar(128),
                    price integer,
                    milage integer,
                    start_dttm datetime default current_timestamp,
                    end_dttm datetime default (datetime('2999-12-31 23:59:59'))) 



""")
    cursor.execute("""
        CREATE VIEW if not exists v_auto as
        select
            id,
            model,
            transmission,
            body_type,
            drive_type,
            color,
            production_year,
            auto_key,
            engine_capacity,
            horsepower,
            engine_type,
            price,
            milage
        from hist_auto
        where current_timestamp  between start_dttm and end_dttm;
""")
    
def newRows(): #выводит таблицу с (актулаьными)записями, которые есть в tmp_auto но нет в hist_auto
    cursor.execute('''CREATE TABLE tmp_new_rows as
                    SELECT * 
                    FROM tmp_auto
                    WHERE auto_key is not( select auto_key from hist_auto)
''')
    
def deletedRows(): ##выводит таблицу с (удаленными)записями, которые есть в hist_auto но нет в tmp_auto
    cursor.execute('''CREATE TABLE tmp_dlt_rows 
                    SELECT *
                    FROM hist_auto
                    WHERE auto_key is not ( select auto_key from tmp_auto)
                    ''')

def changeRows():
    cursor.execute('''CREATE TABLE tmp_change_rows as
                    SELECT *
                    FROM tmp_auto
                    



''')




def showTable(table_name):
    print ('_'*10 + table_name+'_'*10)
    cursor.execute(f"SELECT * FROM {table_name}")#чтобы вставить в запрос переменную нужно добавть f
    for row in cursor.fetchall():
        print(row)
    print('\n'*2)
#csv_sql("C:/Users/cutac/курсы/lesson_6/data.csv", 'tmp_auto')
#showTable('tmp_auto')
#sql_csv('tmp_auto', "C:/Users/cutac/курсы/lesson_6/lol.csv")
