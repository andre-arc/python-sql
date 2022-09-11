import sqlalchemy as db
import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session
from os import walk, path

engine = db.create_engine('mysql+pymysql://root:@localhost:3306/esurat')
session = Session(engine, future=True)

def updateEmail(id, email):
    try:
        query = f'update users set email="{email}" where username="{id}"'
        result = session.execute(text(query))
        print(f"berhasil update user {id} dengan email '{email}'\n")
    except Exception as e:
        session.rollback()
        print(f"{e}")


f = []
data_path = 'data'
for (dirpath, dirnames, filenames) in walk(data_path):
    for filepath in filenames:
        filename, file_extension = path.splitext(filepath)
        if file_extension == '.csv':
            f.append(filepath)
    break

    
total_run_query = 0
for filepath in f:
    print(f"import data menggunakan file: 'data/{str(filepath)}'\n")
    print("==========================================================\n")

    df = pd.read_csv(f'data/{str(filepath)}',sep=';')
    df.columns = range(df.columns.size)
    
    if df[0].dtypes == 'object':
        df[0] = df[0].str.replace(' ', '')

    i=0
    # print(df)
    for index, data in df.iterrows():
        # print(data[3])
        updateEmail(data[0], data[3])
        total_run_query += 1
        
    session.commit()
print(f"TOTAL RUN SQL QUERY: {total_run_query} \n")
print("==========================================================\n")
    