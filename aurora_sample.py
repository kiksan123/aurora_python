
import os
import MySQLdb



class Aurora():
    def __init__(self):
        db_name = os.environ.get("RDS_DB_NAME"),
        user_name = os.environ.get('RDS_USERNAME'),
        password = os.environ.get('RDS_PASSWORD'),
        host_name = os.environ.get('RDS_HOSTNAME'),
        port = int(os.environ.get('RDS_PORT'))
        cursor = None
        
        print(db_name,type(user_name),password,host_name,port)
        connection = MySQLdb.connect(host=host_name[0],
                                     user=user_name[0],
                                     passwd=password[0],
                                     port=port,
                                     #db=db_name[0],
                                     charset='utf8')
        self.cursor = self.connection.cursor()
        

    
    def execute_sql(self,sql):
        self.cursor.execute(sql)

    def load_sqlfile(self,filename):
        with open(filename,"r") as f:
            sql = f.read()
        return sql
    
    def execute_sqlfile(self,filename):
        sql = self.load_sqlfile(filename)
        print(sql)
        self.execute_sql(sql)
        self.connection.commit()
    
    def bulk_insert(self,sql,parm):
        self.cursor.executemany(sql,parm)
        self.connection.commit()

def demo_df():
    import pandas as pd

    base = 100
    id_L = list(range(base))
    aa = ["aa"] * base
    bb = ["a"] * base

    return id_L,aa,bb




if __name__=="__main__":
    db = Aurora()
    #db.execute_sqlfile("create_table_init.sql")
    id_L,aa,bb = demo_df()

    parm = []
    for a,b,c in zip(id_L,aa,bb):
        parm.append([a,b,c])

    sql = "INSERT INTO employees VALUES (%s,%s,%s)"
    db.bulk_insert(sql,parm)
    #print(df)

