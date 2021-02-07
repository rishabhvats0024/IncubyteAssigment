import pyodbc
import pandas as pd
from sqlalchemy import create_engine

try:
  #Source database connection
    con = pyodbc.connect("Driver={ODBC Driver 13 for SQL Server};Server=tcp:asaworkspacesynpse01.sql.azuresynapse.net,1433;Database=SQLPool01;Uid=asa.sql.admin;Pwd=Wind0wsazure@;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;")    #"asaworkspacesynpse01.sql.azuresynapse.net", "root", "Wind0wsazure@", "SQLPool01"
    cr = con.cursor()
    SQL_Query = pd.read_sql_query('''select Customer_Name, Customer ID, Customer Open Date, Last Consulted Date, Country, State, Post Code, Date of Birth, Active Customer  from Existing_table_name''', con)
  #Data Manipulation starts
    df = pd.DataFrame(SQL_Query, columns=['Customer_Name', 'Customer ID', 'Customer Open Date', 'Last Consulted Date', 'Country, State', 'Date of Birth', 'Active Customer'])
    grouped=df.groupby('Country')
    for name,group in grouped:
        engine= create_engine('sqlserver://asaworkspacesynpse01.sql.azuresynapse.net:1433;database=SQLPool01;user=asa.sql.admin@asaworkspacesynpse01;password=Wind0wsazure@;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.sql.azuresynapse.net;loginTimeout=30;',echo=False)
        group.to_sql('NewTable_',con=engine, if_exists ='append')
        print("New Table "+name+" created")

    print("All tables are categorized and created")
except Exception as ex:
    print(ex)
finally:
    con.close()
