import pyodbc
import pandas as pd
from datetime import datetime,timedelta
import os
import fnmatch
from sqlalchemy import create_engine,text
from sqlalchemy.orm import sessionmaker

def sqlalchemy_connection(srv_name,db_name):
     connection_string = f"mssql+pyodbc://{srv_name}/{db_name}?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
     engine=create_engine(connection_string)
     print(f"Connected to the {srv_name}")
     return engine

def close_connection(conn):
    conn.dispose()

def execute_query(conn,query):
    df=pd.read_sql_query(query,conn)
    return df


def process_file_logs():
    conn_centralized=sqlalchemy_connection("GAD1PRCALOG001","RetailDPGroupReporting")
    fetch_data_server_query=f"SELECT [AuditName],[SourceServerName],[SourceDBName],[SourceTableName],[Field_LoadEndDate] "\
        f"FROM [RetailDPGroupReporting].[dbo].[SSIS_ConfigurationInfo] "\
        f"WHERE ActiveFlag=1 and AuditName in ('PetSmart','RiteAid','Wakefern')"\
        f" Union "\
        f" Select 'Kroger-ETL','Kroger.etl.sql.ccaintranet.com','KrogerDataTracker','TrackerLoadLog','LoadEndDate'"
    df_file_log_server=execute_query(conn_centralized,fetch_data_server_query)
    df_file_logs=pd.DataFrame()
    for index,row in df_file_log_server.iterrows():
        auditconn=sqlalchemy_connection(row.SourceServerName,row.SourceDBName)
        if row.AuditName == 'RiteAid':
            query_file_log=f"SELECT '{row.AuditName}' AuditName,"\
                            f"Year({row.Field_LoadEndDate}),Isnull(Round(Cast(sum(filesize) as float)/1024/1024/1024,4),0) as FileSizeProcessedinGB,"\
                            f"count(*) as FileProcessed,"\
                            f"Cast(GetDate() as date) as ReportDate "\
                            f"from dbo.{row.SourceTableName} "\
                            f"where  Year({row.Field_LoadEndDate}) in (2022,2023,2024) "\
                            f"group by Year({row.Field_LoadEndDate})"
        else:
                        query_file_log=f"SELECT '{row.AuditName}' AuditName,"\
                            f"Year({row.Field_LoadEndDate}),Isnull(Round(Cast(sum(filesize) as float)/1024/1024/1024,4),0) as FileSizeProcessedinGB,"\
                            f"count(*) as FileProcessed,"\
                            f"Cast(GetDate() as date) as ReportDate "\
                            f"from dbo.{row.SourceTableName} "\
                            f"where  Year({row.Field_LoadEndDate}) in (2022,2023,2024) "\
                            f"group by Year({row.Field_LoadEndDate})"
        df_file_log=execute_query(auditconn,query_file_log)
        df_file_logs=pd.concat([df_file_logs,df_file_log])
        close_connection(auditconn)
    return df_file_logs



df_data_logs=process_file_logs()
df_data_logs.to_csv('FileProcessedByYear.csv',index='False')