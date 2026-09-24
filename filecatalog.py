import pandas as pd
from sqlalchemy import column
import buildnccs
from pathlib import Path
import os, requests, gc
import sqlalchemy as sa
import urllib

def update_filecatalogs(connstr,catalog_url):
    maindf=buildnccs.GetDF(connstr,"SELECT TableNames,recordid FROM [dbo].[NCCS_Tables] where include = 1")
    for index, row in maindf.iterrows():
        tblname=row['TableNames']
        fileid=row['recordid']
        download_path=Path(tblname)
        download_path.mkdir(parents=True,exist_ok=True)
        sql =f"SELECT url FROM dbo.NCCS_Tables_FileRecord where fileid={fileid} order by datayear asc"        
        filedf=buildnccs.GetDF(connstr,sql)
        for index, frow in filedf.iterrows():
            fileurl = frow['url']
            filename=fileurl.split("/")[-1]            
            file_path = tblname+"/"+filename
            if Path.exists(file_path)==False:
                response = requests.get(fileurl)
                if response.status_code == 200:
                    with open(file_path, 'wb') as file:
                        file.write(response.content)
                        print(f'{file_path} downloaded successfully')
                else:
                    print(f'Failed to download {file_path} ')
            else:
                print(f'{file_path} already exist')
            check_table_columns(fileid,tblname,file_path,connstr)
            load_to_stage(fileid,file_path,connstr,filename)

def load_to_stage(fileid,file_path,connstr,filename):                               
    connparams = urllib.parse.quote_plus(connstr)
    table_name = get_table_name(fileid,connstr)
    
    if (check_file_data_count(fileid,connstr,filename,table_name) == 0):
        sql=f"""IF (OBJECT_ID(N'raw.[{table_name}]', N'U') IS NOT NULL) begin
        delete from raw.[{table_name}] where FileName = '{filename}' end"""                
        buildnccs.ExecQ(connstr,sql)
    else:
        return
    engine = sa.create_engine(f"mssql+pyodbc:///?odbc_connect={connparams}")
    
    
    csv_schema = get_table_schema(fileid,connstr)    
    date_columns=get_table_column_dates(fileid,connstr)    
    
    df_data = pd.read_csv(file_path,low_memory=False,dtype=csv_schema)
    for column in date_columns:        
        if column not in df_data.columns:continue
        df_data[column] = pd.to_datetime(
            df_data[column],
            errors="coerce"
        )
    df_data['FileName'] = filename
    df_data.to_sql(
        name=table_name,         # Name of the SQL table
        schema="raw",
        con=engine,               # SQLAlchemy engine connection
        if_exists="append",       # What to do if table exists: 'fail', 'replace', or 'append'
        index=False               # Set to True if you want to keep the DataFrame index
    )
    sql = f"""update [dbo].[NCCS_Tables_FileRecord] set 
            rowsloaded = (select count(1) from [raw].[{table_name}] where FileName = '{filename}') 
            ,lastupdate=getdate()
            where fileid={fileid} and filename='{filename}'"""
    buildnccs.ExecQ(connstr,sql)
def check_file_data_count(fileid,connstr,filename,tablename):
    sql = f""" 
    IF (OBJECT_ID(N'raw.[{tablename}]', N'U') IS NOT NULL) begin
        select cnt = case when (SELECT [rowcount] FROM [dbo].[NCCS_Tables_FileRecord] where fileid={fileid} and filename='{filename}') =
        (select count(1) cnt from [raw].[{tablename}] where FileName = '{filename}') then 1 else 0 end
    end
    else begin select 0 as cnt end    
    """        
    dfcheck = buildnccs.GetDF(connstr,sql)      
    cnt= dfcheck['cnt'].iloc[0]
    return int(cnt)
def get_table_name(fileid,connstr):
    sql = f"select TableNames from NCCS_Tables where recordid = {fileid}"
    dfcheck = buildnccs.GetDF(connstr,sql)      
    tblname= dfcheck['TableNames'].iloc[0]
    return tblname
def get_table_schema(fileid,connstr):
    sql = f"select colname,coltype from NCCS_Tables_Columns where fileid = {fileid} and coltype != 'datetime64' order by orderid asc"
    dfcol = buildnccs.GetDF(connstr,sql)
    schema={}
    for index, row in dfcol.iterrows():        
        schema[row['colname']] = row['coltype']
    
    return schema
def get_table_column_dates(fileid,connstr):
    sql = f"select colname from NCCS_Tables_Columns where fileid = {fileid} and coltype = 'datetime64' order by orderid asc"
    dfcol = buildnccs.GetDF(connstr,sql)
    datelist=[]
    for index, row in dfcol.iterrows():datelist.append( row['colname'])
    return datelist
def check_table_columns(fileid,tblname,file_path,connstr):
    if Path.exists(file_path)==False:return         
    df_file  = pd.read_csv(file_path, nrows=100,low_memory=False)
    schema = infer_df(df_file)            
    for  row in schema.itertuples(index=False):                
        check_table_columns_records(fileid,row.colname,row.type,connstr,tblname)
    del df_file
    del schema
    gc.collect()
def check_table_columns_records(fileid,colname,coltype,connstr,tblname):
    sqlcoltype = "[varchar](max)"
    if coltype == "Int64":sqlcoltype = "bigint"
    if coltype == "float":sqlcoltype = "float"
    if coltype == "datetime64":sqlcoltype = "datetime"
    if coltype == "boolean":sqlcoltype = "bit"
    sql = f"""
        if (select count(1) from dbo.NCCS_Tables_Columns where fileid={fileid} and colname = '{colname}') = 0 begin
            insert into dbo.NCCS_Tables_Columns(FileID,ColName,ColType, orderID)
            select {fileid}, '{colname}', '{coltype}', (select count(1)+1 from dbo.NCCS_Tables_Columns where fileid = {fileid} )

            if object_id(N'raw.[{tblname}]',N'U') is not null begin
                if (select count(1) from [INFORMATION_SCHEMA].[COLUMNS] where table_name = '{tblname}' and table_schema = 'raw' and COLUMN_NAME = '{colname}') = 0 begin
                    alter table raw.[{tblname}] add [{colname}] {sqlcoltype}  null 
                end
                
            end
        end
    """
    buildnccs.ExecQ(connstr,sql)
def infer_df(df):
    return pd.DataFrame({
        "colname": df.columns,
        "type": [
            profile_column(df[col])
            for col in df.columns
        ]
    })
def profile_column(column):    
        # Remove null/empty values
    values = column.dropna()
    values = values[values.astype(str).str.strip() != ""]
    if len(values) == 0:
        return "string"
    # Convert everything to string for testing
    values = values.astype(str).str.strip()
    # Boolean
    if values.str.lower().isin(
        ["true", "false", "yes", "no", "y", "n", "0", "1"]
    ).all():
        return "boolean"
    # Integer
    if values.str.match(r"^[+-]?\d+$").all():
        return "Int64"
    # Decimal / Float
    if values.str.match(r"^[+-]?\d*\.\d+$").all():
        return "float"
    # Date
    date_values = pd.to_datetime(values, format='%Y-%m-%d', errors="coerce")
    if date_values.notna().all():
        return "datetime64"
    # Default
    return "string"