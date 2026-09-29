import functions as fnc
import build
import toml
import logging
import sys
import requests
import re
import pandas as pd
from pathlib import Path
from bs4 import BeautifulSoup
from io import BytesIO
import pyarrow.parquet as pq
import sqlalchemy as sa
connstr=fnc.getconfig("connstr")
catalog_url = fnc.getconfig("catalogurl")
catalog_dic = fnc.getconfig("catalogdict")
output_par =fnc.getconfig("raw_parq")
logger = logging.getLogger(__name__)

def run(targettable=None,targetfile=None,dictionary=None,buildoption=None):    
    if build.check_db_configuration()==False:   
        logger.debug("Database connectivity is not succesful")
        return False
    if (dictionary=="yes"):
        update_dictionary(targettable,targetfile)
        return

    if check_dictionary(targettable,targetfile) == False:
        logger.warning("Loading of CSV files cannot continue, define column type first")
        return

    if (buildoption!="noaction"):
        build_options(buildoption,targettable,targetfile)
    
    load_data(targettable,targetfile)

def build_options(buildoption,targettable,targetfile):
    sql="SELECT TableName,recordid FROM [dbo].[NCCS_Tables] where include=1 "
    if targettable!="*": sql=sql+f" and tablename='{targettable}' order by recordid"
    df_tbl_list = fnc.GetDF(sql)
    for _, row in df_tbl_list.iterrows():
        tblname = row["TableName"]
        tblid = row["recordid"]        
        if (buildoption=="droptable"):
            sql = f"""IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[raw].[{tblname}]') AND type in (N'U')) begin drop table [raw].[{tblname}] end """
            fnc.ExecQ(sql)
            sql = f"update [dbo].[NCCS_Tables_FileRecord] set rowsloaded = null where tableid=(SELECT recordid FROM [dbo].[NCCS_Tables] where tablename='{tblname}')"
            fnc.ExecQ(sql)
        if (buildoption=="overwrite"):
            sql = f"""
                delete from raw.[{targettable}] where sourcefile in
                (select replace(lower(filename),'.csv','.parquet') from [dbo].NCCS_Tables_FileRecord 
                where tableid = (select recordid from dbo.NCCS_Tables where tablename = '{targettable}') and datayear={targetfile})
            """
            #fnc.ExecQ(sql)
            #sql = f"update [dbo].[NCCS_Tables_FileRecord] set rowsloaded = null where tableid=(SELECT recordid FROM [dbo].[NCCS_Tables] where tablename='{tblname}')"
            #fnc.ExecQ(sql)

def load_data(targettable=None,targetfile=None):            
    sql="SELECT TableName,recordid FROM [dbo].[NCCS_Tables] where include=1 "
    if targettable!="*": sql=sql+f" and tablename='{targettable}' order by recordid"    
    df_tbl_list = fnc.GetDF(sql)
    for _, row in df_tbl_list.iterrows():
        tblname = row["TableName"]
        tblid = row["recordid"]   
        create_sqltable(tblid,tblname)             
        parquet_path = Path(output_par) / tblname     
        filterfile = "*.parquet"        
        if targetfile != "*"  : filterfile = "*" + targetfile+".parquet"
        for file_path in  parquet_path.glob(filterfile):    
            source=file_path.name.replace(".parquet",'.csv')
            sql = f"select [RowCount] - isnull([RowsLoaded],0) cnt from  [dbo].[NCCS_Tables_FileRecord] where filename = '{source}'"
            cnt = int(fnc.GetDFCol(sql,'cnt'))            
            if cnt!=0:                
                source=file_path.name            
                load_parquet(tblid,file_path,tblname,source)
            else:
                logger.info(f"Skipped {tblid} | {source.replace(".csv",'.parquet')}")

def create_sqltable(tblid,tblname):
    sql = f"select orgcolname,newcolname, coltype from dbo.nccs_tables_columns where tableid = {tblid} order by orderid"
    df_columns = fnc.GetDF(sql)
    structure=""    
    structure=structure + "[sourcefile] [nvarchar](500) null " 
    structure=structure + ",[url] [nvarchar](max) null " 

    for _, row in df_columns.iterrows():
        newcol = row["newcolname"]
        orgcol = row["orgcolname"] 
        coltype = row["coltype"]         
        match coltype.lower():
            case "string":                
                structure=structure + ',['+ newcol + "] [nvarchar](max) null " 
            case "int64":                
                structure=structure + ',['+ newcol + "] [int] null "                 
            case "float":
                structure=structure + ',['+ newcol + "] [float] null "                                 
            case "datetime64":
                structure=structure + ',['+ newcol + "] [datetime] null "                                                 
            case "boolean":
                structure=structure + ',['+ newcol + "] [bit] null "                    
                

    sql = f"""IF OBJECT_ID('raw.[{tblname}]', 'U') IS NULL begin CREATE TABLE [raw].[{tblname}]({structure}) end"""    
    fnc.ExecQ(sql)
    sql = f"""IF NOT EXISTS(SELECT * FROM sys.indexes WHERE name = 'NCI-SourceFile' AND object_id = OBJECT_ID('raw.{tblname}'))
    CREATE NONCLUSTERED INDEX [NCI-SourceFile] ON [raw].[{tblname}]([sourcefile] )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
    """    
    fnc.ExecQ(sql)
    #sql = f"delete from [raw].[{tblname}] where sourcefile='{source}'"
    #fnc.ExecQ(sql)

def load_parquet(tableid,file,tblname,source):
    logger.info(f"Loading {tableid} | {source}")

    tblschema=get_table_schema(tableid)
    df_parquet = pd.read_parquet(file,schema=tblschema)

    column_map={}    
    url=False    

    sql = f"select orgcolname,newcolname, coltype from dbo.nccs_tables_columns where tableid = {tableid} order by orderid"
    df_columns = fnc.GetDF(sql)
    for _, row in df_columns.iterrows():
        newcol = row["newcolname"]
        orgcol = row["orgcolname"]             
        coltype =row["coltype"]             
        for pcolumn in df_parquet.columns: # create column map
            if pcolumn.lower()==orgcol.lower():
                column_map[pcolumn]=newcol                
            if pcolumn.lower()=="url":url=True    
    
    for pcolumn in df_parquet.columns: 
        found=False        
        for _, row in df_columns.iterrows():            
            if row["orgcolname"].lower()==pcolumn.lower():
                found=True        
        if found==False:
            if pcolumn.lower()!="url":
                df_parquet = df_parquet.drop(columns=[pcolumn], errors="ignore")

    
    if (url==True):column_map["URL"]="URL"    
    
    for column in get_table_column_dates(tableid):                
        if column not in df_parquet.columns:continue
        df_parquet[column] = pd.to_datetime(df_parquet[column],errors="coerce")

    for column in get_table_column_string(tableid):                
        if column not in df_parquet.columns:continue        
        df_parquet[column] = df_parquet[column].astype("string")        
        df_parquet[column] = df_parquet[column].str.replace(r"^([0-9]+)\.0+$", r"\1", regex=True)
    
    df_parquet = df_parquet.rename(columns=column_map)

    df_parquet.insert(0, "SourceFile", source)
    engine = sa.create_engine(f"mssql+pyodbc:///?odbc_connect={connstr}")            
    df_parquet.to_sql(
        name=tblname,         # Name of the SQL table
        schema="raw",
        con=engine,               # SQLAlchemy engine connection
        if_exists="append",       # What to do if table exists: 'fail', 'replace', or 'append'
        index=False,               # Set to True if you want to keep the DataFrame index
        chunksize=1000,
    )

    sql = f"""
            UPDATE t SET rowsloaded = a.cnt FROM dbo.NCCS_Tables_FileRecord AS t INNER JOIN
            (SELECT COUNT(*) AS cnt,replace(sourcefile,'.parquet','.csv') sourcefile FROM raw.[{tblname}] group by sourcefile
            ) AS a ON a.sourcefile = t.filename WHERE t.tableid = {tableid};"""
    fnc.ExecQ(sql)

    sql = f"update dbo.NCCS_Tables_FileRecord set lastupdate = getdate() where replace(filename,'.csv','.parquet') = '{source}'"
    fnc.ExecQ(sql)




def get_table_schema(fileid):
    sql = f"select orgcolname,coltype from NCCS_Tables_Columns where tableid = {fileid} and coltype != 'datetime64' order by orderid asc"
    dfcol = fnc.GetDF(sql)
    schema={}
    for index, row in dfcol.iterrows():        
        schema[row['orgcolname']] = row['coltype']

def check_dictionary(targettable="",targetfile=""):
    retval=True
    ## identified columns -- for review incase data is incorrect format
    sql = """
    update [dbo].[NCCS_Tables_Columns] set coltype = 'float'  where orgcolname like '%_TOT_AMT_%' and coltype is null;
    update [dbo].[NCCS_Tables_Columns] set coltype = 'float'  where orgcolname = 'F9_07_COMP_DTK_COMP_ORG_SUBTOT' and coltype is null;
    update [dbo].[NCCS_Tables_Columns] set coltype = 'float'  where orgcolname = 'F9_07_COMP_DTK_COMP_RLTD_SUBTOT' and coltype is null;
    update [dbo].[NCCS_Tables_Columns] set coltype = 'float'  where orgcolname = 'F9_07_COMP_DTK_COMP_OTH_SUBTOT' and coltype is null;
    update [dbo].[NCCS_Tables_Columns] set coltype = 'string'  where orgcolname = 'SH_05_HOSPITAL_ADDR_CNTR' and coltype is null;
    update [dbo].[NCCS_Tables_Columns] set coltype = 'string'  where orgcolname = 'SH_05_HOSPITAL_SUBORD_NAME_L2' and coltype is null;
    update [dbo].[NCCS_Tables_Columns] set coltype = 'string'  where orgcolname = 'F9_07_COMP_DTK_EXPL_NAME_ORG_L2' and coltype is null;
    update [dbo].[NCCS_Tables_Columns] set coltype = 'string'  where orgcolname = 'F9_07_COMP_DTK_EXPL_NAME_PERS' and coltype is null;
    update [dbo].[NCCS_Tables_Columns] set coltype = 'string'  where orgcolname = 'F9_07_COMP_DTK_EXPL_TXT' and coltype is null;
    update [dbo].[NCCS_Tables_Columns] set coltype = 'string'  where orgcolname = 'F9_00_EXEMPT_STAT_527_X' and coltype is null;
    update [dbo].[NCCS_Tables_Columns] set coltype = 'string'  where orgcolname = 'F9_07_COMP_DTK_EXPL_NAME_ORG_L1' and coltype is null;
    update [dbo].[NCCS_Tables_Columns] set coltype = 'string'  where orgcolname = 'F9_09_EXP_FEE_SVC_FUNDR_PROG' and coltype is null;
    update [dbo].[NCCS_Tables_Columns] set coltype = 'string'  where orgcolname = 'SC_02_LOB_ACT_PAID_STAFF_AMT' and coltype is null;
    update [dbo].[NCCS_Tables_Columns] set coltype = 'string'  where orgcolname = 'F9_09_EXP_FEE_SVC_FUNDR_MGMT' and coltype is null;
    update [dbo].[NCCS_Tables_Columns] set coltype = 'string'  where orgcolname like '%ADDR%';
    update [dbo].[NCCS_Tables_Columns] set coltype = 'string'  where orgcolname like '%phone%';     
    update [dbo].[NCCS_Tables_Columns] set coltype = 'string'  where orgcolname like '%zip%';     
    """       
    fnc.ExecQ(sql)

    sql="SELECT TableName,recordid FROM [dbo].[NCCS_Tables] where include=1 "
    if targettable!="*": sql=sql+f" and tablename='{targettable}' order by recordid"
    df_tbl_list=fnc.GetDF(sql)    
    for index, row in df_tbl_list.iterrows():
        tblname=row['TableName']
        tblid=row['recordid']        
        sql=f"SELECT orgcolname,description FROM [nccs990_test].[dbo].[NCCS_Tables_Columns] where tableid = {tblid} and coltype is null"        
        df_notype=fnc.GetDF(sql)
        for index, trow in df_notype.iterrows():
            retval=False     
            orgcolname= trow['orgcolname']     
            desc= trow['description']     
            logger.info(f"{tblid} | {tblname} | {orgcolname} | {desc}")             
    return retval

def get_table_column_dates(fileid):
    sql = f"select orgcolname from NCCS_Tables_Columns where tableid = {fileid} and coltype like '%date%' order by orderid asc"
    dfcol = fnc.GetDF(sql)
    datelist=[]
    for index, row in dfcol.iterrows():datelist.append( row['orgcolname'])
    return datelist

def get_table_column_string(fileid):
    sql = f"select orgcolname from NCCS_Tables_Columns where tableid = {fileid} and coltype like '%string%' order by orderid asc"
    dfcol = fnc.GetDF(sql)
    stringlist=[]
    for index, row in dfcol.iterrows():stringlist.append( row['orgcolname'])
    return stringlist
        
def update_dictionary(targettable="",targetfile=""):     
    sql="SELECT TableName,recordid FROM [dbo].[NCCS_Tables] where include=1 "
    if targettable!="*": sql=sql+f" and tablename='{targettable}' order by recordid"
    df_tbl_list=fnc.GetDF(sql)    
    for index, row in df_tbl_list.iterrows():
        tblname=row['TableName']
        tblid=row['recordid']        
        sql="SELECT FileName FROM [dbo].[NCCS_Tables_FileRecord]"
        if targetfile!="*": sql=sql+f" where FileName='{targetfile}' order by FileName,tableid" 
        else: sql=sql+" order by tableid"        
        df_file_list=fnc.GetDF(sql)    
        for index, frow in df_file_list.iterrows():
            filename=frow['FileName'].lower().replace(".csv",".parquet")                        
            file_path=Path(output_par+"/"+tblname+"/"+filename)
            if file_path.exists():
                parquet_file = pq.ParquetFile(file_path)                                
                batch = next(parquet_file.iter_batches(batch_size=100), None)
                df_data = (
                    batch.to_pandas()
                    if batch is not None else parquet_file.schema_arrow.empty_table().to_pandas()
                )
                logger.info(f"Dictionary {tblid} | {tblname} | {filename}")                
                df_columns=infer_df(df_data)
                update_column_dictionary(tblid,df_columns,filename)
                                    

def update_column_dictionary(tblid,df,filename):
    for row in df.itertuples(index=False):                
        sql =f"select orgcolname,newcolname,coltype from [dbo].[NCCS_Tables_Columns] where tableid = {tblid} and orgcolname ='{row.colname}'"
        dfcol=fnc.GetDF(sql)        
        for index, rowc in dfcol.iterrows():
            orgcolname=rowc['orgcolname']
            newcolname=rowc['newcolname']
            coltype=rowc['coltype']
            #if coltype==None:
            logger.info(f"{tblid} | {filename} | {newcolname} | {row.dtype}")
            sql = f"update dbo.NCCS_Tables_Columns set coltype = '{row.dtype}' where tableid={tblid}  and orgcolname ='{row.colname}'"
            fnc.ExecQ(sql)
            

def infer_df(df):
    return pd.DataFrame({"colname": df.columns,"dtype": [profile_column(df[col])for col in df.columns]})

def profile_column(column):    
    if column.name.lower().endswith("_x"): # default value base in csv - not boolean - mixed value 0,x
        return "string"
    
    values = column.dropna()
    values = values[values.astype(str).str.strip() != ""]
    if len(values) == 0:
        return "string"
    if pd.api.types.is_datetime64_any_dtype(column.dtype):
        return "datetime64"
    # Convert everything to string for testing
    values = values.astype(str).str.strip()
    # Boolean
    if values.str.lower().isin(
        ["true", "false", "yes", "no", "y", "n", "0", "1"]
    ).all():
        return "boolean"
    # Preserve codes such as ZIP codes and identifiers with leading zeros.
    if values.str.match(r"^[+-]?0\d+$").any():
        return "string"
    # Check integer bounds without converting through floating point.
    if values.str.fullmatch(r"[+-]?[0-9]+").all():
        if all(-(2**63) <= int(value) < 2**63 for value in values):
            return "Int64"
        return "string"
    # Accept integers mixed with decimals, trailing dots, and exponents.
    if values.str.fullmatch(r"[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?").all():
        numeric = pd.to_numeric(values, errors="coerce")
        if numeric.isna().any() or numeric.isin([float("inf"), float("-inf")]).any():
            return "string"
        return "float"
    # Date
    if values.str.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}").all():
        date_values = pd.to_datetime(values, format='%Y-%m-%d', errors="coerce")
        if date_values.notna().all():
            return "datetime64"
    # Default
    return "string"
