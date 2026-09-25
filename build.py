import functions as fnc
import toml
import logging
import sys
import requests
import re
from pathlib import Path
from bs4 import BeautifulSoup
from io import BytesIO


connstr=fnc.getconfig("connstr")
catalog_url = fnc.getconfig("catalogurl")
catalog_dic = fnc.getconfig("catalogdict")
output_raw =fnc.getconfig("raw_data")
logger = logging.getLogger(__name__)

def run():
    if check_db_configuration()==True:        
        logger.debug("Database connectivity is succesful")
        if fnc.getconfig("build")=="new":
            logger.info("Build is set to New")
            db_cleanup_and_create()
        
        load_table_list()
        create_data_dictionary()
        download_csv_sample()

def download_csv_sample():
    logger.info(f"Download")
    df_tbl_list=fnc.GetDF(connstr,"SELECT TableName,recordid FROM [dbo].[NCCS_Tables]  where include=1 order by recordid asc")    
    for index, row in df_tbl_list.iterrows():
        tblname=row['TableName']
        tblid=row['recordid']
        download_path=Path(output_raw+"/")
        download_path.mkdir(parents=True,exist_ok=True)
        sql =f"SELECT url FROM dbo.NCCS_Tables_FileRecord where tableid={tblid} order by datayear asc"        
        df_files=fnc.GetDF(connstr,sql)
        for index, frow in df_files.iterrows():
            fileurl = frow['url']
            filename=fileurl.split("/")[-1]   
            download_path = Path(output_raw+"/"+tblname+"/csv")
            download_path.mkdir(parents=True,exist_ok=True)
            file_path = output_raw+"/"+tblname+"/csv/"+filename
            if Path.exists(file_path)==False:
                response = requests.get(fileurl)
                if response.status_code == 200:
                    with open(file_path, 'wb') as file:
                        file.write(response.content)
                        logger.info(f"{file_path} downloaded")
                else:
                    logger.warning(f"{file_path} downloaded failed")    

def create_data_dictionary():
    df_tbl_list=fnc.GetDF(connstr,"SELECT TableName,recordid FROM [dbo].[NCCS_Tables]  where include=1 order by recordid asc")
    for index, row in df_tbl_list.iterrows():
            tblname=row['TableName']
            tblid=row['recordid']
            res = requests.get(catalog_dic,timeout=160)  
            soup = BeautifulSoup(res.text,"html.parser")  
            tablediv = soup.find("div",id="table-name-"+tblname.lower())
            table = tablediv.find_next("table")  
            logger.info(f"{tblname} building dictionary")              
            for row in table.find_all("tr"):    
                cells = row.find_all(["td", "th"])    
                if not cells:continue        
                prefix = cells[0].get_text(strip=True)
                varname = cells[1].get_text(strip=True)
                desc = cells[2].get_text(strip=True)
                loccode = cells[3].get_text(strip=True)
                scope = cells[4].get_text(strip=True)
                if (desc.lower()!="description"):
                    sql = f"select recordid from [dbo].[NCCS_Tables] where tablename='{tblname}'";
                    dffileid=fnc.GetDF(connstr,sql)
                    fileid= dffileid['recordid'].iloc[0]
                    sql=f"""
                        update [dbo].[NCCS_Tables_Columns] set newcolname = '{varname}',
                            [description]='{desc}',locationcode='{loccode}',
                            [scope]='{scope}'
                        where orgcolname = '{prefix+varname}' and tableid={fileid}
                    """        
                    fnc.ExecQ(connstr,sql)
                    sql = f"""if (select count(1) cnt from [dbo].[NCCS_Tables_Columns] where newcolname = '{varname}' and tableid = {fileid})=0
                    begin
                        insert into [dbo].[NCCS_Tables_Columns] (tableid,orgcolname,newcolname,description,locationcode,scope,orderid)
                        select {fileid},'{prefix+varname}','{varname}','{desc}','{loccode}','{scope}',(select count(1)+1 from [dbo].[NCCS_Tables_Columns]  where tableid={fileid})
                    end
                    """
                    fnc.ExecQ(connstr,sql)
            sql=f"""
                update [dbo].[NCCS_Tables_Columns] set newcolname = orgcolname
                where orgcolname in (SELECT orgcolname FROM [dbo].[NCCS_Tables_Columns]
                where isnull(newcolname,'') = '' and tableid = {fileid}
                and orgcolname  not in (select newcolname from [dbo].[NCCS_Tables_Columns] where newcolname is not NULL and tableid = {fileid})
                ) and tableid = {fileid};
                update  [dbo].[NCCS_Tables_Columns] set Description = 'Duplicate column', NewColname = 'skip' where orgcolname in
                (select newcolname from [dbo].[NCCS_Tables_Columns] where newcolname is not NULL and tableid = {fileid}) and newcolname is null
                """
            fnc.ExecQ(connstr,sql)



                 

def load_table_list():  
    res = requests.get(catalog_url,timeout=160)  
    soup = BeautifulSoup(res.text,"html.parser")     
    for heading in soup.find_all("h3"):
        table_name = heading.get_text(strip=True)                            
        if not re.match(r"^(F9|SA|SB|SC|SD|SE|SF|SG|SH|SI|SJ|SK|SL|SM|SN|SO|SR)-",table_name):
            continue  
        logger.info(f"{table_name}" )          
        table = heading.find_next("table")
        if table is None:continue # break and continue to the next heading if no table is found            
        for row in table.find_all("tr"):    
            cells = row.find_all(["td", "th"])    
            if not cells:continue
            row_year = cells[0].get_text(strip=True)                                    
            if row_year.isdigit():                
                row_count = float(cells[4].get_text(strip=True).replace(",",""))
                download_link = row.find("a", href=True)
                if download_link:                    
                    url = download_link["href"]
                    filename=url.split("/")[-1]  
                    sql = f"""
                        declare @fileid as int
                        if (select count(1) from dbo.NCCS_Tables where tablename = '{table_name}')=0 begin
                            insert into dbo.NCCS_Tables(tablename,include) select '{table_name}',1
                        end;
                        select @fileid = recordid from dbo.NCCS_Tables where tablename = '{table_name}'
                        if (select count(1) from dbo.NCCS_Tables_FileRecord where tableid = @fileid and datayear = {row_year})=0 begin
                            insert into NCCS_Tables_FileRecord([TableID], [DataYear], [RowCount], [LastUpdate],url,filename)
                            select @fileid,{row_year},{row_count},null, '{url}','{filename}'
                        end
                    """                 
                    if not fnc.ExecQ(connstr,sql):
                        raise RuntimeError(f"Failed to save catalog record for {table_name}")                                                                

def db_cleanup_and_create():    
    logger.info("Drop tables use for catalog and data dictionary")
    sql = "select 'drop table ['+TABLE_SCHEMA+'].['+Table_Name+']' as cmd from INFORMATION_SCHEMA.TABLES"
    dfslist = fnc.GetDF(connstr,sql)
    for index, row in dfslist.iterrows():
        sql=str(row.cmd)
        fnc.ExecQ(connstr,sql)
    logger.info("Recreate tables")
    sql="""
    CREATE TABLE [dbo].[NCCS_Tables](	
        [RecordID] [int] IDENTITY(1,1) NOT NULL,	
        [TableName] [nvarchar](150) NULL,	
        [Include] [bit] NULL)

    CREATE TABLE [dbo].[NCCS_Tables_FileRecord](	
        [TableID] [int] NULL,	
        [FileName] [nvarchar](300) NULL,	
        [url] [nvarchar](4000) NULL,	
        [DataYear] [int] NULL,	
        [RowsLoaded] [float] NULL,	
        [RowCount] [float] NULL,	
        [LastUpdate] [date] NULL)

    CREATE TABLE [dbo].[NCCS_Tables_Columns](	
        [TableID] [int] NULL,	[OrgColName] [nvarchar](150) NULL,	
        [NewColName] [nvarchar](150) NULL,	[ColType] [nvarchar](150) NULL,	
        [Description] [nvarchar](4000) NULL,	
        [LocationCode] [nvarchar](1000) NULL,	[Scope] [nvarchar](1000) NULL,	
        [OrderID] [int] NULL)
    """
    fnc.ExecQ(connstr,sql)
def check_db_configuration(): # check database if contains build tables
    sql = "select 1"
    retval = fnc.ExecQ(connstr,sql) 
    if retval == False:logger.error("Cannot connect to database, check connection string in config file")
    return retval
    
    
    #df=fnc.GetDF(fnc.getconfig("connstr"), "select Table_Name from INFORMATION_SCHEMA.TABLES where TABLE_SCHEMA='dbo' and table_name like '%NCCS_Tables%'")


