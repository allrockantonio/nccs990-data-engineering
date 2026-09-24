import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sqlalchemy import text
import pyodbc
import requests
from bs4 import BeautifulSoup
from io import BytesIO
import re
def build_dimension_tables(connstr):
    # state and county dimension tables
    dfstate = pd.read_csv("state_codename.csv",low_memory=False)    
    for index, row in dfstate.iterrows():
        state_code=row['state_code']
        state_name=row['state_name']
        sql=f"""IF (OBJECT_ID(N'dbo.base_statecodes', N'U') IS NOT NULL) begin
            if (select count(1) from dbo.base_statecodes where statecode = '{state_code}')=0 begin
                insert into dbo.base_statecodes(statecode,statename) select '{state_code}','{state_name}' end 
            end """                
        ExecQ(connstr,sql)
        sql = """
                if object_id(N'dbo.Dim_Organization',N'U') is not null begin drop table dbo.Dim_Organization end;
                if object_id(N'dbo.dim_state',N'U') is not null begin drop table dbo.dim_state end;

                WITH org_ein_CTE AS (
                    SELECT distinct F9_00_ORG_EIN as EIN,EIN2 FROM [raw].[F9-P00-T00-HEADER] 
                ),
                org_info_cte as (
                    select 
                        F9_00_ORG_EIN EIN
                        ,upper(F9_00_ORG_NAME_L1) OrgName
                        ,upper(F9_00_ORG_NAME_L2) OrgName_L2
                        ,upper([F9_00_ORG_ADDR_L1]) Address
                        ,upper([F9_00_ORG_ADDR_CITY]) City 
                        ,upper([F9_00_ORG_ADDR_STATE]) State_Code
                        ,[F9_00_ORG_ADDR_ZIP] ZipCode		
                        ,Row_Number() over (partition by F9_00_ORG_EIN order by F9_00_TAX_YEAR desc) as RowNum
                    from [raw].[F9-P00-T00-HEADER]
                )

                select distinct
                    org_info_cte.EIN,EIN2,
                    OrgName,OrgName_L2
                    [Address],City,State_Code,ZipCode
                    into Dim_Organization
                from org_ein_CTE
                left join org_info_cte on org_ein_CTE.EIN = org_info_cte.EIN and org_info_cte.RowNum = 1

                select * into Dim_State from
                (
                select distinct State_code statecode,State_code statename,State_Information='foreign' from dim_organization where State_code not in (select statecode from base_statecodes)
                union all 
                select statecode, StateName,State_Information='local' from base_statecodes
                ) ds

        """
        ExecQ(connstr,sql)
def get_tables_list(catalog_url,connstr):  
    res = requests.get(catalog_url,timeout=160)  
    soup = BeautifulSoup(res.text,"html.parser")     
    for heading in soup.find_all("h3"):
        table_name = heading.get_text(strip=True)                    
        print(f"Table {table_name} found in catalog")
        if not re.match(r"^(F9|SA|SB|SC|SD|SE|SF|SG|SH|SI|SJ|SK|SL|SM|SN|SO|SR)-",table_name):
            continue            
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
                    _sql = f"""
                        declare @fileid as float
                        if (select count(1) from dbo.NCCS_Tables where tablenames = '{table_name}')=0 begin
                            insert into dbo.NCCS_Tables(tablenames,include) select '{table_name}',0
                        end;
                        select @fileid = recordid from dbo.NCCS_Tables where tablenames = '{table_name}'
                        if (select count(1) from dbo.NCCS_Tables_FileRecord where fileid = @fileid and datayear = {row_year})=0 begin
                            insert into NCCS_Tables_FileRecord([FileID], [DataYear], [RowCount], [LastUpdate],url,filename)
                            select @fileid,{row_year},{row_count},null, '{url}','{filename}'
                        end
                    """
                    ExecQ(connstr,_sql)                  
def GetDF(connstr,sqlQry):        
    try:        
        connection_url = URL.create("mssql+pyodbc",query={"odbc_connect": connstr})    
        engine = create_engine(connection_url)
        conn = engine.connect()        
        return_df = pd.read_sql_query(sqlQry, conn)
        return return_df
    except Exception as e:
        print(f"ExecQ {sqlQry} ",e)        
def ExecQ(connstr,sqlQry):        
    try:
        connection_url = URL.create("mssql+pyodbc",query={"odbc_connect": connstr})    
        engine = create_engine(connection_url)
        with engine.connect() as conn:
            conn.execute(text(sqlQry))
            conn.commit()
            conn.close()
             
    except Exception as e:
        print(f"Error ExecQ ",e)
def build_tables(connstr):
    print("Check and build tables")
    _sql = """
            if object_id(N'dbo.NCCS_Tables',N'U') is null
            begin
            CREATE TABLE [dbo].[NCCS_Tables](
                [RecordID] [int] IDENTITY(1,1) NOT NULL,
                [TableNames] [nvarchar](150) NULL,
                [Include] [bit] NULL
            ) ON [PRIMARY]
            end;
           if object_id(N'dbo.NCCS_Tables_FileRecord',N'U') is null
            begin
            CREATE TABLE [dbo].[NCCS_Tables_FileRecord](
                [FileID] [int] NULL,
                [FileName] nvarchar(300) NULL,
                [url] nvarchar(4000) NULL,
                [DataYear] [Int] NULL,                
                [RowsLoaded] [float] NULL,
                [RowCount] [float] NULL,
                [LastUpdate] date NULL                
            ) ON [PRIMARY]
            end            
            if object_id(N'dbo.NCCS_Tables_Columns',N'U') is null
            begin
                CREATE TABLE [dbo].[NCCS_Tables_Columns](
                    [FileID] [int] NULL,
                    [ColName] [nvarchar](150) NULL,
                    [ColType] [nvarchar](150) NULL,
                    [Description] [nvarchar](4000) NULL,
                    [OrderID] [int] NULL
                ) ON [PRIMARY]
            end    
            if object_id(N'dbo.base_statecodes',N'U') is null
            begin
            CREATE TABLE [dbo].[base_statecodes](
                [StateCode] [nvarchar](3) NULL,
                [StateName] [nvarchar](150) NULL                
            ) ON [PRIMARY]
            end;        
    """    
    ExecQ(connstr,_sql)
   
   
                 