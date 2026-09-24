import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sqlalchemy import text
import pyodbc
import requests
from bs4 import BeautifulSoup
from io import BytesIO
import re
import buildnccs
def get_columns_information(catalog_url,connstr):    
    res = requests.get(catalog_url,timeout=160)  
    soup = BeautifulSoup(res.text,"html.parser")   
    for heading in soup.find_all("h3"):
        table_name = heading.get_text(strip=True)    
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