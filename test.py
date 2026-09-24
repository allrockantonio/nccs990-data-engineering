import pandas as pd
import requests
from bs4 import BeautifulSoup
from io import BytesIO
import re
import sys
import pyarrow

from sqlalchemy import table




def get_available_tables(years, response_text):
    soup = BeautifulSoup(response_text,"html.parser")
    available_tables = []
    for year in years:  
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
                if (row_year == str(year)):
                    download_link = row.find("a", href=True)
                    if download_link:
                        url = download_link["href"]
                        available_tables.append({"table_name": table_name,"url": url})                    
                    break       
    return available_tables

def tables_to_parquet(available_tables):
    """Download each available table and write it to a Parquet file."""
    written_files = []

    for table_info in available_tables:
        table_name = table_info["table_name"]
        url = table_info["url"]
        output_path = f"{table_name}.parquet"        

        try:
            
            file_response = requests.get(url, timeout=120)
            file_response.raise_for_status()
            df = pd.read_csv(BytesIO(file_response.content),nrows=100, low_memory=False)
            col_list = df.columns.tolist()
            print(f"Columns in {table_name}")            
            for col in col_list:
                print(col)

            #df.to_parquet(output_path, index=False)
            #written_files.append(output_path)
        except (requests.RequestException, pd.errors.ParserError, OSError) as error:
            print(f"Failed to process {url}: {error}")

        break # remove later if you want to process all tables, currently it breaks after the first one

    return written_files
    
if __name__ == "__main__":
    years = [2023]
    catalog_url = (
        "https://urbaninstitute.github.io/nccs/"
        "catalogs/catalog-efile-v2_1.html"
    )
    res = requests.get(catalog_url,timeout=160)    
    if res.status_code == 200:    
        available_tables = get_available_tables(years, res.text)            
        if len( available_tables) != 0:            
            tables_to_parquet(available_tables)
    else:
        print('error')
        sys.exit(1)

