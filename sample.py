import pandas as pd
import requests
from bs4 import BeautifulSoup
from io import BytesIO
import re
 
 
catalog_url = (
    "https://urbaninstitute.github.io/nccs/"
    "catalogs/catalog-efile-v2_1.html"
)
 
year = 2023
rows_to_pull = 100
 
 
export_path = r""
 
 
def clean_sheet_name(table_name, used_names):
    """
    Creates a valid, unique Excel worksheet name.
 
    Excel worksheet names:
    - cannot exceed 31 characters
    - cannot contain: \ / ? * [ ] :
    """
 
    sheet_name = re.sub(r'[\\/*?:\[\]]', '_', table_name)
 
    sheet_name = sheet_name[:31]
 
    original_name = sheet_name
    counter = 1
 
    while sheet_name.lower() in used_names:
        suffix = f"_{counter}"
        sheet_name = original_name[:31 - len(suffix)] + suffix
        counter += 1
 
    used_names.add(sheet_name.lower())
 
    return sheet_name
 
 
print("Reading NCCS data catalog...")
 
response = requests.get(
    catalog_url,
    timeout=60
)
 
response.raise_for_status()
 
soup = BeautifulSoup(
    response.text,
    "html.parser"
)
 
 
available_tables = []
 
for heading in soup.find_all("h3"):
    table_name = heading.get_text(strip=True)
    if not re.match(r"^(F9|SA|SB|SC|SD|SE|SF|SG|SH|SI|SJ|SK|SL|SM|SN|SO|SR)-",table_name):
        continue
 
    table = heading.find_next("table")
 
    if table is None:continue
 
    for row in table.find_all("tr"):
 
        cells = row.find_all(["td", "th"])
 
        if not cells:
            continue
 
        row_year = cells[0].get_text(strip=True)
 
        if row_year == str(year):
            download_link = row.find("a",href=True)
            if download_link:
                url = download_link["href"] 
                available_tables.append({"table_name": table_name,"url": url})
 
            break
 
 
print(
    f"Found {len(available_tables)} tables "
    f"available for {year}."
)
 
 
used_sheet_names = set()
index_rows = []
 
 
with pd.ExcelWriter(
    export_path,
    engine="openpyxl"
) as writer:
 
    total_tables = len(available_tables)
 
    for number, item in enumerate(
        available_tables,
        start=1
    ):
 
        table_name = item["table_name"]
        url = item["url"]
 
        print(
            f"[{number}/{total_tables}] "
            f"Reading {table_name}..."
        )
 
        try:
 
 
            file_response = requests.get(
                url,
                timeout=120
            )
 
            file_response.raise_for_status()
 
 
            df = pd.read_csv(
                BytesIO(file_response.content),
                nrows=rows_to_pull,
                low_memory=False
            )
 
 
            sheet_name = clean_sheet_name(
                table_name,
                used_sheet_names
            )
 
 
            df.to_excel(
                writer,
                sheet_name=sheet_name,
                index=False
            )
 
 
            index_rows.append(
                {
                    "Table Name": table_name,
                    "Worksheet": sheet_name,
                    "Tax Year": year,
                    "Rows Exported": len(df),
                    "Columns": len(df.columns),
                    "URL": url,
                    "Status": "Loaded"
                }
            )
 
        except Exception as e:
 
            print(
                f"    ERROR: {e}"
            )
 
            index_rows.append(
                {
                    "Table Name": table_name,
                    "Worksheet": "",
                    "Tax Year": year,
                    "Rows Exported": 0,
                    "Columns": 0,
                    "URL": url,
                    "Status": f"ERROR: {e}"
                }
            )
 
 
    index_df = pd.DataFrame(index_rows)
 
    index_df.to_excel(
        writer,
        sheet_name="INDEX",
        index=False
    )
 
 
print("\nFinished.")
print(f"Excel file created at: {export_path}")