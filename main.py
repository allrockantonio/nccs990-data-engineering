import tomllib
import buildnccs 
import filecatalog
import dictionary
import sys
from sqlalchemy import create_engine
import sys
import logging
import http.client

with open("config.toml","rb") as conffile:
    config = tomllib.load(conffile)
    connstr=config['dbconn']['connstr']
    catalog_url=config['nccsurl']['catalog_url']
    catalog_dict_url=config['nccsurl']['catalog_dict']

# http.client.HTTPConnection.debuglevel = 1
# from sqlalchemy import table
# logging.basicConfig()
# logging.getLogger().setLevel(logging.DEBUG)
# requests_log = logging.getLogger("requests.packages.urllib3")
# requests_log.setLevel(logging.DEBUG)
# requests_log.propagate = True

if __name__ == "__main__":        
    dictionary.get_columns_information(catalog_dict_url,connstr) # create initial tables for catalog record
    actionlist = []        
    if len(sys.argv) > 1:
        for param in sys.argv:
            if (param.lower() == "buildtables"):actionlist.append("buildtables")                            
            if (param.lower() == "loaddata"):actionlist.append("loaddata")
            if (param.lower() == "builddim"):actionlist.append("builddim")                
    else:
        print("Parameters           Desciption")
        print("buildtables          Create base tables")
        print("builddim             Create dim tables")
        print("loaddata             Load data catalog into raw tables")
        sys.exit(0)
    for parrun in actionlist:
        if parrun == "buildtables":
            print("Build repository database structure...")
            buildnccs.build_tables(connstr) # create initial tables for catalog record            
            print("Create table list from html...")
            buildnccs.get_tables_list(catalog_url,connstr) # create initial tables for catalog record
        if parrun == "buildstatedim":
            print("Building dim tables")
            buildnccs.build_dimension_tables(connstr) # create dimension tables for state and county codes                             
        if parrun == "loaddata":
            print("Download and Load data catalog into raw tables")
            filecatalog.update_filecatalogs(connstr,catalog_url)
    print("complete sss")

    



    