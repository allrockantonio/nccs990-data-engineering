import logging
from datetime import datetime
import tomllib
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sqlalchemy import text
import logging
from pathlib import Path
logger = logging.getLogger(__name__)


def getconfig(setting):
    config_path = Path(__file__).with_name("config.toml")
    with config_path.open("rb") as file:
        config = tomllib.load(file)        
        if setting=="connstr":
            return config['dbconn']['connstr']
        if setting=="catalogurl":
            return config['nccsurl']['catalog_url']
        if setting=="catalogdict":        
            return config['nccsurl']['catalog_dict']
        if setting=="raw_data":        
            return config['output']['raw_data']        
        if setting=="debug":        
            return config['logging']['debug']            
        if setting=="build":        
            return config['output']['build']       




def setup_logging(debug=False):
    log_dir = Path(__file__).resolve().parent / "logs"
    log_dir.mkdir(exist_ok=True)

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
    )

    file_handler = logging.FileHandler(
        log_dir / f"{datetime.now():%Y%m%d}.log", encoding="utf-8"
    )
    file_handler.setLevel(logging.DEBUG if debug else logging.INFO)
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    logging.basicConfig(
        level=logging.DEBUG if debug else logging.INFO,
        handlers=[file_handler, console_handler],
        force=True,
    )    

def GetDF(connstr,sqlQry):        
    try:        
        connection_url = URL.create("mssql+pyodbc",query={"odbc_connect": connstr})    
        engine = create_engine(connection_url)
        conn = engine.connect()        
        return_df = pd.read_sql_query(sqlQry, conn)
        logger.debug(f"debug getdf:{sqlQry}")
        return return_df
    except Exception as e:
        print(f"ExecQ {sqlQry} ",e)        
        df = pd.DataFrame()        
        logger.error("getdf")
        logger.error(e)
        return
def ExecQ(connstr, sqlQry, params=None):
    engine = None
    try:
        connection_url = URL.create(
            "mssql+pyodbc",
            query={"odbc_connect": connstr},
        )
        engine = create_engine(connection_url)

        with engine.begin() as conn:
            conn.execute(text(sqlQry), params or {})

        return True
    except Exception:
        logger.exception("ExecQ failed")
        return False
    finally:
        if engine is not None:
            engine.dispose()
