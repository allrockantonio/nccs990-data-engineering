import functions as fnc
import logging
import sys
import build 
import ingestdata as idata
from pathlib import Path
import argparse
logger = logging.getLogger(__name__)

def main():
    fnc.setup_logging(fnc.getconfig("debug"))    
    try:
        parser = argparse.ArgumentParser(description="NCCS processing")

        parser.add_argument(
            "--command",
            required=True,
            choices=["build", "loaddata","dictionary"],
        )        
        parser.add_argument("--cleanupdb",dest="tbuild_cleandb",choices=["yes", "no"],default="no",)
        parser.add_argument("--dictionary",dest="tdictionary",choices=["yes", "no"],default="no",)
        parser.add_argument("--download",dest="tdownload",choices=["yes", "no"],default="no",)
        parser.add_argument("--table", dest="ttable")
        parser.add_argument("--file", dest="tfile")                
        parser.add_argument("--buildoption",dest="tbuildoption",choices=["overwrite", "noaction","droptable"],default="noaction",)  
        args = parser.parse_args()
        
        target_tablename = "*"
        target_file = "*"
        buildoption="noaction"

        if args.ttable != None:target_tablename = args.ttable        
        if args.tfile != None:target_file = args.tfile        
        if args.tbuild_cleandb != None:build_cleandb = args.tbuild_cleandb     
        if args.tdictionary != None:dictionary = args.tdictionary     
        if args.tdownload != None:download = args.tdownload
        

        if args.command.lower()=="loaddata" and target_tablename != "*" and target_file!="*":
            if args.tbuildoption != None:buildoption = args.tbuildoption     

        logger.info(f"NCCS Started with {args.command} table {target_tablename}")

        if args.command.lower() == "build":build.run(build_cleandb,dictionary,download)        
        if args.command.lower() == "loaddata":idata.run(target_tablename,target_file,dictionary,buildoption)        
        logger.info("NCCS Completed")
    except KeyboardInterrupt:
        logger.info("Stopped by user")
        return 130
    except Exception:
        logger.exception("NCCS failed")
        logger.exception(Exception)
        return 1
    finally:
        logger.info("NCCS shutting down")        
        logging.shutdown()  # Flush and close log handlers.    

if __name__ == "__main__":
    sys.exit(main())

