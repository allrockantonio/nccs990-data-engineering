import functions as fnc
import logging
import sys
import build 
from pathlib import Path
logger = logging.getLogger(__name__)

def main():
    fnc.setup_logging(fnc.getconfig("debug"))    
    try:
        logger.info("NCCS Started")
        if len(sys.argv) > 1:
            for param in sys.argv:
                if (param.lower() == "build"):
                    build.run()
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

