import logging
from datetime import datetime
from pathlib import Path

def setup_logging(debug=False):
    log_dir = Path(__file__).resolve().parent / "logs"
    log_dir.mkdir(exist_ok=True)

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y%m%d %H:%M:%S",
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
