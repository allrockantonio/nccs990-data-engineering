import functions as fnc
import logging
from datetime import date
from pathlib import Path
import pandas as pd
from process_timer import ProcessTimer
import sys
logger = logging.getLogger(__name__)
def run():
    timer = ProcessTimer(f"Dimensions", logger)
    create_dim_date()
    timer.step("DIM_Dates")
    create_base_state()
    timer.step("DIM_States")
    timer.total()

def create_base_country():    
    file = Path(__file__).resolve().parent / "files/code_country.csv"
    if not file.is_file():
        raise FileNotFoundError(f"Country lookup CSV not found: {file}")

    df = pd.read_csv(file, dtype="string", keep_default_na=False)
    required = {"alpha2","alpha3","country","callcode"}
    if not required.issubset(df.columns):
        logger.error(f"Columns alpha2, alpha3 , country and callcode not found")
        sys.exit(0)
    df["alpha2"] = df["alpha2"].str.strip().str.upper()
    df["alpha3"] = df["alpha3"].str.strip().str.upper()
    df["country"] = df["country"].str.strip()
    df["callcode"] = df["callcode"].str.strip().str.upper()
    if (df["alpha2"].eq("") | df["alpha3"].eq("") | df["country"].eq("")| df["callcode"].eq("") ).any():
        logger.error(f"Columns should no be empty")
        sys.exit(0)

    sql = """
    IF OBJECT_ID(N'raw.base_countrycodes', N'U') IS NULL
    BEGIN
        CREATE TABLE raw.base_countrycodes (
            alpha2 nvarchar(2) NOT NULL PRIMARY KEY,
            alpha3 nvarchar(3) NOT NULL,
            country nvarchar(100) NOT NULL,
            callcode nvarchar(5) NOT NULL
        );
    END;
    """
    if not fnc.ExecQ(sql):
        logger.error(f"Failed to create base_countrycodes")
        sys.exit(0)

    sql = """
    IF NOT EXISTS (
        SELECT 1 FROM raw.base_countrycodes WHERE alpha2 = :alpha2
    )
    BEGIN
        INSERT INTO raw.base_countrycodes (alpha2, alpha3,country,callcode)
        VALUES (:alpha2, :alpha3, :country,:callcode);
    END;
    """
    for row in df.itertuples(index=False):
        params = {"alpha2": row.alpha2, "alpha3": row.alpha3}
        if not fnc.ExecQ(sql, params):
            logger.error(f"Failed to load state code")
            sys.exit(0)  
    return True
def create_base_state():    
    file = Path(__file__).resolve().parent / "files/state_codename.csv"
    if not file.is_file():
        raise FileNotFoundError(f"State lookup CSV not found: {file}")

    df = pd.read_csv(file, dtype="string", keep_default_na=False)
    required = {"state_code", "state_name"}
    if not required.issubset(df.columns):
        logger.error(f"Columns state_code and state_name not found")
        sys.exit(0)
    df["state_code"] = df["state_code"].str.strip().str.upper()
    df["state_name"] = df["state_name"].str.strip()
    if (df["state_code"].eq("") | df["state_name"].eq("")).any():
        logger.error(f"Columns state_code and state_name should no be empty")
        sys.exit(0)

    sql = """
    IF OBJECT_ID(N'raw.base_statecodes', N'U') IS NULL
    BEGIN
        CREATE TABLE raw.base_statecodes (
            StateCode nvarchar(20) NOT NULL PRIMARY KEY,
            StateName nvarchar(100) NOT NULL
        );
    END;
    """
    if not fnc.ExecQ(sql):
        logger.error(f"Failed to create base_statecodes")
        sys.exit(0)

    sql = """
    IF NOT EXISTS (
        SELECT 1 FROM raw.base_statecodes WHERE StateCode = :state_code
    )
    BEGIN
        INSERT INTO raw.base_statecodes (StateCode, StateName)
        VALUES (:state_code, :state_name);
    END;
    """
    for row in df.itertuples(index=False):
        params = {"state_code": row.state_code, "state_name": row.state_name}
        if not fnc.ExecQ(sql, params):
            logger.error(f"Failed to load state code")
            sys.exit(0)  
    return True

def create_dim_date(start_date="2005-01-01", end_date="2030-12-31"):    
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)

    if start > end:        
        logger.error(f"Start_date must be on or before end_date")
        sys.exit(0)  
    sql = f"""
    IF OBJECT_ID(N'raw.Dim_Date', N'U') IS NULL
    BEGIN
        CREATE TABLE raw.Dim_Date (
            DateKey int NOT NULL PRIMARY KEY,
            FullDate date NOT NULL UNIQUE,
            CalendarYear smallint NOT NULL,
            CalendarQuarter tinyint NOT NULL,
            MonthNumber tinyint NOT NULL,
            MonthName varchar(9) NOT NULL,
            DayOfMonth tinyint NOT NULL,
            DayOfYear smallint NOT NULL,
            DayOfWeek tinyint NOT NULL,
            DayName varchar(9) NOT NULL,
            IsWeekend bit NOT NULL,
            MonthStartDate date NOT NULL,
            MonthEndDate date NOT NULL
        );
    END;

    IF COL_LENGTH(N'raw.Dim_Date', N'YearQuarter') IS NULL
    BEGIN
        EXEC(N'
            ALTER TABLE raw.Dim_Date
            ADD YearQuarter AS (
                CONVERT(varchar(4), CalendarYear) + ''-Q'' +
                CONVERT(varchar(1), CalendarQuarter)
            ) PERSISTED;
        ');
    END;

    -- Seven-day blocks within each month: days 1-7 = week 1.
    IF COL_LENGTH(N'raw.Dim_Date', N'WeekOfMonth') IS NULL
    BEGIN
        EXEC(N'ALTER TABLE raw.Dim_Date ADD WeekOfMonth AS (
            (DAY(FullDate) - 1) / 7 + 1
        ) PERSISTED;');
    END;

    -- ISO weeks start Monday; week 1 contains January 4.
    IF COL_LENGTH(N'raw.Dim_Date', N'WeekOfYear') IS NULL
    BEGIN
        EXEC(N'ALTER TABLE raw.Dim_Date ADD WeekOfYear AS (
            DATEPART(iso_week, FullDate)
        ) PERSISTED;');
    END;

    ;WITH DateOffsets AS (
        SELECT 0 AS DayOffset

        UNION ALL

        SELECT DayOffset + 1
        FROM DateOffsets
        WHERE DayOffset < DATEDIFF(
            day,
            CONVERT(date, '{start:%Y%m%d}', 112),
            CONVERT(date, '{end:%Y%m%d}', 112)
        )
    ),
    Dates AS (
        SELECT DATEADD(
            day,
            DayOffset,
            CONVERT(date, '{start:%Y%m%d}', 112)
        ) AS FullDate
        FROM DateOffsets
    ),
    Calendar AS (
        SELECT
            FullDate,
            -- Monday = 1, Sunday = 7; independent of DATEFIRST.
            (
                (
                    DATEDIFF(
                        day,
                        CONVERT(date, '19000101', 112),
                        FullDate
                    ) % 7 + 7
                ) % 7
            ) + 1 AS WeekdayNumber
        FROM Dates
    )
    INSERT INTO raw.Dim_Date (
        DateKey,
        FullDate,
        CalendarYear,
        CalendarQuarter,
        MonthNumber,
        MonthName,
        DayOfMonth,
        DayOfYear,
        DayOfWeek,
        DayName,
        IsWeekend,
        MonthStartDate,
        MonthEndDate
    )
    SELECT
        YEAR(c.FullDate) * 10000
            + MONTH(c.FullDate) * 100
            + DAY(c.FullDate),
        c.FullDate,
        YEAR(c.FullDate),
        DATEPART(quarter, c.FullDate),
        MONTH(c.FullDate),
        CHOOSE(
            MONTH(c.FullDate),
            'January', 'February', 'March', 'April',
            'May', 'June', 'July', 'August',
            'September', 'October', 'November', 'December'
        ),
        DAY(c.FullDate),
        DATEPART(dayofyear, c.FullDate),
        c.WeekdayNumber,
        CHOOSE(
            c.WeekdayNumber,
            'Monday', 'Tuesday', 'Wednesday', 'Thursday',
            'Friday', 'Saturday', 'Sunday'
        ),
        CASE WHEN c.WeekdayNumber IN (6, 7) THEN 1 ELSE 0 END,
        DATEFROMPARTS(YEAR(c.FullDate), MONTH(c.FullDate), 1),
        EOMONTH(c.FullDate)
    FROM Calendar AS c
    WHERE NOT EXISTS (
        SELECT 1
        FROM raw.Dim_Date AS d
        WHERE d.FullDate = c.FullDate
    )
    OPTION (MAXRECURSION 0);
    """

    if not fnc.ExecQ(sql):        
        logger.error(f"Failed to create/populate raw.Dim_Date")
        sys.exit(0)  

    logger.info("Dim_Date populated from %s to %s", start, end)
    
    return True
