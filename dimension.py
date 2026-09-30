import functions as fnc
import logging
from datetime import date
from pathlib import Path
import pandas as pd
from process_timer import ProcessTimer
logger = logging.getLogger(__name__)
def run():
    timer = ProcessTimer(f"Dimensions", logger)
    create_dim_date()
    timer.step("DIM_Dates")
    create_dim_state()
    timer.step("DIM_States")
    timer.total()

def create_dim_state():
    """Load the state lookup used by dim_tables.sql; retain existing codes."""
    file = Path(__file__).resolve().parent / "state_codename.csv"
    if not file.is_file():
        raise FileNotFoundError(f"State lookup CSV not found: {file}")

    df = pd.read_csv(file, dtype="string", keep_default_na=False)
    required = {"state_code", "state_name"}
    if not required.issubset(df.columns):
        raise ValueError("State lookup CSV requires state_code and state_name columns")
    df["state_code"] = df["state_code"].str.strip().str.upper()
    df["state_name"] = df["state_name"].str.strip()
    if (df["state_code"].eq("") | df["state_name"].eq("")).any():
        raise ValueError("State codes and names must not be empty")

    sql = """
    IF OBJECT_ID(N'dbo.base_statecodes', N'U') IS NULL
    BEGIN
        CREATE TABLE dbo.base_statecodes (
            StateCode nvarchar(20) NOT NULL PRIMARY KEY,
            StateName nvarchar(100) NOT NULL
        );
    END;
    """
    if not fnc.ExecQ(sql):
        raise RuntimeError("Failed to create dbo.base_statecodes")

    sql = """
    IF NOT EXISTS (
        SELECT 1 FROM dbo.base_statecodes WHERE StateCode = :state_code
    )
    BEGIN
        INSERT INTO dbo.base_statecodes (StateCode, StateName)
        VALUES (:state_code, :state_name);
    END;
    """
    for row in df.itertuples(index=False):
        params = {"state_code": row.state_code, "state_name": row.state_name}
        if not fnc.ExecQ(sql, params):
            raise RuntimeError(f"Failed to load state code {row.state_code}")    
    return True

def create_dim_date(start_date="2005-01-01", end_date="2030-12-31"):    
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)

    if start > end:
        raise ValueError("start_date must be on or before end_date")
    sql = f"""
    IF OBJECT_ID(N'dbo.Dim_Date', N'U') IS NULL
    BEGIN
        CREATE TABLE dbo.Dim_Date (
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

    IF COL_LENGTH(N'dbo.Dim_Date', N'YearQuarter') IS NULL
    BEGIN
        EXEC(N'
            ALTER TABLE dbo.Dim_Date
            ADD YearQuarter AS (
                CONVERT(varchar(4), CalendarYear) + ''-Q'' +
                CONVERT(varchar(1), CalendarQuarter)
            ) PERSISTED;
        ');
    END;

    -- Seven-day blocks within each month: days 1-7 = week 1.
    IF COL_LENGTH(N'dbo.Dim_Date', N'WeekOfMonth') IS NULL
    BEGIN
        EXEC(N'ALTER TABLE dbo.Dim_Date ADD WeekOfMonth AS (
            (DAY(FullDate) - 1) / 7 + 1
        ) PERSISTED;');
    END;

    -- ISO weeks start Monday; week 1 contains January 4.
    IF COL_LENGTH(N'dbo.Dim_Date', N'WeekOfYear') IS NULL
    BEGIN
        EXEC(N'ALTER TABLE dbo.Dim_Date ADD WeekOfYear AS (
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
    INSERT INTO dbo.Dim_Date (
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
        FROM dbo.Dim_Date AS d
        WHERE d.FullDate = c.FullDate
    )
    OPTION (MAXRECURSION 0);
    """

    if not fnc.ExecQ(sql):
        raise RuntimeError("Failed to create/populate dbo.Dim_Date")

    logger.info("Dim_Date populated from %s to %s", start, end)
    
    return True
