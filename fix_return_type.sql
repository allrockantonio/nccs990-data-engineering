-- Run against the ingestion database before retrying the failed load.
-- Preserve existing rows while allowing form codes such as 990EZ.
SET XACT_ABORT ON;
BEGIN TRANSACTION;

UPDATE dbo.NCCS_Tables_Columns
SET coltype = 'string'
WHERE orgcolname = 'RETURN_TYPE'
   OR orgcolname LIKE '%[_]RETURN_TYPE';

ALTER TABLE raw.[F9-P07-T00-DIR-TRUST-KEY]
ALTER COLUMN [TAG_RETURN_TYPE] nvarchar(max) NULL;

COMMIT TRANSACTION;
