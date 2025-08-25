-- SCD1.sql — load raw -> upsert into CUSTOMER (SCD1) and clear RAW
USE DATABASE SCD_DEMO;
USE SCHEMA   SCD2;

-- Stored procedure does:
--   1) Disable nondeterministic-merge error for this session
--   2) MERGE RAW -> CUSTOMER using null-safe comparisons
--   3) TRUNCATE RAW
CREATE OR REPLACE PROCEDURE SCD_DEMO.SCD2.PDR_SCD_DEMO()
RETURNS STRING NOT NULL
LANGUAGE JAVASCRIPT
AS
$$
  // 1) Session setting for MERGE behavior
  snowflake.createStatement({
    sqlText: "ALTER SESSION SET ERROR_ON_NONDETERMINISTIC_MERGE = FALSE"
  }).execute();

  // 2) SCD1 upsert using null-safe comparisons (IS DISTINCT FROM)
  const mergeSql = `
    MERGE INTO SCD_DEMO.SCD2.CUSTOMER AS c
    USING SCD_DEMO.SCD2.CUSTOMER_RAW AS cr
      ON c.CUSTOMER_ID = cr.CUSTOMER_ID
    WHEN MATCHED AND (
         c.FIRST_NAME IS DISTINCT FROM cr.FIRST_NAME
      OR c.LAST_NAME  IS DISTINCT FROM cr.LAST_NAME
      OR c.EMAIL      IS DISTINCT FROM cr.EMAIL
      OR c.STREET     IS DISTINCT FROM cr.STREET
      OR c.CITY       IS DISTINCT FROM cr.CITY
      OR c.STATE      IS DISTINCT FROM cr.STATE
      OR c.COUNTRY    IS DISTINCT FROM cr.COUNTRY
    )
    THEN UPDATE SET
         FIRST_NAME       = cr.FIRST_NAME
       , LAST_NAME        = cr.LAST_NAME
       , EMAIL            = cr.EMAIL
       , STREET           = cr.STREET
       , CITY             = cr.CITY
       , STATE            = cr.STATE
       , COUNTRY          = cr.COUNTRY
       , UPDATE_TIMESTAMP = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
      INSERT (CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, STREET, CITY, STATE, COUNTRY)
      VALUES (cr.CUSTOMER_ID, cr.FIRST_NAME, cr.LAST_NAME, cr.EMAIL, cr.STREET, cr.CITY, cr.STATE, cr.COUNTRY);
  `;
  snowflake.createStatement({ sqlText: mergeSql }).execute();

  // 3) Clear RAW after successful merge
  snowflake.createStatement({
    sqlText: "TRUNCATE TABLE SCD_DEMO.SCD2.CUSTOMER_RAW"
  }).execute();

  return 'OK';
$$;

-- Task to run the SCD1 load periodically
-- (Make sure the task owner role has USAGE on COMPUTE_WH and EXECUTE on the procedure)
CREATE OR REPLACE TASK SCD_DEMO.SCD2.TSK_SCD_RAW
  WAREHOUSE = COMPUTE_WH
  SCHEDULE  = '1 MINUTE'
AS
  CALL SCD_DEMO.SCD2.PDR_SCD_DEMO();

-- Enable the task
ALTER TASK SCD_DEMO.SCD2.TSK_SCD_RAW RESUME;

-- Optional: inspect next runs
-- SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
--   WHERE NAME = 'SCD_DEMO.SCD2.TSK_SCD_RAW' ORDER BY SCHEDULED_TIME DESC;
