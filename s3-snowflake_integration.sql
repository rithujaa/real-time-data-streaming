create or replace storage integration realtimedata_s3_integration 
    TYPE = EXTERNAL_STAGE
    storage_provider = 's3'
    ENABLED = true
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::072461053412:role/realtime_data_project'
    STORAGE_ALLOWED_LOCATIONS = ('s3://snowflake-db-tutorial-rithujaa/stream_data')

desc integration realtimedata_s3_integration

create or replace stage customer_realtime_stage
    URL = 's3://snowflake-db-tutorial-rithujaa/stream_data'
    STORAGE_INTEGRATION = realtimedata_s3_integration 
    FILE_FORMAT = ( TYPE = CSV FIELD_DELIMITER = ',' SKIP_HEADER = 1 );

list @customer_realtime_stage

create or replace pipe customer_s3_pipe
auto_ingest = true
as
COPY INTO SCD_DEMO.SCD2.customer_raw
from @customer_realtime_stage
  FILE_FORMAT = (
    TYPE = CSV
    FIELD_DELIMITER = ','
    SKIP_HEADER   = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF       = ('NULL','')
  )
;

show pipes

select * from customer_raw 

select count(*) from customer_raw

truncate customer_raw