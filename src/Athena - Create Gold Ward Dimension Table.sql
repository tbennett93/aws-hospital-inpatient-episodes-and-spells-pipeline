CREATE TABLE IF NOT EXISTS gold_wards
WITH (format='parquet', external_location='s3://hospital-inpatients-pipeline/gold/wards') AS
select distinct ward as ward_id, ward_name
from silver_episodes
  
  