CREATE TABLE IF NOT EXISTS gold_patients
WITH (format='parquet', external_location='s3://hospital-inpatients-pipeline/gold/patients') AS
select distinct patient_id, patient_forename, patient_surname, patient_dob
from silver_episodes
  
  