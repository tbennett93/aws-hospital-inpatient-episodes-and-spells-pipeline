CREATE TABLE silver_episodes
with (
    external_location = 's3://hospital-inpatients-pipeline/silver/episodes',
    format = 'parquet'
)
as
with _data as (
select 
  "spell id" as spell_id, 
  "episode_id", 
  "patient_id" , 
  trim("patient forename") as patient_forename , 
  trim("patient surname") as patient_surname, 
  cast(
    date_parse("patient dob", '%d/%m/%Y') as date
    )as patient_dob, 
  cast(
    date_parse("episode start date", '%d/%m/%Y') as date
    )as episode_start_date, 
  cast(
    date_parse("episode_end_date", '%d/%m/%Y') as date
    )as episode_end_date, 
  trim("ward") as "ward" , 
  trim("ward name")  as ward_name, 
  trim("consultant") as "consultant" , 
  date_parse("dicharge_dttm" , '%d/%m/%Y %T') as dicharge_dttm, 
  date_parse("last_updated_ts" , '%d/%m/%Y %H:%i')  as last_updated_ts, 
  "ingest_dttm" , 
  trim("developer comment") as "developer_comment",
  row_number() over (partition by episode_id order by snapshot_date desc) rn
from bronze_episodes
)

select *
from _data
where rn = 1