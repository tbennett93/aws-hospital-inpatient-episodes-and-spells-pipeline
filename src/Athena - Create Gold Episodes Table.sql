CREATE table IF NOT EXISTS hospital_data.gold_episodes
    WITH (format='PARQUET', external_location='s3://hospital-inpatients-pipeline/gold/episodes') AS
SELECT
  spell_id,
  episode_id,
  patient_id,
  episode_start_date,
  ward ,
  consultant ,	
  discharge_dttm,
  case when discharge_dttm is not null then 'Y' else 'N' end as is_discharged,
  row_number() over (partition by spell_id order by episode_start_date ) as episode_order,
  case when row_number() over (partition by spell_id order by episode_start_date desc) = 1 then 'Y' else 'N' end as is_most_recent_episode
FROM
  silver_episodes
