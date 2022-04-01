## ========================= Amplitude POC ====================================================== ##

create table amplitude_data_final as 
with cte as 
(
select * from agri_reports.DF_ampdata_01_07_2021_31_12_2021
union all
select * from agri_reports.ampdata_01_01_2022_25_03_2022
) select * from cte;

select LENGTH(event_properties), event_properties
from amplitude_data_final # where RIGHT(event_properties, 1) = '}' or ']'
 order by LENGTH(event_properties) DESC ;

# json columns
select event_properties, user_properties,global_user_properties, group_properties, data, groups from agri_reports.amplitude_data_final;

select DISTINCT event_properties from agri_reports.amplitude_data_final; # having data
select DISTINCT user_properties from agri_reports.amplitude_data_final; # having data
select DISTINCT global_user_properties from agri_reports.amplitude_data_final; # blank
select DISTINCT group_properties from agri_reports.amplitude_data_final; # blank
select DISTINCT data from agri_reports.amplitude_data_final; # blank
select DISTINCT groups from agri_reports.amplitude_data_final; # blank



select device_id, event_properties,
JSON_UNQUOTE(JSON_EXTRACT(event_properties, '$.otp')) event_otp,
JSON_UNQUOTE(JSON_EXTRACT(event_properties, '$.mobile')) event_mobile,
JSON_UNQUOTE(JSON_EXTRACT(event_properties, '$.User ID')) event_user_id,
JSON_UNQUOTE(JSON_EXTRACT(event_properties, '$.page')) event_page_name,
JSON_UNQUOTE(JSON_EXTRACT(event_properties, '$.data')) event_data_json,
JSON_UNQUOTE(JSON_EXTRACT(event_properties, '$.index')) event_data_json
from amplitude_data_final
order by LENGTH(event_properties) DESC 
;


SELECT user_id, json_data FROM articles WHERE json_data.title LIKE "%CPU%"

SELECT user_id, json_data
FROM articles 
WHERE json_extract(json_data, '$.title') LIKE '%CPU%';

