drop table if exists agri_reports.geography;
create table agri_reports.geography
(
area_id int,
area varchar(1000),
city varchar(1000),
pincode varchar(10),
district_name varchar(50),
state varchar(50),
state_short_name varchar(10),
updated_key varchar(100),
updated_on timestamp
);
insert into agri_reports.geography
(
area_id,
area,
city,
pincode,
district_name,
state,
state_short_name,
updated_key,
updated_on
) 
select 
p.id,
p.area,
p.city,
p.pincode,
d.name,
s.name,
s.short_name,
md5(COALESCE(p.id,p.city,d.name,s.name)),
CURRENT_TIMESTAMP() 
from private_db.city_pincodes p 
left join private_db.districts d on p.district_id = d.id
left join private_db.states s on s.id = d.state_id;