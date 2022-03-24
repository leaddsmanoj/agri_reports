#  =========================== Final st_users script=====================================


# Not EXISTS table
drop table if exists agri_reports.customer_not_exists_in_pdb;
create table agri_reports.customer_not_exists_in_pdb as 
select * from govt_db.st_users su where not EXISTS  
(select 1 from private_db.customers c WHERE su.client_id = c.gov_db_client_id);

#select count(*), client_type from agri_reports.customer_not_exists_in_pdb cneip group by 2;
#select max(registration_date) from agri_reports.customer_not_exists_in_pdb ;
#select * from agri_reports.customer_not_exists_in_pdb order by registration_date DESC limit 1;
#select max(registration_date)  from agri_reports.customer_not_exists_in_pdb cneip  ;
#select DISTINCT client_type  from agri_reports.customer_not_exists_in_pdb ;

#select count(*) from agri_reports.customer_not_exists_in_pdb cneip where client_type = 'Trader' 
#and year(registration_date)=2022 and MONTH(registration_date)=03 and Day(registration_date)=9;
# select DISTINCT client_type  from agri_reports.customer_not_exists_in_pdb cneip ;
# select count(*) from agri_reports.customer_not_exists_in_pdb cneip ; # 6725

# update values for non matching rows
UPDATE agri_reports.customer_not_exists_in_pdb
SET client_type = 
    CASE 
    WHEN client_type = '11' THEN 'Farmer' else 'Trader'
    END;
   
# Exists table
DROP table if exists st_users_complete_match;
create table st_users_complete_match as 
select * from govt_db.st_users su where EXISTS  
(select 1 from private_db.customers c WHERE su.client_id = c.gov_db_client_id);

# update st_users_complete_match
UPDATE agri_reports.st_users_complete_match
SET client_type = 
    CASE 
    WHEN client_type = '11' THEN 'Farmer' else 'Trader'
    END;
#select max(registration_date)  from st_users_complete_match ;
#select count(*)  client_type  from agri_reports.st_users_complete_match ; # 36301
#select DISTINCT client_type  from agri_reports.st_users_complete_match ;

# Both table got Merge
drop table if exists st_users_merge;
create table st_users_merge as 
select * from agri_reports.st_users_complete_match
Union all
select *  from agri_reports.customer_not_exists_in_pdb;

#select  max(registration_date)  from st_users_merge ; 
# Numbers of rows effected

#select max(registration_date) from agri_reports.st_users_final ;

# Dashboard powered up table
drop table if exists agri_reports.st_users_final;
create table agri_reports.st_users_final as # error, st_users already exits
select a.*, c.customer_type from agri_reports.st_users_merge a 
left join private_db.customers c on a.client_id = c.gov_db_client_id ;
