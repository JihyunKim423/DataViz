# Join region name and country name to the store location dataset.
select c.region as Region, c.name as CountryName, l.*
from starbucks.store_location l join starbucks.continents c on l.Country = c.iso2;

# Impute missing values properly and extract aggregated vales with group by
select * from starbucks.customer_event limit 10;
select * from starbucks.customer_profile limit 10;

select event, count(*) from starbucks.customer_event group by 1;

# Offer performance dataset
with FLAG as (
select person as id,
		case when event = "offer received" then 1 else 0 end as offer_received,
		case when event = "offer viewed" then 1 else 0 end as offer_viewed,
		case when event = "transaction" then 1 else 0 end as transaction,
		case when event = "offer completed" then 1 else 0 end as offer_completed        
from starbucks.customer_event)
, MAX_VAL as (
select id, 
max(offer_received) as offer_received,
max(offer_viewed) as offer_viewed, 
max(transaction) as transaction, 
max(offer_completed) as offer_completed
from FLAG
group by 1)
, AGG as (
select offer_received, offer_viewed, transaction, offer_completed, count(id) as customer_count
from MAX_VAL
group by 1,2,3,4)
select (select sum(customer_count) from AGG where offer_received = 1) as offer_received_cnt,
		(select sum(customer_count) from AGG where offer_received = 1 and offer_viewed = 1) as offer_viewed_cnt,
        (select sum(customer_count) from AGG where offer_received = 1 and offer_viewed = 1 and transaction=1 and offer_completed = 1) as offer_purchase_cnt,
        (select sum(customer_count) from AGG where (offer_received = 1 and offer_viewed = 0 and transaction=1 and offer_completed = 0) or (offer_received = 1 and offer_completed = 0 and transaction=1)) as organic_purchase_cnt;

# Customer profile dataset
with IMPUTE as (
select id, case when length(trim(gender)) = 0 then "N/A"
				when gender = "O" then "N/A" 
                else gender end as gender, 
max(coalesce(age, round((select avg(age) from starbucks.customer_profile),0))) as age,
max(coalesce(income, round((select avg(income) from starbucks.customer_profile),0))) as income
from starbucks.customer_profile
group by 1,2)
select gender, 
case when age < 20 then "10s and below"
	 when age between 21 and 29 then "20s"
     when age between 30 and 39 then "30s"
     when age between 40 and 49 then "40s"
     when age between 50 and 59 then "50s"
     when age between 60 and 69 then "60s"
     when age >=70 then "above 70s"
     else "N/A" end as age_band,
case when income < 80000 then "less than 80K"
	 when income between 80000 and 100000 then "80K-100K"
     when income > 100000 then "greater than 100K"
     else null end as income_band,
count(distinct id) as customer_count  
from IMPUTE
group by 1,2,3
;

