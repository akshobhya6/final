
create table input_ec(brand string,name string,price integer,ratings float, total_rating integer,total_reviews integer, ram integer, rom integer)
row format delimited
fields terminated by ','
stored as textfile;


load data inpath '{{ params.input_path }}/h1.csv' overwrite into table input_ec;


set hive.strict.checks.cartesian.product = false;

create table avg (average_value float)
row format delimited
fields terminated by ','
stored as textfile;

insert into avg 
select avg(ratings) from input_ec;

create table temp1 (average_value float, name string, ratings float, total_rating int)
row format delimited
fields terminated by ','
stored as textfile;

insert into temp1
select a.average_value as
average_value, b.name as name, b.ratings as ratings , 
b.total_rating as total_rating 
from input_ec b
cross join avg a;


create table final1 (average_value float, name string, ratings float, total_rating int)
row format delimited
fields terminated by ','
stored as textfile;

insert into final1
select * from temp1
order by total_rating*(ratings-3) desc limit 10; 


create table result2(name string, price int, catagory string)
row format delimited
fields terminated by ','
stored as textfile;

insert into result2
select name, price,
case when price>0 and price<10000 then "budget_phone"
when price>25001 and price<50000 then "top_range"
when price>10001 and price<25000 then "mid_range_phone"
when price>50000 and price<999999 then "premium_range"
else null
end as catagory 
from input_ec
order by price;

create table t1(name string, price int, catagory string)
row format delimited
fields terminated by ','
stored as textfile;

create table t2(name string, price int, catagory string)
row format delimited
fields terminated by ','
stored as textfile;

create table t3(name string, price int, catagory string)
row format delimited
fields terminated by ','
stored as textfile;

create table t4(name string, price int, catagory string)
row format delimited
fields terminated by ','
stored as textfile;

insert into t1 select * from result2 where catagory='budget_phone'order by price desc limit 5;

insert into t2 select * from result2 where catagory='mid_range_phone'order by price desc limit 5;

insert into t3 select * from result2 where catagory='top_phone'order by price desc limit 5;

insert into t4 select * from result2 where catagory='premium_phone'order by price desc limit 5;

create table final2 (name string, price int, catagory string)
row format delimited
fields terminated by ','
stored as textfile;

Insert into final2 
(select name, price, catagory from t1 order by price) union
(select name, price, catagory from t2 order by price) union
(select name, price, catagory from t3 order by price) union
(select name, price, catagory from t4 order by price);

create table final21 (name string, price int, catagory string)
row format delimited
fields terminated by ','
stored as textfile;

insert into final21
select * from final2 group by catagory, price, name order by price;


create table med (median float)
row format delimited
fields terminated by ','
stored as textfile;

insert into med
select percentile(cast(ratings as bigint),0.5)
from input_ec;

create table temp3 (median float, name string,price int, ratings float, total_rating int)
row format delimited
fields terminated by ','
stored as textfile;


insert into temp3
select a.median,b.name,b.price,b.ratings, b.total_rating
from input_ec b
cross join med a
where b.ratings>= a.median
order by ratings desc
limit 10;

create table final3 (median float, name string,price int, ratings float, total_rating int)
row format delimited
fields terminated by ','
stored as textfile;

insert into final3
select * from temp3 order by ratings desc;
    


set hive.exec.dynamic.partition = true;
set hive.exec.dynamic.partition.mode=nonstrict;

create table temp4(brand string, name string,ram int, rom int,  rn int)
row format delimited
fields terminated by',';

insert into temp4
select brand, name, ram, rom, rank() over  (partition by brand order by ram desc, rom desc) rn
from input_ec;

create table final4 (brand string,name string,ram int, rom int )
row format delimited
fields terminated by',';

insert into final4
select brand,name, ram, rom
from temp4
where rn=1;
