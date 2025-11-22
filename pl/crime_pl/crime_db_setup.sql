drop table if exists workspace.damg7370_crimes.cpl1_bronze;
drop table if exists workspace.damg7370_crimes.cpl2_silver;

drop table if exists damg7370_crimes.dim_date;
create table if not exists damg7370_crimes.dim_date as
  select
    cast(date_format(date_add('2010-01-01', seq), 'yyyyMMdd') as int) as date_key,
    date_add('2010-01-01', seq) as full_date,
    month(date_add('2010-01-01', seq)) as month_number,
    day(date_add('2010-01-01', seq)) as day_of_month,
    date_format(date_add('2010-01-01', seq), 'MMMM') as month_name,
    quarter(date_add('2010-01-01', seq)) as `quarter`,
    year(date_add('2010-01-01', seq)) as `year`,
    dayofweek(date_add('2010-01-01', seq)) as day_of_week,
    date_format(date_add('2010-01-01', seq), 'EEEE') as day_of_week_name,
    case when dayofweek(date_add('2010-01-01', seq)) in (1,7) then true else false end as is_weekend_flag
  from
    (select posexplode(array_repeat(1, 50000)) as (seq, _)) t;

drop table if exists damg7370_crimes.dim_status;
create streaming table if not exists damg7370_crimes.dim_status(
  status_key bigint generated always as identity (start with 1 increment by 1),
  status string,
  status_desc string,
  last_updated timestamp
);

drop table if exists damg7370_crimes.dim_crime_code;
create streaming table if not exists damg7370_crimes.dim_crime_code(
    crime_code_key bigint generated always as identity (start with 1 increment by 1),
    crime_code int,
    crime_code_desc string,
    last_updated timestamp
);

drop table if exists damg7370_crimes.dim_weapon;
create streaming table if not exists damg7370_crimes.dim_weapon(
    weapon_key bigint generated always as identity (start with 1 increment by 1),
    weapon_used_code int,
    weapon_desc string,
    last_updated timestamp
);

drop table if exists damg7370_crimes.dim_location;
create streaming table if not exists damg7370_crimes.dim_location(
    location_key bigint generated always as identity (start with 1 increment by 1),
    area_id int,
    area_name string,
    rpt_dist_no int,
    location_text string,
    cross_street string,
    last_updated timestamp
);