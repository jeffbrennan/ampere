create or replace view commits as
select *
from delta_scan("data/bronze/commits");

drop view if exists forks;
create or replace view forks as
select *
from delta_scan("data/bronze/forks");

create or replace view issues as
select *
from delta_scan("data/bronze/issues");

create or replace view pull_requests as
select *
from delta_scan("data/bronze/pull_requests");

create or replace view releases as
select *
from delta_scan("data/bronze/releases");

create or replace view repos as
select *
from delta_scan("data/bronze/repos");

create or replace view stargazers as
select *
from delta_scan("data/bronze/stargazers");

create or replace view users as
select *
from delta_scan("data/bronze/users");


create or replace view followers as
select *
from delta_scan("data/bronze/followers");

create or replace view pypi_downloads as
select *
from delta_scan("data/bronze/pypi_downloads");

create or replace view pypi_download_queries as
select *
from delta_scan("data/bronze/pypi_download_queries");