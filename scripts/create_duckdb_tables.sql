create table commits as
select *
from delta_scan("/Users/jeffb/Desktop/Life/personal-projects/ampere/data/bronze/commits");

create table forks as
select *
from delta_scan("/Users/jeffb/Desktop/Life/personal-projects/ampere/data/bronze/forks");

create table issues as
select *
from delta_scan("/Users/jeffb/Desktop/Life/personal-projects/ampere/data/bronze/issues");

create table pull_requests as
select *
from delta_scan("/Users/jeffb/Desktop/Life/personal-projects/ampere/data/bronze/pull_requests");

create table releases as
select *
from delta_scan("/Users/jeffb/Desktop/Life/personal-projects/ampere/data/bronze/releases");

create table repos as
select *
from delta_scan("/Users/jeffb/Desktop/Life/personal-projects/ampere/data/bronze/repos");

create table stargazers as
select *
from delta_scan("/Users/jeffb/Desktop/Life/personal-projects/ampere/data/bronze/stargazers");

create table users as
select *
from delta_scan("/Users/jeffb/Desktop/Life/personal-projects/ampere/data/bronze/users");

create table watchers as
select *
from delta_scan("/Users/jeffb/Desktop/Life/personal-projects/ampere/data/bronze/watchers");

create table followers as
select *
from delta_scan("/Users/jeffb/Desktop/Life/personal-projects/ampere/data/bronze/followers");

create table pypi_downloads as
select *
from delta_scan("/Users/jeffb/Desktop/Life/personal-projects/ampere/data/bronze/pypi_downloads");

create table pypi_download_queries as
select *
from delta_scan("/Users/jeffb/Desktop/Life/personal-projects/ampere/data/bronze/pypi_download_queries");