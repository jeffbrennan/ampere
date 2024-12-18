-- usage: to be copied into ampere/data/ folder and executed from there
create view if not exists commits as
select *
from delta_scan("../../ampere/data/bronze/commits");

create view if not exists forks as
select *
from delta_scan("../../ampere/data/bronze/forks");

create view if not exists issues as
select *
from delta_scan("../../ampere/data/bronze/issues");

create view if not exists pull_requests as
select *
from delta_scan("../../ampere/data/bronze/pull_requests");

create view if not exists releases as
select *
from delta_scan("../../ampere/data/bronze/releases");

create view if not exists repos as
select *
from delta_scan("../../ampere/data/bronze/repos");

create view if not exists stargazers as
select *
from delta_scan("../../ampere/data/bronze/stargazers");

create view if not exists users as
select *
from delta_scan("../../ampere/data/bronze/users");

create view if not exists watchers as
select *
from delta_scan("../../ampere/data/bronze/watchers");

create view if not exists followers as
select *
from delta_scan("../../ampere/data/bronze/followers");

create view if not exists pypi_downloads as
select *
from delta_scan("../../ampere/data/bronze/pypi_downloads");

create view if not exists pypi_download_queries as
select *
from delta_scan("../../ampere/data/bronze/pypi_download_queries");