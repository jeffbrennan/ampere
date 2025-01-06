# [ampere](ampere.jeffbrennan.dev)

Tooling to track and visualize engagement with the [mrpowers-io](https://github.com/mrpowers-io) organization

---
![site](docs/site.png)

## Metrics

## Data

![data model](docs/assets.svg)

### Sources

#### [GitHub REST API](https://docs.github.com/en/rest)

updated 4 times a day

- repos
- stars
- issues
- commits
- forks
- releases
- pull requests

#### [Bigquery PyPi Downloads](https://console.cloud.google.com/marketplace/product/gcp-public-data-pypi)

updated daily

- python release downloads
grouped by package version, python version, operating system, cloud platform

additionally gets download statistics for core project dependencies

- pyspark
- deltalake

## Pages

### ampere

Weekly github metrics

- stars
- issues
- commits

### downloads

Weekly download statistics. Currently only includes python release downloads

- overall
- package version
- python version
- cloud platform

### feed

scrolling, color-coded table of recent organization activity

- stars
- issues
- commits
- forks
- pull requests

### issues

summary and details of current issues

- summary: open issues, median issue age, issues closed this month
- details: repo, author title, body, date, days old, comments

### networks

network graphs

#### network stargazers

each star is a node, and people who have starred multiple repositories are connected to each other
density of lines indicates common groups of stars
edges of each cluster indicate users who have only starred one repository

#### network followers

each user is a node, internal (another organization user) followers and following are connected
includes any user with a record in one of these tables:

- stg_stargazers
- stg_forks
- stg_commits
- stg_issues
- stg_pull_requests

blue edges are a mutual connection, grey edges are one-way
color of node indicates number of followers

percentage of internal followers and following computed against user total followers and following

### status

data quality page indicating the last time each data source was updated and the number of records in each table

### about

repo summaries and links

## Tech Stack

This project is build with an open source (d)ata stack:

- [Dagster](https://github.com/dagster-io/dagster): data orchestration
- [Dash](https://github.com/plotly/dash): visualization
- [Delta](https://github.com/delta-io/delta): raw data storage
- [dbt](https://github.com/dbt-labs/dbt-core): data transformation
- [Docker](https://github.com/moby/moby): containerization
- [DuckDB](https://github.com/duckdb/duckdb): database

## TODOs

- [ ] dark mode
- [ ] mobile improvements
- [ ] create API
- [ ] add `ampere` to `mrpowers-io` organization
