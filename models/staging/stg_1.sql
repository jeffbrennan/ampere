select *
from {{ source("main", "repos") }}