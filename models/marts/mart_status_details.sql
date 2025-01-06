select split_part("model", '_', 1) as model_type,
    case when model_type = 'mart' then 'marts'
        when model_type = 'int' then 'transformations'
        when model_type = 'stg' then 'staging'
    end as model_folder,
    "model",
    page,
    timestamp_col,
    date_trunc('second', "timestamp") as "timestamp",
    records::uinteger as records
from {{ ref('int_status_details') }}
