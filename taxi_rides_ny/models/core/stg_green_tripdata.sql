select
    *,
    'Green' as service_type
from {{ source('taxi_data', 'green_tripdata') }}
