select
    trip_id,
    vendor_id,
    pickup_ts,
    dropoff_ts,
    pickup_date,
    passenger_count,
    trip_distance,
    trip_minutes,
    pu_location_id,
    do_location_id,
    case payment_type
        when 1 then 'credit_card'
        when 2 then 'cash'
        when 3 then 'no_charge'
        when 4 then 'dispute'
        else 'other'
    end as payment_method,
    fare_amount,
    tip_amount,
    tolls_amount,
    total_amount,
    ingest_month
from delta_scan('{{ var("silver_path") }}')
