with trips as (
    select * from {{ ref('stg_yellow_trips') }}
)

select
    trips.pickup_date,
    trips.pu_location_id as location_id,
    zones.borough,
    zones.zone_name,
    count(*) as trips,
    round(sum(trips.total_amount), 2) as revenue,
    round(avg(trips.fare_amount), 2) as avg_fare,
    round(avg(trips.trip_minutes), 1) as avg_trip_minutes,
    round(sum(trips.tip_amount) / nullif(sum(trips.fare_amount), 0), 4) as tip_rate,
    round(avg(case when trips.payment_method = 'credit_card' then 1 else 0 end), 4) as card_share
from trips
left join {{ ref('dim_zones') }} as zones
    on trips.pu_location_id = zones.location_id
group by all
