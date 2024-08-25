
{{ config(
    materialized="table"
) }}

with dim_restaurants as (
SELECT
    r.restaurant_id,
    r.restaurant_name,
    LISTAGG(c.cuisine, ', ') WITHIN GROUP (ORDER BY c.cuisine) AS cuisines,
    r.city,
    r.state as state_code,
    s.state as state,
    r.zip_code
FROM {{ source('src_raw', 'raw_restaurants') }}  r
JOIN {{ source('src_raw', 'raw_cuisine') }}  c on c.restaurant_id = r.restaurant_id
JOIN {{ ref( 'states') }} s on r.state = s.state_code
GROUP BY r.restaurant_id, r.restaurant_name, r.city, s.state,r.state, r.zip_code
)
select * from dim_restaurants
