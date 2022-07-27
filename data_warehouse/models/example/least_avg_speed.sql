
select type, avg_speed_per_type from {{ ref("distribution") }} order by avg_speed_per_type 