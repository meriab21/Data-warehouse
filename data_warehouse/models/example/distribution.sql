
select type, count(*) as total, sum(avg_speed) as total_avg_speed, avg(avg_speed) as avg_speed_per_type, (sum(traveled_d) / 1000) as total_traveled_d_km from data_warehouse group by "type"
