create table route_destination (routedest_id serial, route_id text, headsign text);
insert into route_destination (route_id, headsign) with gvb as (select route_id, trip_headsign, rank, row_number() over(partition by route_id order by rank desc) AS rk from (select route_id, trip_headsign, count(*) as rank from gtfs_trips group by route_id, trip_headsign) as sub) select route_id, trip_headsign from gvb where rk <= 2;
create table route_destination_stops (id serial, routedest_id integer, stop_id text);
INSERT INTO route_destination_stops (routedest_id, stop_id) select distinct routedest_id, stop_id from gtfs_stop_times, gtfs_trips, route_destination where gtfs_stop_times.trip_id = gtfs_trips.trip_id and gtfs_trips.route_id = route_destination.route_id and gtfs_trips.trip_headsign = route_destination.headsign;
CREATE INDEX route_destination_stops_idx ON route_destination_stops (stop_id);
