from consts import ZMQ_PUBSUB_KV8_ANNOTATE, pg_connect

import uwsgi
import zmq
from datetime import datetime, timedelta
import psycopg2
import time
import simplejson

# from const import ZMQ_KV78DEMO
COMMON_HEADERS = [('Content-Type', 'application/json'), ('Access-Control-Allow-Origin', '*'), ('Access-Control-Allow-Headers', 'Requested-With,Content-Type')]


def timestampSplit(unixtime):
    dt = datetime.fromtimestamp(unixtime)
    operationaldate = dt.date()
    seconds = (((dt.hour * 60) + dt.minute) * 60) + dt.second
    if (dt.hour < 5):
        operationaldate -= timedelta(days=1)
        seconds += 86400

    return (operationaldate, seconds)

def getClusters(lat=None, lon=None, radius=None, name=None):
    if lat is not None and lon is not None:
        sql = "select clusters.stop_id, clusters.stop_name, clusters.stop_lon, clusters.stop_lat, s.stop_id, s.stop_name, s.stop_lon, s.stop_lat from (select stop_id, stop_name, stop_lon, stop_lat from gtfs_stops where ST_DWithin(the_geom, st_setsrid(st_makepoint(%s, %s), 4326), %s) and parent_station is null) as clusters, gtfs_stops as s where s.location_type = 0 and (clusters.stop_id = s.parent_station OR clusters.stop_id = s.stop_id);"
        param = [lon, lat, radius]
    elif name is not None:
        sql = "select clusters.stop_id, clusters.stop_name, clusters.stop_lon, clusters.stop_lat, s.stop_id, s.stop_name, s.stop_lon, s.stop_lat from (select stop_id, stop_name, stop_lon, stop_lat from gtfs_stops where stop_name ilike %s and parent_station is null) as clusters, gtfs_stops as s where s.location_type = 0 and (clusters.stop_id = s.parent_station OR clusters.stop_id = s.stop_id);"
        param = ['%' + name + '%']
    else:
        sql = "select clusters.stop_id, clusters.stop_name, clusters.stop_lon, clusters.stop_lat, s.stop_id, s.stop_name, s.stop_lon, s.stop_lat from gtfs_stops as clusters, gtfs_stops as s where clusters.parent_station is null and s.location_type = 0 and (clusters.stop_id = s.parent_station OR clusters.stop_id = s.stop_id);"
        param = []
    
    conn = psycopg2.connect(pg_connect)
    cur = conn.cursor()
    cur.execute(sql, param)
    rows = cur.fetchall()

    stopids = "','".join([row[4] for row in rows])

    sql = "select rds.stop_id, rd.route_id, gr.agency_id, rd.headsign, gr.route_short_name, description from gtfs_routes as gr, route_destination as rd, route_destination_stops as rds, gtfs_route_types where gr.route_id = rd.route_id and rd.routedest_id = rds.routedest_id and gr.route_type = gtfs_route_types.route_type and rds.stop_id IN ('"+stopids+"') order by rds.stop_id, rd.route_id;"

    cur.execute(sql)
    routes = cur.fetchall()
    cur.close()
    conn.close()

    stops = {}
    for route in routes:
        line = {'line_id': route[1], 'line_company': route[2], 'line_destination': route[3], 'line_number': route[4], 'line_type': route[5]}
        if route[0] not in stops:
            stops[route[0]] = [line]
        else:
            stops[route[0]].append(line)

    clusters = {}
    for row in rows:
        if row[0] not in clusters:
            clusters[row[0]] = { "cluster_id": row[0], "cluster_name": row[1], "cluster_longitude": row[2], "cluster_latitude": row[3], "stops": [] }
        
        if row[4] in stops:
            lines = stops[row[4]]
        else:
            lines = []

        clusters[row[0]]["stops"].append({"stop_id": row[4], "stop_name": row[5], "stop_longitude": row[6], "stop_latitude": row[7], "lines": lines })

    return simplejson.dumps(clusters.values(), sort_keys=True, indent=4)

def getTimeTable(stop_id, route_id, operationaldate):
    offset = int(time.mktime(operationaldate.timetuple()))

    sql = "select departure_time_seconds from gtfs_trips as t, gtfs_stop_times as st, gtfs_calendar_dates as cd where t.trip_id = st.trip_id and t.service_id = cd.service_id and route_id = %s and stop_id = %s and cd.date = %s and departure_time_seconds is not null order by departure_time;"
    print operationaldate
    param = [route_id, stop_id, operationaldate]

    conn = psycopg2.connect(pg_connect)
    cur = conn.cursor()
    cur.execute(sql, param)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    return simplejson.dumps([{'departure_planned': offset + row[0]} for row in rows], sort_keys=True, indent=4)
    
def getDepartures(stop_id, route_id, maxcount, operationaldate, seconds):
    offset = int(time.mktime(operationaldate.timetuple()))

    sql = "select gr.agency_id, rd.headsign, gr.route_short_name, description from gtfs_routes as gr, route_destination as rd, route_destination_stops as rds, gtfs_route_types where gr.route_id = rd.route_id and rd.routedest_id = rds.routedest_id and gr.route_type = gtfs_route_types.route_type and rd.route_id = %s and rds.stop_id = %s;"
    
    param = [route_id, stop_id]

    conn = psycopg2.connect(pg_connect)
    cur = conn.cursor()
    cur.execute(sql, param)
    routes = cur.fetchall()
    if len(routes) > 0:
        route = routes[0]
	
	result = {"line": {"line_id": route_id, "line_company": route[0], "line_destination": route[1], "line_number": route[2], "line_type": route[3]}, "departures": []}
        sql = "select departure_time_seconds, st.trip_id||'_'||st.stop_sequence from gtfs_trips as t, gtfs_stop_times as st, gtfs_calendar_dates as cd where t.trip_id = st.trip_id and t.service_id = cd.service_id and route_id = %s and stop_id = %s and cd.date = %s and departure_time_seconds >= %s order by departure_time limit %s;"
        param = [route_id, stop_id, operationaldate, seconds, maxcount]
        print param

    	conn = psycopg2.connect(pg_connect)
        cur = conn.cursor()
        cur.execute(sql, param)
        rows = cur.fetchall()
	cur.close()
    	conn.close()

	actuals = {}
#	if len(rows) > 0:
#		context = zmq.Context()
#		client_annotate = context.socket(zmq.REQ)
#		client_annotate.connect(ZMQ_PUBSUB_KV8_ANNOTATE)
#		client_annotate.send_json([row[1] for row in rows])
#		actuals = client_annotate.recv_json()

        for row in rows:
            if row[1] not in actuals:
                actual = 0
            else:
                actual = offset + actuals[row[1]]

            result["departures"].append({"departure_id": row[1], "departure_planned": offset + int(row[0]), "departure_actual": actual})

        return simplejson.dumps(result, sort_keys=True, indent=4)
    else:
	cur.close()
        conn.close()
    return '[]'

def getLineStops(stop_id, route_id, operationaldate, seconds):
    offset = int(time.mktime(operationaldate.timetuple()))

    # sql = "select s.stop_id, s.stop_name, s.stop_lon, s.stop_lat, st.departure_time_seconds, st.trip_id||'_'||st.stop_sequence from gtfs_stop_times as st, gtfs_stops as s, gtfs_stop_times as st2 where st.stop_id = s.stop_id and st2.stop_id = %s and st.stop_sequence >= st2.stop_sequence and st.trip_id = st2.trip_id and st.trip_id = (select st.trip_id from gtfs_trips as t, gtfs_stop_times as st, gtfs_calendar_dates as cd, gtfs_stops as s where t.trip_id = st.trip_id and t.service_id = cd.service_id and st.stop_id = s.stop_id and route_id = %s and st.stop_id = %s and cd.date = %s and departure_time_seconds is not null order by abs(departure_time_seconds - %s) limit 1) order by st.stop_sequence;"
    sql = "select s.stop_id, s.stop_name, s.stop_lon, s.stop_lat, st.departure_time_seconds, st.trip_id||'_'||st.stop_sequence from gtfs_stop_times as st, gtfs_stops as s, gtfs_stop_times as st2 where st.stop_id = s.stop_id and st2.stop_id = %s and st.stop_sequence >= st2.stop_sequence and st.trip_id = st2.trip_id and st.trip_id = (select st.trip_id from gtfs_trips as t, gtfs_stop_times as st, gtfs_calendar_dates as cd, gtfs_stops as s where t.trip_id = st.trip_id and t.service_id = cd.service_id and st.stop_id = s.stop_id and route_id = %s and st.stop_id = %s and cd.date = %s and st.departure_time_seconds is not null order by abs(departure_time_seconds - %s) limit 1) and st.departure_time_seconds is not null and st2.departure_time_seconds is not null order by st.stop_sequence;"

    param = [stop_id, route_id, stop_id, operationaldate, seconds]

    conn = psycopg2.connect(pg_connect)
    cur = conn.cursor()
    cur.execute(sql, param)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    result = []

    actuals = {}
#    if len(rows) > 0:
#        context = zmq.Context()
#        client_annotate = context.socket(zmq.REQ)
#        client_annotate.connect(ZMQ_PUBSUB_KV8_ANNOTATE)
#        client_annotate.send_json([row[5] for row in rows])
#        actuals = client_annotate.recv_json()

    for row in rows:
        if row[5] not in actuals:
            actual = 0
        else:
            actual = offset + actuals[row[1]]
            
        result.append({"stop_id": row[0], "stop_name": row[1], "stop_longitude": row[2], "stop_latitude": row[3], "passing_time_planned": offset + row[4], "passing_time_actual": actual})

    return simplejson.dumps(result, sort_keys=True, indent=4)

def notfound(start_response):
    start_response('404 File Not Found', COMMON_HEADERS + [('Content-length', '2')])
    yield '[]'

def GVB(environ, start_response):
    url = environ['PATH_INFO'][1:]
    if len(url) > 0 and url[-1] == '/':
        url = url[:-1]
        
    arguments = url.split('/')

    if arguments[0] == 'getClusters' and (len(arguments) == 1 or len(arguments) == 2 or len(arguments) == 4):
        if len(arguments) == 4:
            reply = getClusters(float(arguments[1]), float(arguments[2]), float(arguments[3]))
        if len(arguments) == 2:
            reply = getClusters(name=arguments[1])
        elif len(arguments) == 1:
            reply = getClusters()

    elif arguments[0] == 'getDepartures' and (len(arguments) == 4):
        operationaldate, seconds = timestampSplit(int(time.time()))        
        reply = getDepartures(arguments[1], arguments[2], arguments[3], operationaldate, seconds)

    elif arguments[0] == 'getTimeTable' and (len(arguments) == 4):
        operationaldate, seconds = timestampSplit(int(arguments[3]))
        reply = getTimeTable(arguments[1], arguments[2], operationaldate)

    elif arguments[0] == 'getLineStops' and (len(arguments) == 4):
        operationaldate, seconds = timestampSplit(int(arguments[3]))
        reply = getLineStops(arguments[1], arguments[2], operationaldate, seconds)

    else:
        return notfound(start_response)

    #context = zmq.Context()
    #client = context.socket(zmq.REQ)
    #client.connect(ZMQ_KV78DEMO)
    #client.send(url)
    #reply = client.recv()
    #if len(reply) == 0:
    #    return notfound(start_response)
        
    start_response('200 OK', COMMON_HEADERS + [('Content-length', str(len(reply)))])
    return reply

uwsgi.applications = {'': GVB}
