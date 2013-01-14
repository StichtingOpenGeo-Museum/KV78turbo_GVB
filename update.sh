#!/bin/sh
FILENAME=`date "+%F"`
cd /tmp
wget -O gtfs-kv1gvb-latest.zip http://gtfs.ovapi.nl/gvb/gtfs-kv1gvb-latest.zip
rm -r gvb
mkdir gvb
cd gvb
unzip ../gtfs-kv1gvb-latest.zip
cd /usr/src/gtfs_sql_importer/src
python import_gtfs_to_sql.py /tmp/gvb > /tmp/gvb-$FILENAME.sql
createdb  gtfs-gvb-$FILENAME
cat gtfs_tables.sql | psql -d gtfs-gvb-$FILENAME
cat /tmp/gvb-$FILENAME.sql | psql -d gtfs-gvb-$FILENAME
echo "create extension postgis;" | psql -d gtfs-gvb-$FILENAME
cat gtfs_tables_makeindexes.sql | psql -d gtfs-gvb-$FILENAME
cat gtfs_tables_makespatial.sql | psql -d gtfs-gvb-$FILENAME
cat /home/gvbapi/KV78turbo_GVB/gtfs_extra.sql | psql -d gtfs-gvb-$FILENAME
echo "grant select on geography_columns to gvbapi; GRANT SELECT ON ALL TABLES IN SCHEMA public TO gvbapi; GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO gvbapi;" | psql -d gtfs-gvb-$FILENAME
echo "pg_connect = \"dbname='gtfs-gvb-$FILENAME'\"" > /home/gvbapi/KV78turbo_GVB/dbconnect.py
