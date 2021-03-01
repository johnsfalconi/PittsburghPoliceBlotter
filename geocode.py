import geocoder
import datetime
import pandas as pd
from sqlalchemy import create_engine
from datetime import date, timedelta
import calendar
import sys
import json

#### TO-DO: ####
# NEEDS TO BE SET UP IN TASK SCHEDULER

engine = create_engine('postgresql://postgres:postgres@localhost:5432/blotter')
records = 0
updated = 0

if len(sys.argv) == 1:
    yesterday = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    run_source = 'Automatic'
elif len(sys.argv) > 1 and len(sys.argv) <= 2:
    yesterday = sys.argv[1]
    run_source = 'Automatic'
elif len(sys.argv) > 2:
    yesterday = sys.argv[1]
    run_source = sys.argv[2]

geocode_table_check = list(engine.execute(
    "select count(*) from ppd_blotter_latlon where arrest_date = '" + yesterday + "'"))

if geocode_table_check[0][0] == 0:
    arrest_locations = list(engine.execute(
        "SELECT DISTINCT ccr, REPLACE(address, ' block', ''), neighborhood, arrest_date FROM pittsburghpd_blotter where arrest_date = '" + yesterday + "'"))

    for i in arrest_locations:
        g = geocoder.osm(i[1] + ', Pittsburgh')
        records += 1
        if g.json is not None:
            engine.execute("INSERT INTO ppd_blotter_latlon (ccr, arrest_date, lat, lon) VALUES ('" +
                           str(i[0]) + "', '" + str(i[3]) + "', " + str(g.json['lat']) + ", " + str(g.json['lng']) + "); COMMIT;")
            updated += 1
        else:
            engine.execute("INSERT INTO ppd_blotter_latlon_manualreview (ccr, address, neighborhood, arrest_date) VALUES ('" +
                           str(i[0]) + "', '" + str(i[1]) + "', '" + str(i[2]) + "', '" + str(i[3]) + "'); COMMIT;")

    engine.execute("SELECT log_activity('Pittsburgh Police Blotter Geocode', '" + run_source + "', '" +
                   yesterday + "', 'Success', '" + str(updated) + " records out of " + str(records) + " were geocoded'); commit;")
else:
    manual_records_outstanding = list(engine.execute(
        "select count(*) from ppd_blotter_latlon_manualreview where arrest_date = '" + yesterday + "'"))

    if manual_records_outstanding[0][0] > 0:
        engine.execute("SELECT log_activity('Pittsburgh Police Blotter Geocode', '" + run_source + "', '" +
                       yesterday + "', 'Failure', 'Records already geocoded. " + str(manual_records_outstanding[0][0]) + " records still need to be manually reviewed.'); commit;")
    else:
        engine.execute("SELECT log_activity('Pittsburgh Police Blotter Geocode', '" + run_source + "', '" +
                       yesterday + "', 'Failure', 'Records already geocoded.'); commit;")
