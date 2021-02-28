import datetime
import pandas as pd
from sqlalchemy import create_engine
from datetime import date, timedelta
import calendar
import sys

#### TO-DO: ###
# SET UP REPORTS TO CHECK THE STATUS
# CREATE POWER BI FOR VISUALIZATION AND PUBLISHING
# SET UP EXPLANATION ON GITHUB

engine = create_engine('postgresql://postgres:postgres@localhost:5432/blotter')

if len(sys.argv) == 1:
    yesterday = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    run_source = 'Automatic'
elif len(sys.argv) > 1 and len(sys.argv) <= 2:
    yesterday = sys.argv[1]
    run_source = 'Manual'
elif len(sys.argv) > 2:
    yesterday = sys.argv[1]
    run_source = sys.argv[2]


# this block of code is used to check if the necessary tables are set up for the data pull
##########################################################################################
blotter_table = list(engine.execute(
    "SELECT table_status('pittsburghpd_blotter')"))[0][0]
import_table = list(engine.execute(
    "SELECT table_status('pittsburghpd_blotter_import')"))[0][0]
activity_table = list(engine.execute(
    "SELECT table_status('blotter_system_activity')"))[0][0]

# if anything is missing then it will either end the program
# (or in the case of the import table, simply drop the table)
if blotter_table == 0:
    print('Blotter table does not exist.')
    quit()
elif import_table == 1:
    print('Import table currently exists and will be dropped.')
    engine.execute('DROP TABLE pittsburghpd_blotter_import; commit;')
elif activity_table == 0:
    print('Activity table does not exist.')
    quit()
# -----------------------------------------------------------------------------------------


# here we check to see if the manually-passed date parameter is in the correct format
##########################################################################################
try:
    day_in_question = datetime.datetime.strptime(yesterday, '%Y-%m-%d')
# if not, we will log the attempt, remind the user of the format, and quit out the script
except:
    print('Invalid date format. Date must be in the YYYY-MM-DD format.')
    engine.execute("SELECT log_activity('Pittsburgh Police Blotter Pull', 'Manual', '" +
                   yesterday + "', 'Failure', 'Invalid date format. Date must be in the YYYY-MM-DD format.'); commit;")
    quit()
# -----------------------------------------------------------------------------------------


# Finally we will attempt to check for the date to be in a specific range.  Otherwise it will not be able to pull the correct data.
###################################################################################################################################
if day_in_question.date() >= date.today() or day_in_question.date() < date.today() - timedelta(days=7):
    print('Invalid date. Date must be be within 7 days of the current date: ' + str(date.today()))
    engine.execute("SELECT log_activity('Pittsburgh Police Blotter Pull', 'Manual', '" +
                   yesterday + "', 'Failure', 'Invalid date. Date must be be within 7 days of the current date.'); commit;")
# ----------------------------------------------------------------------------------------------------------------------------------
else:
    data_check = engine.execute(
        "select date_check(\'" + str(day_in_question) + "\');")
    # if there are 0 records in the database for the specific date, we haven't pulled it yet
    if list(data_check)[0][0] == 0:
        df = pd.read_csv('http://apps.pittsburghpa.gov/police/arrest_blotter/arrest_blotter_' +
                         calendar.day_name[day_in_question.weekday()] + '.csv', keep_default_na=False)
        df.columns = df.columns.values
        df.reset_index(drop=True, inplace=True)
        df.to_sql('pittsburghpd_blotter_import', engine,
                  if_exists='append', index=False)
        engine.execute("SELECT transfer(); SELECT log_activity('Pittsburgh Police Blotter Pull', 'Manual', '" +
                       yesterday + "', 'Success', null); commit;")
        print('Records pulled successfully for ' + yesterday)
    # if there are more than 0 records in the database, we can inform the user that we pulled it already
    else:
        print('Records already pulled for ' + yesterday)
        engine.execute("SELECT log_activity('Pittsburgh Police Blotter Pull', '" + run_source + "', '" +
                       yesterday + "', 'Failure', 'Records already pulled for " + yesterday + "'); commit;")
