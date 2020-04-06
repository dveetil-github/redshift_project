import configparser
import psycopg2
from sqlalchemy import create_engine
import pandas as pd
from sql_queries import copy_table_queries, insert_table_queries, time_table_insert, select_staging_events, DB_USER, \
    DB_PASSWORD, HOST, \
    DB_PORT, DB_NAME


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        print("query---->" + query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def loadDataToAnalyticTables(cur, conn):
    print("Inside loadDataToAnalyticTables")
    conn_string = "postgresql://{}:{}@{}:{}/{}".format(DB_USER, DB_PASSWORD, HOST, DB_PORT, DB_NAME)
    engine = create_engine(conn_string)
    df = pd.read_sql_query(select_staging_events, engine)
    # print("count of all events=" + df.count())
    filterNextSong = df['page'] == 'NextSong'
    df1 = df[filterNextSong]
    # convert timestamp column to datetime
    t = pd.to_datetime(df1['ts'], unit='ms')
    # insert time data records
    time_data = pd.DataFrame()
    time_data['start_time'] = t.dt.strftime("%d-%b-%Y %H:%M:%S.%f")
    time_data['hour'] = t.dt.hour
    time_data['day'] = t.dt.day
    time_data['week'] = t.dt.week
    time_data['month'] = t.dt.month
    time_data['year'] = t.dt.year
    time_data['weekday'] = t.dt.weekday

    # column_labels =
    time_df = time_data

    for i, row in time_df.iterrows():
        print("counter=" + str(i))
        print(list(row))
        cur.execute(time_table_insert, list(row))
    df2 = pd.read_sql_query('select * from time limit 100', engine)
    print(df2.count)


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print("loading staging tables")
    # load_staging_tables(cur, conn)
    print("completed loading staging tables")
    # insert_tables(cur, conn)
    print("loading Analytic  tables")
    loadDataToAnalyticTables(cur, conn)

    print("complete loading to Analytic  tables")

    conn.close()


if __name__ == "__main__":
    main()
