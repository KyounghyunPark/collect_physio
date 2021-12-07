import sys
import psycopg2 as pg
import pandas.io.sql as psql

## Database
host = '211.41.186.138'
db='etri'
user='etri_admin'
passwd='Neighbor_2020'
port='9432'

'''
## check Parameters
if len(sys.argv) != 2:
    print('Error: Insufficient Arguements')
    print('\t Usage: python run.py <Date>')
    print('\t        python run.py 2021-09-12')
    sys.exit()
'''

## check Parameters
if len(sys.argv) != 2:
    print('Error: Invalid Command Line!')
    print('  Usage: run.sh <Date>')
    print('     ex) run.sh 2021-09-28')
    sys.exit()    


## log functions
def _log_sql_(msg):
    print('log> sql = %s' % msg)

def _log_df_(df):
    print('log> results size : %s\n' % str(df.shape))


## utility functions
def save_file(file_name, df):
    df.to_csv(file_name, mode='w')

def get_user_name(user_id):
    sql = "select user_nm from wmind2.user_info where user_id='" + user_id + "'"
    _log_sql_(sql)

    
    with pg.connect(host=host, dbname=db, user=user, password=passwd, port=port) as conn:
        df = psql.read_sql(sql, conn)    
    
    return  df.iloc[0][0]


## daily data functions
def get_daily_users(required_date):
    sql = "select distinct user_id from wmind2.expt_creat_hist where created::date = '" + required_date + "'"
    _log_sql_(sql)

    with pg.connect(host=host, dbname=db, user=user, password=passwd, port=port) as conn:
        df = psql.read_sql(sql, conn)

    _log_df_(df)

    return df

def get_daily_user_records(user_id, required_date):
    sql = "select (select user_nm from wmind2.user_info where user_id='" + user_id + "') as user_name, data_sn, data_type, start_time from wmind2.bio_data_creat_hist where user_id='" + user_id + "' and start_time::date = '" + required_date + "'"
    _log_sql_(sql)

    with pg.connect(host=host, dbname=db, user=user, password=passwd, port=port) as conn:
        df = psql.read_sql(sql, conn)

    _log_df_(df)
    
    return df


def get_device_daily_data(sql):
    _log_sql_(sql)

    with pg.connect(host=host, dbname=db, user=user, password=passwd, port=port) as conn:
        df = psql.read_sql(sql, conn)
    
    _log_df_(df)

    return df

def get_env_data(user_id, required_date):
    sql = "select * from wmind2.env_expt_data where user_id='" + user_id + "' and created::date = '" + required_date + "'"
    _log_sql_(sql)

    with pg.connect(host=host, dbname=db, user=user, password=passwd, port=port) as conn:
        df = psql.read_sql(sql, conn)
    
    _log_df_(df)

    file_name = user_name + "_env_" + required_date + ".csv"
    save_file(file_name, df)


def get_galaxy_data(user_id, required_date):
    sql_hr = "select * from wmind2.galaxy_hr_data where user_id='" + user_id + "' and created::date = '" + required_date + "'"
    sql_rri = "select * from wmind2.galaxy_rri_data where user_id='" + user_id + "' and created::date = '" + required_date + "'"
    sql_pedometer = "select * from wmind2.galaxy_pedometer_data where user_id='" + user_id + "' and created::date = '" + required_date + "'"

    user_name = get_user_name(user_id)

    # hr
    df = get_device_daily_data(sql_hr)
    file_name = user_name + "_galaxy_hr_" + required_date + ".csv"
    save_file(file_name, df)

    # rri
    df = get_device_daily_data(sql_rri)
    file_name = user_name + "_galaxy_rri_" + required_date + ".csv"
    save_file(file_name, df)
    
    # pedometer
    df = get_device_daily_data(sql_rri)
    file_name = user_name + "_galaxy_rri_" + required_date + ".csv"
    save_file(file_name, df)



def get_e4_data(user_id, required_date):
    sql_temp = "select * from wmind2.e4_temp_data where user_id='" + user_id + "' and created::date = '" + required_date + "'"
    sql_ibi = "select * from wmind2.e4_ibi_data where user_id='" + user_id + "' and created::date = '" + required_date + "'"
    sql_acc = "select * from wmind2.e4_acc_data where user_id='" + user_id + "' and created::date = '" + required_date + "'"
    sql_gsr = "select * from wmind2.e4_gsr_data where user_id='" + user_id + "' and created::date = '" + required_date + "'"
    sql_bvp = "select * from wmind2.e4_bvp_data where user_id='" + user_id + "' and created::date = '" + required_date + "'"

    user_name = get_user_name(user_id)

    # temperature
    df = get_device_daily_data(sql_temp)
    file_name = user_name + "_e4_temp_" + required_date + ".csv"
    save_file(file_name, df)

    # ibi
    df = get_device_daily_data(sql_ibi)
    file_name = user_name + "_e4_ibi_" + required_date + ".csv"
    save_file(file_name, df)

    # acc
    df = get_device_daily_data(sql_acc)
    file_name = user_name + "_e4_acc_" + required_date + ".csv"
    save_file(file_name, df)

    # gsr
    df = get_device_daily_data(sql_gsr)
    file_name = user_name + "_e4_gsr_" + required_date + ".csv"
    save_file(file_name, df)

    # bvp
    df = get_device_daily_data(sql_bvp)
    file_name = user_name + "_e4_bvp_" + required_date + ".csv"
    save_file(file_name, df)



## main
required_date = sys.argv[1]

users = get_daily_users(required_date)          # user_id collection
for index, row in users.iterrows():             # for each user_id, search data
    # get daily user data
    user_id = str(row['user_id'])
    df = get_daily_user_records(user_id, required_date)

    user_name = get_user_name(user_id)
    file_name = user_name + "_" + required_date + ".csv"
    save_file(file_name, df)                       # save daily user data

    # E4 Devcie Data
    get_e4_data(user_id, required_date)

    # Galaxy Watch Data
    get_galaxy_data(user_id, required_date)

    # Environmental Data
    get_env_data(user_id, required_date)


