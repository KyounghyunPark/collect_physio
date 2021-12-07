import os
import sys
import psycopg2 as pg
import pandas as pd
import pandas.io.sql as psql

## Database
host = '211.41.186.138'
db='etri'
user='etri_admin'
passwd='Neighbor_2020'
port='9432'

## print
def _log_sql_(msg):
    print('log> sql = %s' % msg)

def _log_df_(df):
    print('log> results size : %s\n' % str(df.shape))


## utility functions
def save_file(file_name, df):
    if os.path.isfile(file_name):
        df.to_csv(file_name, mode='a', header=False)
    else:
        df.to_csv(file_name, mode='w', header=True)


def make_folder(dir):
    try:
        if not os.path.exists(dir):
            os.makedirs(dir)
    except OSError:
        print('Error: Creating directory. ' + dir)


def get_user_name(user_id):
    sql = "select user_nm from wmind2.user_info where user_id='" + user_id + "'"
    _log_sql_(sql)

    with pg.connect(host=host, dbname=db, user=user, password=passwd, port=port) as conn:
        df = psql.read_sql(sql, conn)

    return df.iloc[0][0]


## daily users
def get_daily_users(required_date):
    sql = "select distinct user_id from wmind2.expt_creat_hist where created::date = '" + required_date + "'"

    _log_sql_(sql)

    with pg.connect(host=host, dbname=db, user=user, password=passwd, port=port) as conn:
        df = psql.read_sql(sql, conn)

    _log_df_(df)

    return df

## daily survey

def get_survey_daily_data(sql):
    _log_sql_(sql)

    with pg.connect(host=host, dbname=db, user=user, password=passwd, port=port) as conn:
        df = psql.read_sql(sql, conn)

    _log_df_(df)

    return df

def is_valid(user_id, required_date):
    sql = "select distinct sort_ordr from wmind2.expt_creat_hist  where user_id='" + user_id + "' and created::date = '" + required_date + "'"
    
    _log_sql_(sql)

    with pg.connect(host=host, dbname=db, user=user, password=passwd, port=port) as conn:
        df = psql.read_sql(sql, conn)

    _log_df_(df)

    if (len(df) == 8):
        return True
    else:
        return False

# get survey history number
def proc_sn_number(user_id, required_date):
    sql_sn_get = "select * from wmind2.expt_creat_hist where user_id='" + user_id + "' and created::date = '" + required_date + "'"
    df = get_survey_daily_data(sql_sn_get)
    df = df.iloc[0:8]  #8 row 1명의 참가자는 모두 8번의 설문 진행하므로 8개 번호
    df.set_index('sort_ordr', inplace = True)
    return df

# 1. before : 실험하기 전에 사전 설문
def proc_bef_survey(user_id, required_date):
     sql_before = "select * from wmind2.qustnr_before_expt where user_id='" + user_id + "' and created_date::date = '" + required_date + "'"
     df = get_survey_daily_data(sql_before)
     df.set_index('qustnr_sn', inplace = True) # key 지정안하면 loc처리시 에러 발생
     return df

# 2. after_stress : 스트레스 실험후 설문
def proc_stress_survey(user_id, required_date):
     sql_after_stress = "select * from wmind2.qustnr_after_stress where user_id='" + user_id + "' and created_date::date = '" + required_date + "'"
     df = get_survey_daily_data(sql_after_stress)
     df.set_index('qustnr_sn', inplace = True) # key 지정안하면 loc처리시 에러 발생
     return df

# 3. after_solution :웰빙 솔루션 실험후 설문
def proc_relax_survey(user_id, required_date):
     sql_after_solution = "select * from wmind2.qustnr_after_solution where user_id='" + user_id + "' and created_date::date = '" + required_date + "'"
     df = get_survey_daily_data(sql_after_solution)
     df.set_index('qustnr_sn', inplace = True) # key 지정안하면 loc처리시 에러 발생
     return df

# 4. final, after :모든 실험 종료후 설문
def proc_final_survey(user_id, required_date):
     sql_final = "select * from wmind2.qustnr_after_expt where user_id='" + user_id + "' and created_date::date = '" + required_date + "'"
     df = get_survey_daily_data(sql_final)
     df.set_index('qustnr_sn', inplace = True) # key 지정안하면 loc처리시 에러 발생
     return df


def get_survey_data(user_id, required_date):
    folder = './data/' + required_date + '/'
    df_number = proc_sn_number(user_id, required_date)
    df_before = proc_bef_survey(user_id, required_date)
    df_stress = proc_stress_survey(user_id, required_date)
    df_solution = proc_relax_survey(user_id, required_date)
    df_final = proc_final_survey(user_id, required_date)

    for row in range(len(df_number)):
        print(row)
        sn = df_number.iloc[row, 2]
        print(sn)

        if row == 0:  # before experiment
            bef = df_before.loc[[sn], :]
            print(bef)
        elif row == 1:  # stress 1
            s1 = df_stress.loc[[sn], :]
            print(s1)
        elif row == 2:  # relax 1
            r1 = df_solution.loc[[sn], :]
            print(r1)
        elif row == 3:
            s2 = df_stress.loc[[sn], :]
            print(s2)
        elif row == 4:  # relax 2
            r2 = df_solution.loc[[sn], :]
            print(r2)
        elif row == 5:  # stress 3
            s3 = df_stress.loc[[sn], :]
            print(s3)
        elif row == 6:  # relax 3
            r3 = df_solution.loc[[sn], :]
            print(r3)
        elif row == 7:  # after
            aft = df_final.loc[[sn], :]
            print(aft)

    # end of for
    folder2 = './data/'
    # bef.rename(columns={'qustnr_sn':'bef_qustnr_sn','created_date':'bef_c_date', 'created_day':'bef_c_day', 'created_time':'bef_c_time'}, inplace = True)
    bef.rename(columns={'qustnr_sn': 'bef_qustnr_sn'}, inplace=True)
    bef.drop(['created_date', 'created_day', 'created_time'], axis=1, inplace=True)

    # s1.rename(columns={'created_date':'s1_c_date', 'created_day':'s1_c_day', 'created_time':'s1_c_time'}, inplace = True)
    s1.drop(['created_date', 'created_day', 'created_time', 'data_sn'], axis=1, inplace=True)
    s1.rename(columns={'qustnr_sn': 'S1_qustnr_sn', 'answer1_1': 'S1_1_1', 'answer2_1': 'S1_2_1', 'answer2_2': 'S1_2_2',
                       'answer2_3': 'S1_2_3',
                       'answer2_4': 'S1_2_4', 'answer2_5': 'S1_2_5', 'answer2_6': 'S1_2_6', 'answer2_7': 'S1_2_7',
                       'answer2_8': 'S1_2_8', 'answer2_9': 'S1_2_9'}, inplace=True)

    s2.drop(['created_date', 'created_day', 'created_time', 'data_sn'], axis=1, inplace=True)
    # s2.rename(columns={'created_date':'s2_c_date', 'created_day':'s2_c_day', 'created_time':'s2_c_time'}, inplace = True)
    s2.rename(columns={'qustnr_sn': 'S2_qustnr_sn', 'answer1_1': 'S2_1_1', 'answer2_1': 'S2_2_1', 'answer2_2': 'S2_2_2',
                       'answer2_3': 'S2_2_3',
                       'answer2_4': 'S2_2_4', 'answer2_5': 'S2_2_5', 'answer2_6': 'S2_2_6', 'answer2_7': 'S2_2_7',
                       'answer2_8': 'S2_2_8', 'answer2_9': 'S2_2_9'}, inplace=True)

    s3.drop(['created_date', 'created_day', 'created_time', 'data_sn'], axis=1, inplace=True)
    # s3.rename(columns={'created_date':'s3_c_date', 'created_day':'s3_c_day', 'created_time':'s3_c_time'}, inplace = True)
    s3.rename(columns={'qustnr_sn': 'S3_qustnr_sn', 'answer1_1': 'S3_1_1', 'answer2_1': 'S3_2_1', 'answer2_2': 'S3_2_2',
                       'answer2_3': 'S3_2_3',
                       'answer2_4': 'S3_2_4', 'answer2_5': 'S3_2_5', 'answer2_6': 'S3_2_6', 'answer2_7': 'S3_2_7',
                       'answer2_8': 'S3_2_8', 'answer2_9': 'S3_2_9'}, inplace=True)

    r1.drop(['created_date', 'created_day', 'created_time', 'data_sn'], axis=1, inplace=True)
    # r1.rename(columns={'created_date':'r1_c_date', 'created_day':'r1_c_day', 'created_time':'r1_c_time'}, inplace = True)
    r1.rename(columns={'qustnr_sn': 'R1_qustnr_sn', 'answer1_1': 'R1_1_1', 'answer2_1': 'R1_2_1', 'answer3_1': 'R1_3_1',
                       'answer3_2': 'R1_3_2',
                       'answer3_3': 'R1_3_3', 'answer3_4': 'R1_3_4', 'answer3_5': 'R1_3_5', 'answer3_6': 'R1_3_6',
                       'answer3_7': 'R1_3_7', 'answer3_8': 'R1_3_8', 'answer3_9': 'R1_3_9'}, inplace=True)

    r2.drop(['created_date', 'created_day', 'created_time', 'data_sn'], axis=1, inplace=True)
    # r2.rename(columns={'created_date':'r2_c_date', 'created_day':'r2_c_day', 'created_time':'r2_c_time'}, inplace = True)
    r2.rename(columns={'qustnr_sn': 'R2_qustnr_sn', 'answer1_1': 'R2_1_1', 'answer2_1': 'R2_2_1', 'answer3_1': 'R2_3_1',
                       'answer3_2': 'R2_3_2',
                       'answer3_3': 'R2_3_3', 'answer3_4': 'R2_3_4', 'answer3_5': 'R2_3_5', 'answer3_6': 'R2_3_6',
                       'answer3_7': 'R2_3_7', 'answer3_8': 'R2_3_8', 'answer3_9': 'R2_3_9'}, inplace=True)

    r3.drop(['created_date', 'created_day', 'created_time', 'data_sn'], axis=1, inplace=True)
    # r3.rename(columns={'created_date':'r3_c_date', 'created_day':'r3_c_day', 'created_time':'r3_c_time'}, inplace = True)
    r3.rename(columns={'qustnr_sn': 'R3_qustnr_sn', 'answer1_1': 'R3_1_1', 'answer2_1': 'R3_2_1', 'answer3_1': 'R3_3_1',
                       'answer3_2': 'R3_3_2',
                       'answer3_3': 'R3_3_3', 'answer3_4': 'R3_3_4', 'answer3_5': 'R3_3_5', 'answer3_6': 'R3_3_6',
                       'answer3_7': 'R3_3_7', 'answer3_8': 'R3_3_8', 'answer3_9': 'R3_3_9'}, inplace=True)

    aft.drop(['created_date', 'created_day', 'created_time'], axis=1, inplace=True)
    # aft.rename(columns={'created_date':'aft_c_date', 'created_day':'aft_c_day', 'created_time':'aft_c_time'}, inplace = True)
    aft.rename(columns={'qustnr_sn': 'final_qustnr_sn', 'answer1_1': 'final_1_1', 'answer1_2': 'final_1_2',
                        'answer1_3': 'final_1_3'}, inplace=True)

    # df_tot = pd.DataFrame[s1,r1,s2,r2]
    # df_tot = pd.concat([bef,s1], axis=1, join='inner', ignore_index=True)

    file_name = folder2 + "survey_before" + ".csv"
    save_file(file_name, bef)

    df_s1_r1 = pd.merge(s1, r1, on='user_id')
    file_name = folder2 + "survey_S1_R1" + ".csv"
    save_file(file_name, df_s1_r1)

    df_s2_r2 = pd.merge(s2, r2, on='user_id')
    file_name = folder2 + "survey_S2_R2" + ".csv"
    save_file(file_name, df_s2_r2)

    df_s3_r3 = pd.merge(s3, r3, on='user_id')
    file_name = folder2 + "survey_S3_R3" + ".csv"
    save_file(file_name, df_s3_r3)

    imsi1 = pd.merge(bef, df_s1_r1, on='user_id')
    imsi2 = pd.merge(imsi1, df_s2_r2, on='user_id')
    imsi3 = pd.merge(imsi2, df_s3_r3, on='user_id')
    df_tot = pd.merge(imsi3, aft, on='user_id')

    file_name = folder2 + "survey_total" + ".csv"
    save_file(file_name, df_tot)

def main():
    #---------------------
    # required_date = "2021-10-02"
    #---------------------
    #required_date = input("날짜 입력 (YYYY-MM-DD) : ")
    required_date = "2021-10-05"

    users = get_daily_users(required_date)          # user_id collection
    for index, row in users.iterrows():  # for each user_id, search data
        # get daily user data
        user_id = str(row['user_id'])

        # Survey Data
        if is_valid(user_id, required_date):
            get_survey_data(user_id, required_date)

    print("\n")
    print("Survey backup completed !! Thank you ^^~")

if __name__ == "__main__":
	main()