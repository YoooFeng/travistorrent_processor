# -*- coding:utf-8-*-

from __future__ import division
import MySQLdb as mdb
import pandas as pd
import time
from db_setting import *
from sqlalchemy import create_engine


# 连接数据库
def read_db_to_df(sql, contain, info='', verbose=True):
    db = mdb.connect(host=R_HOST, port=R_PORT, user=R_USER, passwd=R_PASSWORD, db=R_DATABASE, charset=R_CHARSET)
    cursor = db.cursor()
    start = time.time()
    try:
        # 执行sql语句
        cursor.execute(sql)
        db_data = cursor.fetchall()
        result = pd.DataFrame(list(db_data), columns=contain)
    except AssertionError:
        raise AssertionError(info, '构造DataFrame时出错')
    except:
        raise IOError(info, '读取数据库时出错')
    # 关闭数据库连接
    db.close()
    if verbose:
        print(info, '数据读取完毕，用时',
              int((time.time() - start) // 60), '分',
              int((time.time() - start) % 60), '秒')
    return result


def get_project_from_db(verbose=True):
    db = mdb.connect(host=R_HOST, port=R_PORT, user=R_USER, passwd=R_PASSWORD, db=R_DATABASE, charset=R_CHARSET)
    cursor = db.cursor()
    try:
        # 执行sql语句
        sql = 'SELECT distinct (gh_project_name) FROM travistorrent_8_2_2017'
        cursor.execute(sql)
        project_list = cursor.fetchall()
        if len(project_list) == 0:
            raise Exception
        if verbose:
            print('成功从travistorrent_8_2_2017读取项目名列表')
    except:
        if verbose:
            print('从travistorrent_8_2_2017读取项目名列表出错')
    # 关闭数据库连接
    db.close()
    return project_list


def calculate_project_related(project_name):
    _sql = 'SELECT distinct(tr_build_id), ' \
           'gh_project_name, ' \
           'gh_is_pr, ' \
           'gh_lang, ' \
           'git_branch, ' \
           'gh_num_commits_in_push, ' \
           'gh_team_size, ' \
           'git_diff_src_churn, ' \
           'gh_build_started_at, ' \
           'tr_duration, ' \
           'tr_status ' \
           'FROM travistorrent_8_2_2017 ' \
           'WHERE gh_project_name = \'{project}\' ' \
           'AND (tr_status =  \'passed\' OR tr_status = \'failed\')' \
           'ORDER BY tr_build_id'.format(project=project_name)
    _contain = ['build_id', 'project_name', 'is_pr', 'language', 'branch', 'num_commits', 'team_size', 'modified_lines', 'build_started_at', 'duration', 'status']
    result = read_db_to_df(_sql, _contain)
    # print '==========================tr_status===================================/n/n'
    # print result

    # 获取result的复制
    # result = result.copy()

    # 为df增加三列新的数据
    # 上一次构建是否成功
    result['last_build'] = 0
    # 项目历史构建成功率
    result['project_history'] = 0
    # 项目近期构建成功率 - 最近5次构建，
    result['project_recent'] = 0

    current_total = 0
    current_passed = 0
    current_failed = 0
    for i in result.index:

        current_total += 1
        # 先计算上一次的构建赋值, 1 - 成功/ 0 - 失败
        if i == 0:
            # 默认上一次是成功的
            result.loc[i, 'last_build'] = 1
        else:
            last_build = result.loc[i - 1, 'status']
            if last_build == 'passed':
                result.loc[i, 'last_build'] = 1
            else:
                result.loc[i, 'last_build'] = 0

        # 在计算当前构建记录情况下的project_history、project_recent
        # project_history
        if result.loc[i, 'status'] == 'passed':
            current_passed += 1
        else:
            current_failed += 1
        # print 'current_passed: ', current_passed
        # print 'current_total: ', current_total
        # print 'division', round(current_passed / current_total, 4)
        result.loc[i, 'project_history'] = round(current_passed / current_total, 4)

        # project_recent
        if i >= 5:
            recent_total = 0
            recent_passed = 0
            for k in range(i-5, i):
                recent_total += 1
                if result.loc[k, 'status'] == 'passed':
                    recent_passed += 1
            if recent_passed == 0:
                result.loc[i, 'project_recent'] = 0
            else:
                result.loc[i, 'project_recent'] = round(recent_passed/recent_total, 4)
        else:
            recent_total = 0
            recent_passed = 0
            for n in range(i):
                recent_total += 1
                if result.loc[n, 'status'] == 'passed':
                    recent_passed += 1
            if recent_passed == 0:
                result.loc[i, 'project_recent'] = 0
            else:
                result.loc[i, 'project_recent'] = round(recent_passed/recent_total, 4)

    print 'build_id, status, last_build, project_recent, project_history'
    for m in result.index:
        print result.loc[m, 'build_id'], result.loc[m, 'status'], result.loc[m, 'last_build'], \
            result.loc[m, 'project_recent'], result.loc[m, 'project_history']
    save_data_into_db(result)
    # 统计某一列的数值
    # print result['status'].value_counts()


def save_data_into_db(df):
    try:
        print '*** write_records_into_mysql ***'
        db = create_engine('mysql+mysqldb://root:123456@localhost:3306/travistorrent_calculated?charset=utf8')
        df.to_sql(name='travistorrent_calculated_20_11', con=db, if_exists='append', index=False)
    except Exception as e:
        print 'error', e
    else:
        print 'write records successfully!'


project_list = get_project_from_db()

# 按项目名遍历所有记录
for project_name in project_list:
    print project_name[0]
    project_name = project_name[0].encode("utf-8")
    calculate_project_related(project_name)




