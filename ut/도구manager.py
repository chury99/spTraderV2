import os
import pandas as pd
import sqlite3


# noinspection PyPep8Naming,SpellCheckingInspection,NonAsciiCharacters
def df저장(df, path, li_타입=None):
    """ 입력받은 df를 path에 pkl, csv로 저장 """
    # 저장타입 지정
    li_타입 = ['pkl', 'csv'] if li_타입 is None else li_타입

    # pkl 저장
    if 'pkl' in li_타입:
        path_pkl = f'{path}.pkl'
        df.to_pickle(path_pkl)

    # csv 저장
    if 'csv' in li_타입:
        path_csv = f'{path}.csv'
        df.to_csv(path_csv, encoding='cp949', index=False)

# noinspection PyPep8Naming,SpellCheckingInspection,NonAsciiCharacters
def sql불러오기(path, s_테이블명=None, b_전체=False):
    """ sql 파일을 불러와서 테이블명, 데이터 리턴 """
    # 파일 연결
    con = sqlite3.connect(path)

    # 전체 데이터 불러오기
    if b_전체:
        df_테이블명 = pd.read_sql(f'SELECT name FROM sqlite_master WHERE type="table"', con=con)
        dic_데이터 = dict()
        for s_테이블명 in df_테이블명['name']:
            dic_데이터[s_테이블명] = pd.read_sql(f'SELECT * FROM {s_테이블명}', con=con)

        return dic_데이터

    # 테이블명 불러오기
    elif s_테이블명 is None:
        df_테이블명 = pd.read_sql(f'SELECT name FROM sqlite_master WHERE type="table"', con=con)
        li_테이블명 = list(df_테이블명['name'])
        con.close()

        return li_테이블명

    # 데이터 불러오기
    elif s_테이블명 is not None:
        df_데이터 = pd.read_sql(f'SELECT * FROM {s_테이블명}', con=con)
        con.close()

        return df_데이터

    else:
        con.close()
        return None
