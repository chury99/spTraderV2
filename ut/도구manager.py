import os
import pandas as pd


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
