import os
import pandas as pd
import sqlite3
import json
import paramiko
from pandas.core.methods.selectn import SelectNSeries


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


# noinspection PyPep8Naming,SpellCheckingInspection,NonAsciiCharacters
def sftp_동기화_파일명(folder_로컬, folder_서버, s_모드):
    """ sftp 서버 접속 후 파일명 기준으로 동기화 """
    # 기준정보 정의
    folder_베이스 = os.path.dirname(os.path.abspath(__file__))
    folder_프로젝트 = os.path.dirname(folder_베이스)

    # 서버정보 정의
    dic_서버정보 = json.load(open(os.path.join(folder_프로젝트, 'server_info.json'), mode='rt', encoding='utf-8'))
    dic_서버접속 = dic_서버정보['sftp']
    dic_서버폴더 = dic_서버정보['folder']

    # 서버 접속
    li_동기화파일명 = list()
    with paramiko.SSHClient() as ssh:
        # ssh 서버 연결 (알수없는 서버 경고 방지 포함)
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(hostname=dic_서버접속['hostname'], port=dic_서버접속['port'],
                    username=dic_서버접속['username'], password=dic_서버접속['password'])

        # sftp 세션 시작
        with ssh.open_sftp() as sftp:
            # 폴더 내 파일 확인
            li_로컬파일 = sorted(파일 for 파일 in os.listdir(folder_로컬) if not 파일.startswith('.'))
            li_서버파일 = sorted(파일 for 파일 in sftp.listdir(folder_서버) if not 파일.startswith('.'))

            # 대상파일 확인
            li_대상파일 = [파일 for 파일 in li_로컬파일 if 파일 not in li_서버파일] if s_모드 == '로컬2서버' else\
                        [파일 for 파일 in li_서버파일 if 파일 not in li_로컬파일] if s_모드 == '서버2로컬' else list()

            # 대상폴더 정의
            folder_원본 = folder_로컬 if s_모드 == '로컬2서버' else folder_서버 if s_모드 == '서버2로컬' else None
            folder_타겟 = folder_서버 if s_모드 == '로컬2서버' else folder_로컬 if s_모드 == '서버2로컬' else None

            # 파일 복사
            for s_파일명 in li_대상파일:
                # 경로 정의
                path_원본 = f'{folder_원본}/{s_파일명}'
                path_타겟 = f'{folder_타겟}/{s_파일명}'

                # 파일 복사
                ret = sftp.put(path_원본, path_타겟) if s_모드 == '로컬2서버' else\
                        sftp.get(path_원본, path_타겟) if s_모드 == '서버2로컬' else None

                # 정보 등록
                li_동기화파일명.append(s_파일명)

    return li_동기화파일명
