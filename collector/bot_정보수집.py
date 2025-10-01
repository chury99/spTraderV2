import os
import sys
import json
import time

import pandas as pd

import ut.로그maker, ut.폴더manager, ut.도구manager as Tool
import xapi.RestAPI_kiwoom, xapi.WebsocketAPI_kiwoom


# noinspection NonAsciiCharacters,SpellCheckingInspection,PyPep8Naming
class CollectorBot:
    def __init__(self):
        # config 읽어 오기
        self.folder_베이스 = os.path.dirname(os.path.abspath(__file__))
        self.folder_프로젝트 = os.path.dirname(self.folder_베이스)
        self.s_파일명 = os.path.basename(__file__).replace('.py', '')
        dic_config = json.load(open(os.path.join(self.folder_프로젝트, 'config.json'), mode='rt', encoding='utf-8'))

        # 로그 설정
        log = ut.로그maker.LogMaker(s_파일명=self.s_파일명, s_로그명='로그이름_collector')
        sys.stderr = ut.로그maker.StderrHook(path_에러로그=log.path_에러)
        self.make_로그 = log.make_로그

        # 폴더 정의
        dic_폴더정보 = ut.폴더manager.define_폴더정보()
        self.folder_전체종목 = dic_폴더정보['데이터|전체종목']
        self.folder_대상종목 = dic_폴더정보['데이터|대상종목']
        self.folder_조회순위 = dic_폴더정보['데이터|조회순위']
        os.makedirs(self.folder_전체종목, exist_ok=True)
        os.makedirs(self.folder_대상종목, exist_ok=True)
        os.makedirs(self.folder_조회순위, exist_ok=True)

        # 기준정보 정의
        self.s_오늘 = pd.Timestamp.now().strftime('%Y%m%d')

        # 로그 기록
        self.make_로그(f'구동 시작')

    def get_전체종목(self):
        """ 코스피, 코스닥 전체 종목 조회하여 저장 """
        # API 정의
        api = xapi.RestAPI_kiwoom.RestAPIkiwoom()

        # 데이터 받아오기
        li_df_전체종목 = list()
        for s_시장 in ['코스피', '코스닥']:
            df_종목 = api.tr_업종별주가요청(s_시장=s_시장)
            df_종목['시장'] = s_시장
            li_df_전체종목.append(df_종목)

        # 데이터 정리
        df_전체종목 = pd.concat(li_df_전체종목, axis=0)
        df_전체종목 = df_전체종목.loc[:, ['종목코드', '종목명', '시장']].reset_index(drop=True)

        # 데이터 저장
        Tool.df저장(df=df_전체종목, path=os.path.join(self.folder_전체종목, f'df_전체종목_{self.s_오늘}'))

        # 로그 기록
        self.make_로그(f'저장 완료 - {len(df_전체종목):,.0f} 종목')

    def get_대상종목(self):
        """ 조건검색식에서 대상종목 다운받아서 저장 """
        # 기준정보 불러오기
        df_전체종목 = pd.read_pickle(os.path.join(self.folder_전체종목, f'df_전체종목_{self.s_오늘}.pkl'))
        dic_코드2종목명 = df_전체종목.set_index('종목코드')['종목명'].to_dict()

        # API 정의
        api = xapi.WebsocketAPI_kiwoom.SimpleWebsocketAPI()

        # 데이터 받아오기
        n_검색식번호 = 5
        df_조검검색목록, df_대상종목 = api.get_조건검색(n_검색식번호=n_검색식번호)

        # 데이터 정리
        df_대상종목['종목코드'] = df_대상종목['종목코드'].str[1:]
        df_대상종목['종목명'] = df_대상종목['종목코드'].apply(lambda x: dic_코드2종목명[x] if x in dic_코드2종목명 else None)
        df_대상종목['검색식'] = df_조검검색목록.set_index('검색식번호')['검색식명'].to_dict()[str(n_검색식번호)]
        df_대상종목 = df_대상종목.sort_values('종목코드').reset_index(drop=True)

        # 데이터 저장
        Tool.df저장(df=df_대상종목, path=os.path.join(self.folder_대상종목, f'df_대상종목_{self.s_오늘}'))

        # 로그 기록
        self.make_로그(f'저장 완료 - {len(df_대상종목):,.0f} 종목')


def run():
    """ 실행 함수 """
    c = CollectorBot()
    c.get_전체종목()
    c.get_대상종목()

if __name__ == '__main__':
    run()
