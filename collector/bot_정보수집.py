import os
import sys
import json
import time
import re

import pandas as pd

import ut.설정manager, ut.로그maker, ut.폴더manager, ut.도구manager as Tool
import xapi.RestAPI_kiwoom, xapi.WebsocketAPI_kiwoom


# noinspection NonAsciiCharacters,SpellCheckingInspection,PyPep8Naming
class CollectorBot:
    def __init__(self):
        # config 읽어 오기
        self.folder_베이스 = os.path.dirname(os.path.abspath(__file__))
        self.folder_프로젝트 = os.path.dirname(self.folder_베이스)
        self.s_파일명 = os.path.basename(__file__).replace('.py', '')
        # dic_config = json.load(open(os.path.join(self.folder_프로젝트, 'config.json'), mode='rt', encoding='utf-8'))
        dic_config = ut.설정manager.ConfigManager().dic_config

        # 로그 설정
        log = ut.로그maker.LogMaker(s_파일명=self.s_파일명, s_로그명='로그이름_collector')
        sys.stderr = ut.로그maker.StderrHook(path_에러로그=log.path_에러)
        self.make_로그 = log.make_로그

        # 폴더 정의
        dic_폴더정보 = ut.폴더manager.define_폴더정보()
        self.folder_전체종목 = dic_폴더정보['데이터|전체종목']
        self.folder_조건검색 = dic_폴더정보['데이터|조건검색']
        self.folder_대상종목 = dic_폴더정보['데이터|대상종목']
        os.makedirs(self.folder_전체종목, exist_ok=True)
        os.makedirs(self.folder_조건검색, exist_ok=True)
        os.makedirs(self.folder_대상종목, exist_ok=True)

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
        df_전체종목 = pd.concat(li_df_전체종목, axis=0).sort_values('종목코드')
        df_전체종목 = df_전체종목.loc[:, ['종목코드', '종목명', '시장']].reset_index(drop=True)

        # 데이터 저장
        Tool.df저장(df=df_전체종목, path=os.path.join(self.folder_전체종목, f'df_전체종목_{self.s_오늘}'))

        # 로그 기록
        df_코스피 = df_전체종목[df_전체종목['시장']=='코스피']
        df_코스닥 = df_전체종목[df_전체종목['시장']=='코스닥']
        self.make_로그(f'저장 완료 - {self.s_오늘}\n'
                     f' - {len(df_전체종목):,.0f} 종목 - 코스피 {len(df_코스피):,.0f}, 코스닥 {len(df_코스닥):,.0f}')

    def get_조건검색(self):
        """ 조건검색에 등록된 항목 조회하여 하나의 df로 저장 """
        # 기준정보 불러오기
        df_전체종목 = pd.read_pickle(os.path.join(self.folder_전체종목, f'df_전체종목_{self.s_오늘}.pkl'))
        dic_코드2종목명 = df_전체종목.set_index('종목코드')['종목명'].to_dict()

        # 조건검색목록 확인
        df_조검검색목록 = xapi.WebsocketAPI_kiwoom.SimpleWebsocketAPI().get_조건검색()
        dic_번호2검색식명 = df_조검검색목록.set_index('검색식번호')['검색식명'].to_dict()

        # 데이터 받아오기
        li_df조건검색 = list()
        for s_검색식번호 in df_조검검색목록['검색식번호'].unique():
            # 검색식 조회
            df_검색종목 = xapi.WebsocketAPI_kiwoom.SimpleWebsocketAPI().get_조건검색(n_검색식번호=int(s_검색식번호))

            # 데이터 정리
            s_검색식명 = dic_번호2검색식명[s_검색식번호]
            b_데이터존재 = not df_검색종목.empty
            # df_검색종목 = df_검색종목 if b_데이터존재 else li_df조건검색[-1][-1:].copy()
            df_검색종목 = df_검색종목 if b_데이터존재 else pd.DataFrame()
            df_검색종목['종목코드'] = df_검색종목['종목코드'].str[1:]\
                                    if b_데이터존재 else [None]
            df_검색종목['종목명'] = df_검색종목['종목코드'].apply(lambda x: dic_코드2종목명[x] if x in dic_코드2종목명 else None)\
                                    if b_데이터존재 else None
            df_검색종목['검색식번호'] = s_검색식번호
            df_검색종목['검색식명'] = s_검색식명
            df_검색종목['조회일자'] = self.s_오늘
            df_검색종목['조회시간'] = pd.Timestamp.now().strftime('%H%M%S')

            # 데이터 정리
            df_검색종목 = df_검색종목[df_검색종목['종목명'].notna()] if b_데이터존재 else df_검색종목
            df_검색종목 = df_검색종목.sort_values('종목코드').reset_index(drop=True)

            # 데이터 추가
            li_df조건검색.append(df_검색종목)

        # 데이터 통합
        df_조건검색 = pd.concat(li_df조건검색, axis=0)

        # 데이터 저장
        Tool.df저장(df=df_조건검색, path=os.path.join(self.folder_조건검색, f'df_조건검색_{self.s_오늘}'))

        # 로그 기록
        self.make_로그(f'저장 완료 - {self.s_오늘}\n'
                     f' - {len(df_조건검색['검색식번호'].unique()):,.0f}개 검색식')

    def get_대상종목(self):
        """ 저장된 조건검색에서 대상종목 필터링 후 저장 """
        # 대상일자 확인
        li_전체일자 = sorted(re.findall(r'\d{8}', 파일)[0] for 파일 in os.listdir(self.folder_조건검색) if '.pkl' in 파일)
        li_완료일자 = [re.findall(r'\d{8}', 파일)[0] for 파일 in os.listdir(self.folder_대상종목) if '.pkl' in 파일]
        li_대상일자 = [일자 for 일자 in li_전체일자 if 일자 not in li_완료일자]

        # 일자별 데이터 생성
        for s_일자 in li_대상일자:
            # 조건검색 불러오기
            df_조건검색 = pd.read_pickle(os.path.join(self.folder_조건검색, f'df_조건검색_{s_일자}.pkl'))

            # 대상종목 필터링
            df_대상종목 = df_조건검색[df_조건검색['검색식명'] == '분석대상종목'].sort_values('종목코드').reset_index(drop=True)

            # 데이터 저장
            Tool.df저장(df=df_대상종목, path=os.path.join(self.folder_대상종목, f'df_대상종목_{s_일자}'))

            # 로그 기록
            self.make_로그(f'저장 완료 - {s_일자}\n'
                         f' - {len(df_대상종목):,.0f} 종목')


def run():
    """ 실행 함수 """
    c = CollectorBot()
    c.get_전체종목()
    c.get_조건검색()
    c.get_대상종목()

if __name__ == '__main__':
    try:
        run()
    except KeyboardInterrupt:
        print('\n### [ KeyboardInterrupt detected ] ###')
