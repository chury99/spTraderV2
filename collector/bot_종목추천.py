import os
import sys
import json
import time
import re

import pandas as pd

import ut.로그maker, ut.폴더manager, ut.도구manager as Tool
import xapi.RestAPI_kiwoom, xapi.WebsocketAPI_kiwoom
import analyzer.logic_상승후보


# noinspection NonAsciiCharacters,SpellCheckingInspection,PyPep8Naming
class CollectorBot:
    def __init__(self):
        # config 읽어 오기
        self.folder_베이스 = os.path.dirname(os.path.abspath(__file__))
        self.folder_프로젝트 = os.path.dirname(self.folder_베이스)
        self.s_파일명 = os.path.basename(__file__).replace('.py', '')
        dic_config = Tool.config로딩()

        # 로그 설정
        log = ut.로그maker.LogMaker(s_파일명=self.s_파일명, s_로그명='로그이름_collector')
        sys.stderr = ut.로그maker.StderrHook(path_에러로그=log.path_에러)
        self.make_로그 = log.make_로그

        # 폴더 정의
        dic_폴더정보 = ut.폴더manager.define_폴더정보()
        self.folder_차트캐시 = dic_폴더정보['데이터|차트캐시']
        self.folder_전체종목 = dic_폴더정보['데이터|전체종목']
        self.folder_조건검색 = dic_폴더정보['데이터|조건검색']
        self.folder_조회순위 = dic_폴더정보['데이터|조회순위']
        self.folder_대상종목 = dic_폴더정보['데이터|대상종목']
        self.folder_종목추천 = dic_폴더정보['데이터|종목추천']
        os.makedirs(self.folder_종목추천, exist_ok=True)

        # 기준정보 정의
        self.s_오늘 = pd.Timestamp.now().strftime('%Y%m%d')

        # 카카오 API 연결
        sys.path.append(dic_config['folder_kakao'])
        # noinspection PyUnresolvedReferences
        import API_kakao
        self.kakao = API_kakao.KakaoAPI()

        # 로그 기록
        self.make_로그(f'구동 시작')

    def find_거북이추천(self):
        """ 조회순위 종목 중 거북이추천 종목에 포함된 종목 선정하여 저장 """
        # 조회순위 불러오기
        df_조회순위 = pd.read_csv(os.path.join(self.folder_조회순위, f'df_조회순위_{self.s_오늘}.csv'), encoding='cp949', dtype=str)
        # li_조회순위 = df_조회순위['종목코드'].unique().tolist()
        li_조회순위 = df_조회순위.dropna(subset='종목코드')['종목코드'].unique().tolist()

        # 거북이추천 불러오기
        df_조건검색 = pd.read_pickle(os.path.join(self.folder_조건검색, f'df_조건검색_{self.s_오늘}.pkl'))
        li_거북이추천 = df_조건검색[df_조건검색['검색식명'] == '거북이추천']['종목코드'].tolist()

        # 대상종목 불러오기 - 이상한 종목 제외용
        df_대상종목 = pd.read_pickle(os.path.join(self.folder_대상종목, f'df_대상종목_{self.s_오늘}.pkl'))
        li_대상종목 = df_대상종목['종목코드'].tolist()
        dic_코드2종목명 = df_대상종목.set_index('종목코드')['종목명'].to_dict()

        # 추천종목 생성
        li_추천종목 = [종목 for 종목 in li_조회순위 if 종목 in li_거북이추천 and 종목 in li_대상종목]
        li_dic추천종목 = [dict(일자=self.s_오늘, 종목코드=종목, 종목명=dic_코드2종목명[종목]) for 종목 in li_추천종목]
        df_추천종목 = pd.DataFrame(li_dic추천종목)

        # 데이터 저장
        folder_타겟 = os.path.join(self.folder_종목추천, '거북이')
        os.makedirs(folder_타겟, exist_ok=True)
        Tool.df저장(df=df_추천종목, path=os.path.join(folder_타겟, f'df_종목추천_거북이_{self.s_오늘}'))

        # 카톡송부
        s_메세지 = f'## [{self.s_오늘}] 거북이 추천종목 {len(li_추천종목)}개 ##'
        for s_종목코드 in li_추천종목:
            s_메세지 = s_메세지 + f'\n  {dic_코드2종목명[s_종목코드]}({s_종목코드})'
        self.kakao.send_메세지(s_사용자='알림봇', s_수신인='여봉이', s_메세지=s_메세지)

        # 로그 기록
        self.make_로그(f'{self.s_오늘} 완료\n'
                     f' - {len(df_추천종목):,.0f} 종목')

    def find_조회순위추천(self):
        """ 조회순위 데이터 기준으로 일봉차트 확인하여 대상종목 선정 """
        # 일자 확인
        li_전체일자 = [re.findall(r'\d{8}', 파일)[0]
                        for 파일 in os.listdir(os.path.join(self.folder_차트캐시, '일봉1')) if '.pkl' in 파일]
        s_일자 = max(일자 for 일자 in li_전체일자 if 일자 <= self.s_오늘)

        # 데이터 정리
        df_상승후보 = analyzer.logic_상승후보.check_조회순위(s_일자=s_일자)
        df_추천종목 = df_상승후보.loc[(df_상승후보['당일조건'])
                                & (df_상승후보['당일바디'] > 0) & (df_상승후보['당일바디'] < 2)]\
                    if len(df_상승후보) > 0 else pd.DataFrame()
        li_추천종목 = sorted(df_추천종목['종목코드'].unique())
        dic_코드2종목명 = df_추천종목.set_index('종목코드')['종목명'].to_dict()

        # 데이터 저장
        folder_타겟 = os.path.join(self.folder_종목추천, '조회순위')
        os.makedirs(folder_타겟, exist_ok=True)
        Tool.df저장(df=df_추천종목, path=os.path.join(folder_타겟, f'df_종목추천_조회순위_{s_일자}'))

        # 카톡송부
        s_메세지 = f'## [{s_일자}] 조회순위 추천종목 {len(li_추천종목)}개 ##'
        for s_종목코드 in li_추천종목:
            s_메세지 = s_메세지 + f'\n  {dic_코드2종목명[s_종목코드]}({s_종목코드})'
        self.kakao.send_메세지(s_사용자='알림봇', s_수신인='여봉이', s_메세지=s_메세지)

        # 로그 기록
        self.make_로그(f'{s_일자} 완료\n'
                     f' - {len(df_추천종목):,.0f} 종목')


# noinspection NonAsciiCharacters,SpellCheckingInspection,PyPep8Naming
def run_거북이추천():
    """ 실행 함수 """
    c = CollectorBot()
    c.find_거북이추천()

# noinspection NonAsciiCharacters,SpellCheckingInspection,PyPep8Naming
def run_조회순위추천():
    """ 실행 함수 """
    c = CollectorBot()
    c.find_조회순위추천()

if __name__ == '__main__':
    try:
        run_거북이추천()
        run_조회순위추천()
    except KeyboardInterrupt:
        print('\n### [ KeyboardInterrupt detected ] ###')
