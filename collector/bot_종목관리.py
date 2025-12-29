import os
import sys
import json
import time
import re

import pandas as pd

import ut.로그maker, ut.폴더manager, ut.도구manager as Tool
import xapi.RestAPI_kiwoom, xapi.WebsocketAPI_kiwoom
import collector.bot_정보수집


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
        self.folder_조건검색 = dic_폴더정보['데이터|조건검색']
        self.folder_종목관리 = dic_폴더정보['데이터|종목관리']
        os.makedirs(self.folder_종목관리, exist_ok=True)

        # 기준정보 정의
        self.s_오늘 = pd.Timestamp.now().strftime('%Y%m%d')

        # 카카오 API 연결
        sys.path.append(dic_config['folder_kakao'])
        # noinspection PyUnresolvedReferences
        import API_kakao
        self.kakao = API_kakao.KakaoAPI()

        # 로그 기록
        self.make_로그(f'구동 시작')

    def check_손절신호(self):
        """ 보유종목 중 손절이 필요한 종목 찾아서 카톡 발송 """
        # 보유종목 가져오기 - 조건검색 활용
        df_조건검색 = pd.read_pickle(os.path.join(self.folder_조건검색, f'df_조건검색_{self.s_오늘}.pkl'))
        df_보유종목 = df_조건검색.loc[df_조건검색['검색식명'] == '보유종목']

        # 일봉 가져오기 - 전일 기준
        folder_일봉 = os.path.join(self.folder_차트캐시, '일봉1')
        li_일자 = [re.findall(r'\d{8}', 파일)[0] for 파일 in os.listdir(folder_일봉) if '.pkl' in 파일]
        s_전일 = max(일자 for 일자 in li_일자 if 일자 < self.s_오늘)
        dic_전일일봉 = pd.read_pickle(os.path.join(folder_일봉, f'dic_차트캐시_1일봉_{s_전일}.pkl'))

        # 종목별 데이터 확인
        li_dic손절신호 = list()
        for s_종목코드 in df_보유종목['종목코드']:
            # 기준정보 정의
            df_전일일봉 = dic_전일일봉[s_종목코드]
            dt_전일 = df_전일일봉.index[-1]
            s_일자 = df_전일일봉.loc[dt_전일, '일자']
            s_종목명 = df_전일일봉.loc[dt_전일, '종목명']
            n_시가 = df_전일일봉.loc[dt_전일, '시가']
            n_고가 = df_전일일봉.loc[dt_전일, '고가']
            n_저가 = df_전일일봉.loc[dt_전일, '저가']
            n_종가 = df_전일일봉.loc[dt_전일, '종가']
            n_저가3봉 = min(df_전일일봉['저가'].values[-4:-1])

            # 손절신호 확인
            b_손절신호 = n_종가 < n_저가3봉

            # 데이터 생성
            dic_손절신호 = dict(일자=s_일자, 종목코드=s_종목코드, 종목명=s_종목명, 시가=n_시가, 고가=n_고가, 저가=n_저가, 종가=n_종가,
                            저가3봉=n_저가3봉, 손절신호=b_손절신호)
            li_dic손절신호.append(dic_손절신호)

        # 데이터 정리
        df_손절신호 = pd.DataFrame(li_dic손절신호) if len(li_dic손절신호) > 0 else pd.DataFrame()
        li_손절종목 = df_손절신호.loc[df_손절신호['손절신호']]['종목코드'].tolist()
        dic_코드2종목명 = df_손절신호.set_index('종목코드')['종목명'].to_dict()


        # 데이터 저장
        folder_타겟 = os.path.join(self.folder_종목관리, '손절신호')
        os.makedirs(folder_타겟, exist_ok=True)
        Tool.df저장(df=df_손절신호, path=os.path.join(folder_타겟, f'df_종목관리_손절신호_{self.s_오늘}'))

        # 카톡송부
        s_메세지 = f'# [{self.s_오늘}] 손절종목(시가정리) {len(li_손절종목)}개 #'
        for s_종목코드 in li_손절종목:
            s_메세지 = s_메세지 + f'\n  {dic_코드2종목명[s_종목코드]}({s_종목코드})'
        self.kakao.send_메세지(s_사용자='알림봇', s_수신인='여봉이', s_메세지=s_메세지)

        # 로그 기록
        self.make_로그(f'{self.s_오늘} 완료\n'
                     f' - 보유 {len(df_손절신호):,.0f}종목, 손절 {len(li_손절종목)}종목')


# noinspection NonAsciiCharacters,SpellCheckingInspection,PyPep8Naming
def run():
    """ 실행 함수 """
    c = CollectorBot()
    c.check_손절신호()

if __name__ == '__main__':
    try:
        run()
    except KeyboardInterrupt:
        print('\n### [ KeyboardInterrupt detected ] ###')
