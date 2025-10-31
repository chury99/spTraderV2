import os
import sys
import json
import time
import re
import multiprocessing as mp


import pandas as pd
from tqdm import tqdm
import matplotlib.pyplot as plt

import ut.로그maker, ut.폴더manager, ut.도구manager as Tool, ut.차트maker
import xapi.RestAPI_kiwoom, xapi.WebsocketAPI_kiwoom


# noinspection NonAsciiCharacters,SpellCheckingInspection,PyPep8Naming
class AnalyzerBot:
    def __init__(self, b_디버그모드=False):
        # config 읽어 오기
        self.folder_베이스 = os.path.dirname(os.path.abspath(__file__))
        self.folder_프로젝트 = os.path.dirname(self.folder_베이스)
        self.s_파일명 = os.path.basename(__file__).replace('.py', '')
        dic_config = json.load(open(os.path.join(self.folder_프로젝트, 'config.json'), mode='rt', encoding='utf-8'))

        # 로그 설정
        log = ut.로그maker.LogMaker(s_파일명=self.s_파일명, s_로그명='로그이름_analyzer')
        sys.stderr = ut.로그maker.StderrHook(path_에러로그=log.path_에러)
        self.make_로그 = log.make_로그

        # 폴더 정의
        dic_폴더정보 = ut.폴더manager.define_폴더정보()
        self.folder_차트캐시 = dic_폴더정보['데이터|차트캐시']
        self.folder_전체종목 = dic_폴더정보['데이터|전체종목']
        self.folder_백테스팅 = dic_폴더정보['분석|백테스팅']
        os.makedirs(self.folder_백테스팅, exist_ok=True)

        # 기준정보 정의
        self.s_오늘 = pd.Timestamp.now().strftime('%Y%m%d')
        self.b_디버그모드 = b_디버그모드
        self.n_멀티코어수 = mp.cpu_count() - 2
        self.dic_매개변수 = dict()

        # 차트maker 정의
        self.chart = ut.차트maker.ChartMaker()

        # 로그 기록
        self.make_로그(f'구동 시작')

    def find_상승시점(self):
        """ 초봉 데이터 기준 상승 시점 찾아서 그래프 저장 """
        # 초봉 데이터 선정
        for n_봉수 in [1, 2, 3, 5, 10, 12, 15, 20, 30]:
            # 기준정보 정의
            folder_소스 = os.path.join(self.folder_차트캐시, f'초봉{n_봉수}')
            file_소스 = f'dic_차트캐시_{n_봉수}초봉'
            folder_결과 = os.path.join(self.folder_백테스팅, '상승시점', f'초봉{n_봉수}')
            file_결과 = f'df_상승시점_{n_봉수}'
            os.makedirs(folder_결과, exist_ok=True)

            # 대상일자 확인
            li_전체일자 = sorted(re.findall(r'\d{8}', 파일)[0] for 파일 in os.listdir(folder_소스) if '.pkl' in 파일)
            li_완료일자 = [re.findall(r'\d{8}', 파일)[0] for 파일 in os.listdir(folder_결과) if '.pkl' in 파일]
            li_대상일자 = [일자 for 일자 in li_전체일자 if 일자 not in li_완료일자]

            # 일자별 캐시 생성
            for s_일자 in li_대상일자:
                # 기준정보 정의
                self.dic_매개변수['s_일자'] = s_일자
                self.dic_매개변수['n_봉수'] = n_봉수

                # 초봉 읽어오기
                dic_초봉 = pd.read_pickle(os.path.join(folder_소스, f'{file_소스}_{s_일자}.pkl'))
                self.dic_매개변수['dic_초봉'] = dic_초봉
                li_대상종목 = list(dic_초봉.keys())

                # 종목명 생성
                df_전체종목 = pd.read_pickle(os.path.join(self.folder_전체종목, f'df_전체종목_{s_일자}.pkl'))
                self.dic_매개변수['dic_코드2종목'] = df_전체종목.set_index(['종목코드'])['종목명'].to_dict()

                # 종목별 검증
                li_df상승시점 = list()
                if self.b_디버그모드:
                    for s_종목코드 in tqdm(li_대상종목, desc=f'상승시점-{n_봉수}초봉-{s_일자}', file=sys.stdout):
                        li_df상승시점.append(self._find_상승시점_종목(s_종목코드=s_종목코드))
                else:
                    with mp.Pool(processes=self.n_멀티코어수) as pool:
                        li_df상승시점 = list(tqdm(pool.imap(self._find_상승시점_종목, li_대상종목),
                                    total=len(li_대상종목), desc=f'상승시점-{n_봉수}초봉-{s_일자}', file=sys.stdout))
                dic_상승시점 = dict(zip(li_대상종목, li_df상승시점))

                # 데이터 정리

                # 데이터 저장

                # 로그 기록
                self.make_로그(f'저장 완료 - {len(df_전체종목):,.0f} 종목 - 코스피 {len(df_코스피):,.0f}, 코스닥 {len(df_코스닥):,.0f}')

                pass

    def _find_상승시점_종목(self, s_종목코드):
        """ 종목코드별 초봉 데이터의 상승시점 기록 후 리턴 """
        # 기준정보 정의
        df_초봉 = self.dic_매개변수['dic_초봉'][s_종목코드].sort_index()
        s_종목명 = self.dic_매개변수['dic_코드2종목'][s_종목코드]
        s_일자 = self.dic_매개변수['s_일자']
        n_봉수 = self.dic_매개변수['n_봉수']

        # 데이터 추가
        df_상승 = df_초봉.copy()
        df_상승['상승'] = df_상승['고가'] > df_상승['고가'].shift(1)

        # 차트 생성
        fig = plt.Figure(figsize=(16, 9), tight_layout=True)
        df_상승1 = df_상승[df_상승.index >= pd.Timestamp(f'{s_일자} 09:00:00')]
        df_상승1 = df_상승1[df_상승1.index <= pd.Timestamp(f'{s_일자} 12:00:00')]
        dic_매개변수 = dict(df_초봉=df_상승1, s_종목코드=s_종목코드, s_종목명=s_종목명, s_일자=s_일자, n_봉수=n_봉수)
        ax = self.chart._make_초봉차트(ax=fig.add_subplot(1, 1, 1), dic_매개변수=dic_매개변수)

        # 차트 저장
        folder = os.path.join(self.folder_백테스팅, f'상승시점_차트_{s_일자}')
        os.makedirs(folder, exist_ok=True)
        fig.savefig(os.path.join(folder, f'상승시점_{s_일자}_{s_종목코드}.png'))

        pass
        return


def run():
    """ 실행 함수 """
    a = AnalyzerBot(b_디버그모드=True)
    a.find_상승시점()

if __name__ == '__main__':
    run()
