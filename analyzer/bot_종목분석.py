import os
import sys
import json
import time
import re
import multiprocessing as mp


import pandas as pd
from fontTools.ttLib.tables.otTables import DeltaSetIndexMap
from tqdm import tqdm
import matplotlib.pyplot as plt

import ut.설정manager, ut.로그maker, ut.폴더manager, ut.도구manager as Tool, ut.차트maker
import xapi.RestAPI_kiwoom, xapi.WebsocketAPI_kiwoom


# noinspection NonAsciiCharacters,SpellCheckingInspection,PyPep8Naming
class AnalyzerBot:
    def __init__(self, b_디버그모드=False, s_시작일자=None):
        # config 읽어 오기
        self.folder_베이스 = os.path.dirname(os.path.abspath(__file__))
        self.folder_프로젝트 = os.path.dirname(self.folder_베이스)
        self.s_파일명 = os.path.basename(__file__).replace('.py', '')
        # dic_config = json.load(open(os.path.join(self.folder_프로젝트, 'config.json'), mode='rt', encoding='utf-8'))
        dic_config = ut.설정manager.ConfigManager().dic_config

        # 로그 설정
        log = ut.로그maker.LogMaker(s_파일명=self.s_파일명, s_로그명='로그이름_analyzer')
        sys.stderr = ut.로그maker.StderrHook(path_에러로그=log.path_에러)
        self.make_로그 = log.make_로그

        # 폴더 정의
        dic_폴더정보 = ut.폴더manager.define_폴더정보()
        self.folder_차트캐시 = dic_폴더정보['데이터|차트캐시']
        self.folder_전체종목 = dic_폴더정보['데이터|전체종목']
        self.folder_조건검색 = dic_폴더정보['데이터|조건검색']
        self.folder_조회순위 = dic_폴더정보['데이터|조회순위']
        self.folder_종목분석 = dic_폴더정보['분석|종목분석']
        os.makedirs(self.folder_종목분석, exist_ok=True)

        # 시작일자 정의
        n_보관기간 = int(dic_config['파일보관기간(일)_analyzer'])
        s_보관일자 = (pd.Timestamp.now() - pd.DateOffset(days=n_보관기간)).strftime('%Y%m%d')
        self.s_시작일자 = s_시작일자 if s_시작일자 is not None else s_보관일자

        # 기준정보 정의
        self.s_오늘 = pd.Timestamp.now().strftime('%Y%m%d')
        self.b_디버그모드 = b_디버그모드
        self.n_멀티코어수 = mp.cpu_count() - 2
        self.dic_매개변수 = dict()

        # 차트maker 정의
        self.chart = ut.차트maker.ChartMaker()

        # 로그 기록
        self.make_로그(f'구동 시작')

    def find_상승후보(self):
        """ 조회순위에 등장한 종목 중 추천종목에 포함된 종목 찾아서 저장 """
        # 기준정보 정의
        folder_소스 = self.folder_조회순위
        file_소스 = f'df_조회순위'
        folder_타겟 = os.path.join(self.folder_종목분석, '10_상승후보')
        file_타겟 = f'df_상승후보'
        os.makedirs(folder_타겟, exist_ok=True)

        # 대상일자 확인
        li_전체일자 = sorted(re.findall(r'\d{8}', 파일)[0] for 파일 in os.listdir(folder_소스) if '.csv' in 파일)
        li_전체일자 = [일자 for 일자 in li_전체일자 if 일자 >= self.s_시작일자 and 일자 != self.s_오늘]
        li_완료일자 = [re.findall(r'\d{8}', 파일)[0] for 파일 in os.listdir(folder_타겟) if '.pkl' in 파일]
        li_대상일자 = [일자 for 일자 in li_전체일자 if 일자 not in li_완료일자]

        # 일자별 데이터 생성
        for s_일자 in li_대상일자:
            # 소스파일 불러오기
            df_조회순위 = pd.read_csv(os.path.join(folder_소스, f'{file_소스}_{s_일자}.csv'), encoding='cp949', dtype=str)
            dic_코드2종목 = df_조회순위.drop_duplicates('종목코드').set_index('종목코드')['종목명'].to_dict()
            li_조회순위 = df_조회순위['종목코드'].unique().tolist()

            # 추천종목 불러오기
            li_일자 = [re.findall(r'\d{8}', 파일)[0] for 파일 in os.listdir(self.folder_조건검색) if '.pkl' in 파일]
            s_기준일자_추천종목 = min(일자 for 일자 in li_일자 if 일자 >= s_일자) if max(li_일자) >= s_일자 else max(li_일자)
            df_조건검색 = pd.read_pickle(os.path.join(self.folder_조건검색, f'df_조건검색_{s_기준일자_추천종목}.pkl'))
            li_추천종목 = df_조건검색[df_조건검색['검색식명'] == '거북이추천']['종목코드'].tolist()

            # 후보종목 생성
            li_dic상승후보 = list()
            li_상승후보 = [종목 for 종목 in li_조회순위 if 종목 in li_추천종목]
            for s_종목코드 in li_상승후보:
                dic_상승후보 = dict(등장일자=s_일자, 종목코드=s_종목코드, 종목명=dic_코드2종목[s_종목코드])
                li_dic상승후보.append(dic_상승후보)
            df_상승후보 = pd.DataFrame(li_dic상승후보) if len(li_dic상승후보) > 0 else pd.DataFrame()

            # 데이터 저장
            Tool.df저장(df=df_상승후보, path=os.path.join(folder_타겟, f'{file_타겟}_{s_일자}'))

            # 로그 기록
            self.make_로그(f'{s_일자} 완료 - {len(df_상승후보):,.0f} 종목')

    def make_매수신호(self, n_포함일수=5):
        """ 상승후보 종목 대상으로 매수신호 확인 후 저장 """
        # 기준정보 정의
        folder_소스 = os.path.join(self.folder_종목분석, '10_상승후보')
        file_소스 = f'df_상승후보'
        folder_타겟 = os.path.join(self.folder_종목분석, '20_매수신호')
        file_타겟 = f'df_매수신호'
        os.makedirs(folder_타겟, exist_ok=True)

        # 대상일자 확인
        li_전체일자 = sorted(re.findall(r'\d{8}', 파일)[0] for 파일 in os.listdir(folder_소스) if '.pkl' in 파일)
        li_완료일자 = [re.findall(r'\d{8}', 파일)[0] for 파일 in os.listdir(folder_타겟) if '.pkl' in 파일]
        li_대상일자 = [일자 for 일자 in li_전체일자 if 일자 not in li_완료일자]

        # 일자별 데이터 생성
        for s_일자 in li_대상일자:
            # 소스파일 불러오기
            li_파일 = sorted(파일 for 파일 in os.listdir(folder_소스)
                                if '.pkl' in 파일 and re.findall(r'\d{8}', 파일)[0] <= s_일자)[-n_포함일수:]
            li_df상승후보 = [pd.read_pickle(os.path.join(folder_소스, 파일)) for 파일 in li_파일]
            df_상승후보 = pd.concat(li_df상승후보, axis=0).drop_duplicates(subset='종목코드', keep='last')

            # 기준정보 생성
            dic_코드2종목 = df_상승후보.set_index(['종목코드'])['종목명'].to_dict()
            dic_코드2등장일자 = df_상승후보.set_index(['종목코드'])['등장일자'].to_dict()

            # 일봉 불러오기
            dic_일봉 = pd.read_pickle(os.path.join(self.folder_차트캐시, '일봉1', f'dic_차트캐시_1일봉_{s_일자}.pkl'))

            # 매수신호 생성
            li_dic매수신호 = list()
            for s_종목코드 in df_상승후보['종목코드']:
                # 기준정보 정의
                df_일봉 = dic_일봉[s_종목코드]
                df_일봉 = df_일봉[df_일봉['일자'] <= s_일자]
                n_종가 = df_일봉['종가'].values[-1]
                n_종가60 = df_일봉['종가ma60'].values[-1]
                n_종가120 = df_일봉['종가ma120'].values[-1]
                n_고가3봉 = df_일봉['고가'].values[-4: -1].max()

                # 데이터 정의
                dic_매수신호 = dict(일자=s_일자, 종목코드=s_종목코드, 종목명=dic_코드2종목[s_종목코드],
                                등장일자=dic_코드2등장일자[s_종목코드], 종가=n_종가, 종가120=n_종가120, 고가3봉=n_고가3봉,
                                b정배열=n_종가 > n_종가60 > n_종가120, b고가3봉=n_종가 > n_고가3봉)
                li_dic매수신호.append(dic_매수신호)
            df_매수신호 = pd.DataFrame(li_dic매수신호)

            # 데이터 저장
            Tool.df저장(df=df_매수신호, path=os.path.join(folder_타겟, f'{file_타겟}_{s_일자}'))

            # 로그 기록
            self.make_로그(f'{s_일자} 완료 - {len(df_매수신호):,.0f} 종목')

    def verify_수익검증(self):
        """ 매수신호를 바탕으로 매수, 매도 진행 후 수익 검증 """
        # 기준정보 정의
        folder_소스 = os.path.join(self.folder_종목분석, '20_매수신호')
        file_소스 = f'df_매수신호'
        folder_타겟 = os.path.join(self.folder_종목분석, '30_수익검증')
        file_타겟 = f'df_수익검증'
        os.makedirs(folder_타겟, exist_ok=True)

        # 대상일자 확인
        li_전체일자 = sorted(re.findall(r'\d{8}', 파일)[0] for 파일 in os.listdir(folder_소스) if '.pkl' in 파일)
        li_완료일자 = [re.findall(r'\d{8}', 파일)[0] for 파일 in os.listdir(folder_타겟) if '.pkl' in 파일]
        li_대상일자 = [일자 for 일자 in li_전체일자 if 일자 not in li_완료일자]

        # 일자별 데이터 생성
        for s_일자 in li_대상일자:
            # 소스파일 불러오기 - 전일
            li_파일명 = [파일 for 파일 in os.listdir(folder_소스) if '.pkl' in 파일
                                and re.findall(r'\d{8}', 파일)[0] < s_일자]
            if len(li_파일명) == 0:
                continue
            df_매수신호 = pd.read_pickle(os.path.join(folder_소스, max(li_파일명)))

            # 수익검증 불러오기
            li_파일명 = [파일 for 파일 in os.listdir(folder_타겟) if '.pkl' in 파일]
            df_수익검증 = pd.read_pickle(os.path.join(folder_타겟, max(li_파일명))) if len(li_파일명) > 0 else pd.DataFrame()
            li_보유종목 = df_수익검증[pd.isna(df_수익검증['매도가'])]['종목코드'].tolist() if len(df_수익검증) > 0 else list()

            # 일봉 불러오기
            dic_일봉 = pd.read_pickle(os.path.join(self.folder_차트캐시, '일봉1', f'dic_차트캐시_1일봉_{s_일자}.pkl'))

            # 매수 확인
            li_dic수익검증_매수 = list()
            for s_종목코드 in df_매수신호['종목코드']:
                # 보유 여부 확인
                if s_종목코드 in li_보유종목:
                    continue

                # 기준정보 정의
                df_일봉 = dic_일봉[s_종목코드]
                df_일봉 = df_일봉[df_일봉['일자'] <= s_일자]
                n_시가 = df_일봉['시가'].values[-1]
                n_시가1 = df_일봉['시가'].values[-2]
                n_종가1 = df_일봉['종가'].values[-2]
                n_종가2 = df_일봉['종가'].values[-3]
                n_몽통1 = (n_종가1 / n_종가2 - n_시가1 / n_종가2) * 100
                df_매수신호_종목 = df_매수신호[df_매수신호['종목코드'] == s_종목코드]
                b_정배열 = df_매수신호_종목['b정배열'].values[0]
                b_고가3봉 = df_매수신호_종목['b고가3봉'].values[0]

                # 매수조건 확인
                b_매수 = b_정배열 and n_종가1 > n_시가1 and n_몽통1 < 5
                # b_매수 = b_정배열
                if not b_매수:
                    continue

                # 데이터 정의
                dic_수익검증_매수 = {컬럼: df_매수신호_종목[컬럼].values[0] for 컬럼 in df_매수신호.columns
                                if 컬럼 not in ['종가', '종가120', '고가3봉']}
                dic_수익검증_매수.update(일자=s_일자, 매수일=s_일자, 매수가=n_시가, 매도일=None, 매도가=None)
                li_dic수익검증_매수.append(dic_수익검증_매수)
            df_수익검증_매수 = pd.DataFrame(li_dic수익검증_매수)

            # 수익검증 업데이트
            df_수익검증_매수 = pd.concat([df_수익검증, df_수익검증_매수], axis=0).drop_duplicates(['종목코드', '등장일자'])
            df_수익검증_매수_보유 = df_수익검증_매수[pd.isna(df_수익검증_매수['매도가'])] if len(df_수익검증_매수) > 0 else df_수익검증_매수
            df_수익검증_매수_보유 = df_수익검증_매수_보유.reset_index(drop=True)
            # li_보유종목 = df_수익검증_매수[pd.isna(df_수익검증_매수['매도가'])]['종목코드'].tolist() if len(li_파일명) > 0 else list()

            # 매도 확인
            li_dic수익검증_매도 = list()
            for idx in df_수익검증_매수_보유.index:
                # 기준정보 정의
                s_종목코드 = df_수익검증_매수_보유.loc[idx, '종목코드']
                df_일봉 = dic_일봉[s_종목코드]
                df_일봉 = df_일봉[df_일봉['일자'] <= s_일자]
                n_시가 = df_일봉['시가'].values[-1]
                n_고가 = df_일봉['고가'].values[-1]
                n_종가1 = df_일봉['종가'].values[-2]
                n_저가3봉1 = df_일봉['저가'].values[-5: -2].min()
                n_저가2봉1 = df_일봉['저가'].values[-4: -2].min()
                n_매수가 = df_수익검증_매수_보유.loc[idx, '매수가']

                # 매도조건 확인
                # b_매도_고가 = (n_고가 / n_매수가 - 1) * 100 > 3 and s_일자 == df_수익검증_매수_보유.loc[idx, '매수일']
                # b_매도_저가3봉 = n_종가1 < n_저가3봉1
                b_매도_저가2봉 = n_종가1 < n_저가2봉1
                # b_매도 = b_매도_고가 or b_매도_저가3봉
                b_매도 = b_매도_저가2봉
                if not b_매도:
                    continue

                # 데이터 정의
                # n_매도가 = n_매수가 * 1.03 if b_매도_고가 else n_시가
                n_매도가 = n_시가
                dic_수익검증_매도 = {컬럼: df_수익검증_매수_보유.loc[idx, 컬럼] for 컬럼 in df_수익검증_매수_보유.columns}
                dic_수익검증_매도.update(일자=s_일자, 매도일=s_일자, 매도가=n_매도가)
                li_dic수익검증_매도.append(dic_수익검증_매도)
            df_수익검증_매도 = pd.DataFrame(li_dic수익검증_매도)

            # 수익검증 업데이트
            df_수익검증 = pd.concat([df_수익검증_매수, df_수익검증_매도], axis=0)
            if len(df_수익검증) > 0:
                df_수익검증 = df_수익검증.drop_duplicates(subset=['종목코드', '등장일자'], keep='last')
                df_수익검증 = df_수익검증.sort_values(['매수일', '종목코드']).reset_index(drop=True)
                df_수익검증['수익률'] = (df_수익검증['매도가'] / df_수익검증['매수가'] - 1) * 100

            # 데이터 저장
            Tool.df저장(df=df_수익검증, path=os.path.join(folder_타겟, f'{file_타겟}_{s_일자}'))

            # 로그 기록
            n_수익률 = df_수익검증['수익률'].sum() if len(df_수익검증) > 0 else 0
            self.make_로그(f'{s_일자} 완료 - {len(df_수익검증):,.0f} 종목, {n_수익률:,.1f}%')

    def verify_과거실적(self, n_m일수=5, n_p일수=1):
        """ 조회순위에 등장한 종목 중 추천종목에 포함된 종목 찾아서 저장 """
        # 기준정보 정의
        folder_소스 = self.folder_조회순위
        file_소스 = f'df_조회순위'
        folder_타겟 = os.path.join(self.folder_종목분석, '상승후보')
        file_타겟 = f'df_상승후보'
        os.makedirs(folder_타겟, exist_ok=True)

        # 대상일자 확인
        li_전체일자 = sorted(re.findall(r'\d{8}', 파일)[0] for 파일 in os.listdir(folder_소스) if '.csv' in 파일)
        li_완료일자 = [re.findall(r'\d{8}', 파일)[0] for 파일 in os.listdir(folder_타겟) if '.pkl' in 파일]
        li_대상일자 = [일자 for 일자 in li_전체일자 if 일자 not in li_완료일자 and 일자 != self.s_오늘]

        # 일자별 데이터 생성
        for s_일자 in li_대상일자:
            # 기준일자 확인
            folder_일봉캐시 = os.path.join(self.folder_차트캐시, '일봉1')
            li_기준일자 = sorted(re.findall(r'\d{8}', 파일)[0] for 파일 in os.listdir(folder_일봉캐시) if '.pkl' in 파일)
            idx_일자 = li_기준일자.index(s_일자)
            li_기준일자 = li_기준일자[(idx_일자 - n_m일수): (idx_일자 + n_p일수 + 1)]
            if len(li_기준일자) < n_m일수 + 1:
                continue

            # 소스파일 불러오기
            dic_li조회순위 = dict()
            for s_기준일 in li_기준일자:
                df_조회순위 = pd.read_csv(os.path.join(folder_소스, f'{file_소스}_{s_기준일}.csv'), encoding='cp949', dtype=str)
                dic_li조회순위[s_기준일] = list(df_조회순위['종목코드'].unique())

            # 추천종목 불러오기
            li_일자 = [re.findall(r'\d{8}', 파일)[0] for 파일 in os.listdir(self.folder_조건검색) if '.pkl' in 파일]
            s_기준일자_추천종목 = min(일자 for 일자 in li_일자 if 일자 >= s_일자) if max(li_일자) >= s_일자 else max(li_일자)
            df_조건검색 = pd.read_pickle(os.path.join(self.folder_조건검색, f'df_조건검색_{s_기준일자_추천종목}.pkl'))
            li_추천종목 = df_조건검색[df_조건검색['검색식명'] == '거북이추천']['종목코드'].tolist()

            # 일봉 불러오기 (n_p일수 기준)
            li_일봉파일 = sorted(파일 for 파일 in os.listdir(folder_일봉캐시)
                             if '.pkl' in 파일 and re.findall(r'\d{8}', 파일)[0] > s_일자)
            if len(li_일봉파일) < n_p일수:
                continue
            file_일봉캐시 = li_일봉파일[:n_p일수][-1]
            dic_일봉 = pd.read_pickle(os.path.join(folder_일봉캐시, file_일봉캐시))

            # 상승후보 데이터 생성
            li_조회순위 = dic_li조회순위[s_일자]
            li_상승후보 = [종목 for 종목 in li_조회순위 if 종목 in li_추천종목]
            li_dic상승후보 = list()
            for s_종목코드 in li_상승후보:
                # 기준정보 정의
                df_일봉 = dic_일봉[s_종목코드]
                sri_시가, sri_고가, sri_저가, sri_종가 = df_일봉['시가'], df_일봉['고가'], df_일봉['저가'], df_일봉['종가']
                li_기준일자p = [일자 for 일자 in li_기준일자 if 일자 >= s_일자]
                li_기준일자m = [일자 for 일자 in li_기준일자 if 일자 < s_일자]

                # 데이터 생성
                dic_상승후보 = dict(일자=s_일자, 종목코드=s_종목코드, 종목명=df_일봉.loc[s_일자, '종목명'])
                for i, s_일자m in enumerate(li_기준일자m):
                    dic_상승후보[f'존재m{len(li_기준일자m) - i}'] = 1 if s_종목코드 in dic_li조회순위[s_일자m] else 0
                dic_상승후보.update(시가=sri_시가[s_일자], 종가=sri_종가[s_일자])
                dic_상승후보.update(고가vs시가=(sri_고가[s_일자] / sri_시가[s_일자] - 1) * 100)
                dic_상승후보.update(저가vs시가=(sri_저가[s_일자] / sri_시가[s_일자] - 1) * 100)
                dic_상승후보.update(종가vs시가=(sri_종가[s_일자] / sri_시가[s_일자] - 1) * 100)
                dic_상승후보.update(시가p1=sri_시가[li_기준일자p[1]])
                for i, s_일자p in enumerate(li_기준일자p):
                    if i == 0:
                        continue
                    dic_상승후보[f'고가p{i}'] = sri_고가[s_일자p]
                    dic_상승후보[f'저가p{i}'] = sri_저가[s_일자p]
                    dic_상승후보[f'종가p{i}'] = sri_종가[s_일자p]

                # 데이터 등록
                li_dic상승후보.append(dic_상승후보)

            # 데이터 정리
            df_상승후보 = pd.DataFrame(li_dic상승후보) if len(li_dic상승후보) > 0 else pd.DataFrame()
            for 컬럼명 in [컬럼 for 컬럼 in df_상승후보.columns if 'p' in 컬럼]:
                df_상승후보[컬럼명] = (df_상승후보[컬럼명] / df_상승후보['종가'] - 1) * 100

            # 데이터 저장
            Tool.df저장(df=df_상승후보, path=os.path.join(folder_타겟, f'{file_타겟}_{s_일자}'))

            # 로그 기록
            self.make_로그(f'{s_일자} 완료 - {len(df_상승후보):,.0f} 종목')

    def detect_상승후보_누적(self):
        """ 산출된 상승후보 데이터를 일별 누적하여 하나의 파일로 저장 """
        # 기준정보 정의
        folder_소스 = os.path.join(self.folder_종목분석, '상승후보')
        file_소스 = f'df_상승후보'
        folder_타겟 = os.path.join(self.folder_종목분석, '상승후보_누적')
        file_타겟 = f'df_상승후보누적'
        os.makedirs(folder_타겟, exist_ok=True)

        # 대상일자 확인
        li_전체일자 = sorted(re.findall(r'\d{8}', 파일)[0] for 파일 in os.listdir(folder_소스) if '.pkl' in 파일)
        li_완료일자 = [re.findall(r'\d{8}', 파일)[0] for 파일 in os.listdir(folder_타겟) if '.pkl' in 파일]
        li_대상일자 = [일자 for 일자 in li_전체일자 if 일자 not in li_완료일자]

        # 일자별 데이터 생성
        for s_일자 in li_대상일자:
            # 소스파일 불러오기
            li_파일명 = sorted(파일 for 파일 in os.listdir(folder_소스)
                            if file_소스 in 파일 and '.pkl' in 파일 and re.findall(r'\d{8}', 파일)[0] <= s_일자)
            li_df상승후보 = [pd.read_pickle(os.path.join(folder_소스, 파일)) for 파일 in li_파일명]

            # 데이터 정리
            df_상승후보_누적 = pd.concat(li_df상승후보, axis=0).sort_values(['일자', '종목코드']).reset_index(drop=True)
            n_전체건수 = len(df_상승후보_누적)
            n_상승건수 = len(df_상승후보_누적[df_상승후보_누적['고가p1'] < df_상승후보_누적['시가p1']])
            n_수익률 = sum(df_상승후보_누적['고가p1'] - df_상승후보_누적['시가p1'])

            # 데이터 저장
            Tool.df저장(df=df_상승후보_누적, path=os.path.join(folder_타겟, f'{file_타겟}_{s_일자}'))

            # 로그 기록
            self.make_로그(f'{s_일자} 완료\n - 전체 {n_전체건수:,.0f}건, 상승 {n_상승건수:,.0f}건, 수익 {n_수익률:,.1f}%')


def run():
    """ 실행 함수 """
    a = AnalyzerBot(b_디버그모드=True, s_시작일자=None)
    a.find_상승후보()
    a.make_매수신호()
    a.verify_수익검증()
    # a.verify_과거실적()
    # a.detect_상승후보_누적()

if __name__ == '__main__':
    try:
        run()
    except KeyboardInterrupt:
        print('\n### [ KeyboardInterrupt detected ] ###')
