import os
import sys
import json
import time
import re
import multiprocessing as mp

from fontTools.varLib.models import nonNone

# win용 디버거 설정
if sys.platform == 'win32':
    import matplotlib
    matplotlib.use('TkAgg')

import pandas as pd
from fontTools.ttLib.tables.otTables import DeltaSetIndexMap
from tqdm import tqdm

import ut.로그maker, ut.폴더manager, ut.도구manager as Tool, ut.차트maker
import analyzer.logic_상승후보


# noinspection NonAsciiCharacters,SpellCheckingInspection,PyPep8Naming,PyTypeChecker
class AnalyzerBot:
    def __init__(self, b_디버그모드=False, s_시작일자=None):
        # config 읽어 오기
        self.folder_베이스 = os.path.dirname(os.path.abspath(__file__))
        self.folder_프로젝트 = os.path.dirname(self.folder_베이스)
        self.s_파일명 = os.path.basename(__file__).replace('.py', '')
        dic_config = Tool.config로딩()

        # 로그 설정
        log = ut.로그maker.LogMaker(s_파일명=self.s_파일명, s_로그명='로그이름_analyzer')
        sys.stderr = ut.로그maker.StderrHook(path_에러로그=log.path_에러)
        self.make_로그 = log.make_로그

        # 폴더 정의
        dic_폴더정보 = ut.폴더manager.define_폴더정보()
        self.folder_work = dic_폴더정보['folder_work']
        self.folder_차트캐시 = dic_폴더정보['데이터|차트캐시']
        self.folder_전체종목 = dic_폴더정보['데이터|전체종목']
        self.folder_대상종목 = dic_폴더정보['데이터|대상종목']
        self.folder_조건검색 = dic_폴더정보['데이터|조건검색']
        self.folder_조회순위 = dic_폴더정보['데이터|조회순위']
        self.folder_일봉분석 = dic_폴더정보['분석|일봉분석']
        os.makedirs(self.folder_일봉분석, exist_ok=True)

        # 시작일자 정의
        n_보관기간 = int(dic_config['파일보관기간(일)_analyzer'])
        s_보관일자 = (pd.Timestamp.now() - pd.DateOffset(days=n_보관기간)).strftime('%Y%m%d')
        self.s_시작일자 = s_시작일자 if s_시작일자 is not None else s_보관일자

        # 기준정보 정의
        self.s_오늘 = pd.Timestamp.now().strftime('%Y%m%d')
        self.b_디버그모드 = b_디버그모드
        self.n_멀티코어수 = mp.cpu_count() - 2
        self.dic_매개변수 = dict()

        # 서버정보 정의
        dic_서버정보 = json.load(open(os.path.join(self.folder_프로젝트, 'server_info.json'), mode='rt', encoding='utf-8'))
        self.dic_서버접속 = dic_서버정보.get('sftp')
        self.dic_서버폴더 = dic_서버정보.get('folder')

        # 차트maker 정의
        self.chart = ut.차트maker.ChartMaker()

        # 로그 기록
        self.make_로그(f'구동 시작')

    def sync_소스파일(self):
        """ 서버에 있는 소스파일을 로컬폴더로 동기화 """
        # 대상폴더 선정
        li_대상폴더 = [self.folder_차트캐시, self.folder_전체종목, self.folder_대상종목, self.folder_조회순위]

        # 폴더별 동기화
        li_동기화파일명 = list()
        for s_로컬폴더 in li_대상폴더:
            # 기준정보 정의
            s_서버폴더 = f'{self.dic_서버폴더['server_work']}{s_로컬폴더.replace(self.folder_work, '')}'
            s_서버폴더 = s_서버폴더.replace('\\', '/')

            # 파일 동기화
            li_동기화파일명_개별 = Tool.sftp폴더동기화(folder_로컬=s_로컬폴더, folder_서버=s_서버폴더, s_모드='서버2로컬',
                                          s_시작일자='20251001')
            li_동기화파일명 = li_동기화파일명 + li_동기화파일명_개별

        # 로그 기록
        s_동기화파일명 = ''.join(f'\n - {파일명}' for 파일명 in li_동기화파일명)
        self.make_로그(f'{len(li_동기화파일명):,.0f}개 파일 완료'
                      f'{s_동기화파일명}')

    def find_상승후보(self):
        """ 수집된 조회순위 데이터 기준으로 일봉차트 확인하여 대상종목 선정 """
        # 기준정보 정의
        folder_소스 = os.path.join(self.folder_차트캐시, f'일봉1')
        file_소스 = f'dic_차트캐시'
        folder_타겟 = os.path.join(self.folder_일봉분석, '10_상승후보')
        file_타겟 = f'df_상승후보'
        os.makedirs(folder_타겟, exist_ok=True)

        # 대상일자 확인
        li_전체일자 = sorted(re.findall(r'\d{8}', 파일)[0] for 파일 in os.listdir(folder_소스) if '.pkl' in 파일)
        li_전체일자 = [일자 for 일자 in li_전체일자 if 일자 >= self.s_시작일자]
        li_완료일자 = [re.findall(r'\d{8}', 파일)[0] for 파일 in os.listdir(folder_타겟) if '.pkl' in 파일]
        li_대상일자 = [일자 for 일자 in li_전체일자 if 일자 not in li_완료일자]

        # 일자별 데이터 생성
        for s_일자 in li_대상일자:
            # 데이터 생성
            df_상승후보 = analyzer.logic_상승후보.check_조회순위(s_일자=s_일자)
            df_후보만 = df_상승후보.loc[(df_상승후보['당일조건']) & (df_상승후보['당일바디'] > 0) & (df_상승후보['당일바디'] < 2)]\
                        if len(df_상승후보) > 0 else pd.DataFrame()

            # 데이터 저장
            Tool.df저장(df=df_상승후보, path=os.path.join(folder_타겟, f'{file_타겟}_{s_일자}'))

            # 로그 기록
            self.make_로그(f'{s_일자} 완료\n - 전체 {len(df_상승후보):,.0f}종목, 상승후보 {len(df_후보만):,.0f}종목')

    def make_매매정보(self, n_포함일수=5):
        """ 상승후보 종목 대상으로 일봉기준 매매신호 생성 후 저장 """
        # 기준정보 정의
        folder_소스 = os.path.join(self.folder_일봉분석, '10_상승후보')
        file_소스 = f'df_상승후보'
        folder_타겟 = os.path.join(self.folder_일봉분석, '20_매매정보')
        file_타겟 = f'df_매매정보'
        os.makedirs(folder_타겟, exist_ok=True)

        # 대상일자 확인
        li_전체일자 = sorted(re.findall(r'\d{8}', 파일)[0] for 파일 in os.listdir(folder_소스) if '.pkl' in 파일)
        li_완료일자 = [re.findall(r'\d{8}', 파일)[0] for 파일 in os.listdir(folder_타겟) if '.pkl' in 파일]
        li_대상일자 = [일자 for 일자 in li_전체일자 if 일자 not in li_완료일자]

        # 일자별 데이터 생성
        for s_일자 in li_대상일자:
            # 전일 확인
            li_일자 = [일자 for 일자 in li_전체일자 if 일자 < s_일자]
            if len(li_일자) == 0: continue
            s_전일 = max(li_일자)

            # 상승후보 불러오기 - 전일 기준
            df_상승후보 = pd.read_pickle(os.path.join(folder_소스, f'{file_소스}_{s_전일}.pkl'))
            df_후보만 = df_상승후보.loc[(df_상승후보['당일조건']) & (df_상승후보['당일바디'] > 0) & (df_상승후보['당일바디'] < 2)]
            li_상승후보 = df_후보만['종목코드'].tolist()

            # 일봉 불러오기 - 당일 기준
            dic_일봉 = pd.read_pickle(os.path.join(self.folder_차트캐시, '일봉1', f'dic_차트캐시_1일봉_{s_일자}.pkl'))

            # 이전 데이터 불러오기
            path_매매정보_전일 = os.path.join(folder_타겟, f'{file_타겟}_{s_전일}.pkl')
            df_매매정보_전일 = pd.read_pickle(path_매매정보_전일) if os.path.exists(path_매매정보_전일) else pd.DataFrame()
            # li_파일 = [파일 for 파일 in os.listdir(folder_타겟) if '.pkl' in 파일 and re.findall(r'\d{8}', 파일)[0] < s_일자]
            # df_매매정보_전일 = pd.read_pickle(os.path.join(folder_타겟, max(li_파일))) if len(li_파일) > 0 else pd.DataFrame()
            df_보유종목 = df_매매정보_전일.loc[pd.isna(df_매매정보_전일['매도일'])] if len(df_매매정보_전일) > 0 else pd.DataFrame()
            li_보유종목 = df_보유종목['종목코드'].tolist() if len(df_보유종목) > 0 else list()
            li_상승후보 = li_보유종목 + [종목 for 종목 in li_상승후보 if 종목 not in li_보유종목]

            # 종목별 매매정보 생성 - 시가매수, 종가매도, 손실시 최대 5일 보유
            li_dic매매정보 = list()
            for s_종목코드 in li_상승후보:
                # 기준정보 정의
                df_일봉 = dic_일봉[s_종목코드]
                dt_오늘 = df_일봉.index[-1]
                s_종목명 = df_일봉.loc[dt_오늘, '종목명']
                n_시가 = df_일봉.loc[dt_오늘, '시가']
                n_고가 = df_일봉.loc[dt_오늘, '고가']
                n_저가 = df_일봉.loc[dt_오늘, '저가']
                n_종가 = df_일봉.loc[dt_오늘, '종가']
                n_전일종가 = df_일봉.loc[dt_오늘, '전일종가']
                n_전일저가3봉 = min(df_일봉['저가'].values[-5:-2])

                # 보유종목 정보 불러오기
                b_보유종목 = s_종목코드 in df_보유종목['종목코드'].values if len(df_보유종목) > 0 else False
                df_보유종목_종목 = df_보유종목.loc[df_보유종목['종목코드'] == s_종목코드] if b_보유종목 else pd.DataFrame()
                s_보유종목_매수일 = df_보유종목_종목['매수일'].values[0] if b_보유종목 else None
                n_보유종목_매수가 = df_보유종목_종목['매수가'].values[0] if b_보유종목 else None
                n_보유종목_경과일 = df_보유종목_종목['경과일'].values[0] if b_보유종목 else None

                # 매수정보 생성
                # n_매수가 = n_시가 if not b_보유종목 else n_보유종목_매수가
                n_매수가 = n_전일종가 if not b_보유종목 else n_보유종목_매수가
                s_매수일 = s_일자 if not b_보유종목 else s_보유종목_매수일
                n_경과일 = 0 if not b_보유종목 else n_보유종목_경과일 + 1

                # 매도정보 생성
                # n_기준수익률 = 0 if b_보유종목 else 5
                n_일절기준 = 10
                n_손절기준 = -3
                n_수익률 = (n_종가 / n_매수가 - 1) * 100 - 0.2
                n_매도가 = n_종가 if (n_수익률 > n_일절기준) or (n_수익률 < n_손절기준) or (n_경과일 >= 5) else None
                # n_매도가 = n_종가 if n_수익률 > 0 or n_경과일 >= 2 else None
                s_매도일 = s_일자 if n_매도가 is not None else None

                # 손절정보 생성 - 보유종목 대상
                if b_보유종목 and (s_매도일 is None) and (n_전일종가 < n_전일저가3봉):
                    n_매도가 = n_시가
                    s_매도일 = s_일자
                    n_수익률 = (n_매도가 / n_매수가 - 1) * 100 - 0.2

                # 매매정보 정리
                dic_매매정보 = dict(일자=s_일자, 종목코드=s_종목코드, 종목명=s_종목명,
                                시가=n_시가, 고가=n_고가, 저가=n_저가, 종가=n_종가, 전일종가=n_전일종가,
                                매수일=s_매수일, 매도일=s_매도일, 경과일=n_경과일, 매수가=n_매수가, 매도가=n_매도가, 수익률=n_수익률)
                li_dic매매정보.append(dic_매매정보)

            # 데이터 정리
            df_매매정보 = pd.DataFrame(li_dic매매정보).sort_values(['매수일', '종목코드'])

            # 데이터 저장
            Tool.df저장(df=df_매매정보, path=os.path.join(folder_타겟, f'{file_타겟}_{s_일자}'))

            # 누적 데이터 생성
            li_파일 = sorted(파일 for 파일 in os.listdir(folder_타겟)
                            if '.pkl' in 파일 and re.findall(r'\d{8}', 파일)[0] <= s_일자)
            import warnings
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                df_매매정보_누적 = pd.concat([pd.read_pickle(os.path.join(folder_타겟, 파일) )for 파일 in li_파일], axis=0)\
                                if len(li_파일) > 0 else pd.DataFrame()
                df_매매정보_누적 = df_매매정보_누적.sort_values('일자')

            # 누적 데이터 저장
            folder_누적 = f'{folder_타겟}_누적'
            os.makedirs(folder_누적, exist_ok=True)
            Tool.df저장(df=df_매매정보_누적, path=os.path.join(folder_누적, f'{file_타겟}_누적_{s_일자}'))

            # 로그 기록
            n_매수종목 = len(df_매매정보.loc[df_매매정보['매수일'] == s_일자])
            n_매도종목 = len(df_매매정보.loc[df_매매정보['매도일'] == s_일자])
            n_잔여종목 = len(df_매매정보.loc[pd.isna(df_매매정보['매도일'])])
            n_수익 = df_매매정보.loc[df_매매정보['매도일'] == s_일자]['수익률'].sum()
            self.make_로그(f'{s_일자} 완료\n'
                         f' - 총 {len(df_매매정보):,.0f}건, 매수 {n_매수종목:,.0f}건, 매도 {n_매도종목:,.0f}건,'
                         f' 잔여 {n_잔여종목:,.0f}건, 수익 {n_수익:,.1f}%')

    def make_수익정보(self):
        """ 매매정보를 바탕으로 일별 수익정보 생성 """
        # 기준정보 정의
        folder_소스 = os.path.join(self.folder_일봉분석, '20_매매정보')
        file_소스 = f'df_매매정보'
        folder_타겟 = os.path.join(self.folder_일봉분석, '30_수익정보')
        file_타겟 = f'df_수익정보'
        os.makedirs(folder_타겟, exist_ok=True)

        # 대상일자 확인
        li_전체일자 = sorted(re.findall(r'\d{8}', 파일)[0] for 파일 in os.listdir(folder_소스) if '.pkl' in 파일)
        li_완료일자 = [re.findall(r'\d{8}', 파일)[0] for 파일 in os.listdir(folder_타겟) if '.pkl' in 파일]
        li_대상일자 = [일자 for 일자 in li_전체일자 if 일자 not in li_완료일자]

        # 일자별 데이터 생성
        for s_일자 in li_대상일자:
            # 소스파일 불러오기
            # li_파일 = sorted(파일 for 파일 in os.listdir(folder_소스)
            #                 if '.pkl' in 파일 and re.findall(r'\d{8}', 파일)[0] <= s_일자)
            # import warnings
            # with warnings.catch_warnings():
            #     warnings.simplefilter("ignore")
            #     df_매매정보_누적 = pd.concat([pd.read_pickle(os.path.join(folder_소스, 파일) )for 파일 in li_파일], axis=0)\
            #                     if len(li_파일) > 0 else pd.DataFrame()
            df_매매정보_누적 = pd.read_pickle(os.path.join(f'{folder_소스}_누적', f'{file_소스}_누적_{s_일자}.pkl'))
            df_매매정보_당일 = pd.read_pickle(os.path.join(folder_소스, f'{file_소스}_{s_일자}.pkl'))
            # df_매매정보 = pd.read_pickle(os.path.join(folder_소스, f'{file_소스}_{s_일자}.pkl'))
            li_정리일자 = ['Total'] + sorted(df_매매정보_누적['일자'].unique())

            # 수익정보 생성
            li_dic수익정보 = list()
            for s_정리일자 in li_정리일자:
                # 정리일자 데이터 분리
                df_매매정보 = df_매매정보_누적[df_매매정보_누적['일자'] == s_정리일자] if s_정리일자 != 'Total' else df_매매정보_누적

                # 수익정보 생성
                df_매매정보_매수 = df_매매정보.loc[df_매매정보['매수일'] == df_매매정보['일자']]
                df_매매정보_매도 = df_매매정보.loc[df_매매정보['매도일'] == df_매매정보['일자']]
                df_매매정보_잔여 = df_매매정보.loc[pd.isna(df_매매정보['매도일'])]
                dic_수익정보 = dict(일자=s_정리일자,
                                전체종목=len(df_매매정보),
                                매수종목=len(df_매매정보_매수),
                                매도종목=len(df_매매정보_매도),
                                잔여종목=len(df_매매정보_잔여))
                dic_수익정보.update(매도건수=len(df_매매정보_매도),
                                이익건수=len(df_매매정보_매도.loc[df_매매정보_매도['수익률'] > 0]),
                                손실건수=len(df_매매정보_매도.loc[df_매매정보_매도['수익률'] <= 0]),
                                총이익률=df_매매정보_매도.loc[df_매매정보_매도['수익률'] > 0]['수익률'].sum(),
                                총손실률=df_매매정보_매도.loc[df_매매정보_매도['수익률'] <= 0]['수익률'].sum())
                dic_수익정보.update(수익률=df_매매정보_매도['수익률'].sum(),
                                수익률0일=df_매매정보_매도.loc[df_매매정보_매도['경과일'] == 0, '수익률'].sum(),
                                수익률1일=df_매매정보_매도.loc[df_매매정보_매도['경과일'] == 1, '수익률'].sum(),
                                수익률2일=df_매매정보_매도.loc[df_매매정보_매도['경과일'] == 2, '수익률'].sum(),
                                수익률3일=df_매매정보_매도.loc[df_매매정보_매도['경과일'] == 3, '수익률'].sum(),
                                수익률4일=df_매매정보_매도.loc[df_매매정보_매도['경과일'] == 4, '수익률'].sum(),
                                수익률5일=df_매매정보_매도.loc[df_매매정보_매도['경과일'] == 5, '수익률'].sum(),
                                잔여종목수익률=df_매매정보_잔여['수익률'].sum())
                dic_수익정보.update(승률=dic_수익정보['이익건수'] / dic_수익정보['매도건수'] if dic_수익정보['매도건수'] != 0 else 0,
                                평균이익=dic_수익정보['총이익률'] / dic_수익정보['이익건수'] if dic_수익정보['이익건수'] != 0 else 0,
                                평균손실=dic_수익정보['총손실률'] / dic_수익정보['손실건수'] if dic_수익정보['손실건수'] != 0 else 0)
                dic_수익정보.update(기대수익=(dic_수익정보['승률'] * dic_수익정보['평균이익'] / abs(dic_수익정보['평균손실']))
                                            - (1 - dic_수익정보['승률']) if dic_수익정보['평균손실'] != 0 else 0)

                # 데이터 추가
                li_dic수익정보.append(dic_수익정보)

            # 데이터 정리
            df_수익정보 = pd.DataFrame(li_dic수익정보)

            # 데이터 저장
            df_수익정보 = df_수익정보.sort_values('일자', ascending=False)
            Tool.df저장(df=df_수익정보, path=os.path.join(folder_타겟, f'{file_타겟}_{s_일자}'))

            # 수익금액 생성
            df_수익금액_누적 = self._make_수익정보_수익금액(df_매매정보=df_매매정보_누적, n_초기자본=1 * 10 ** 7)

            # 수익금액 저장
            folder_수익금액 = f'{folder_타겟}_수익금액'
            os.makedirs(folder_수익금액, exist_ok=True)
            Tool.df저장(df=df_수익금액_누적, path=os.path.join(folder_수익금액, f'매매정보_수익금액_{s_일자}'))

            # 로그 기록
            n_당일수익 = df_매매정보_당일['수익률'].sum() if len(df_매매정보_당일) > 0 else 0
            n_누적수익 = df_수익금액_누적['누적수익률'].values[-1] if len(df_수익금액_누적) > 0 else 0
            n_기대수익 = df_수익정보.loc[df_수익정보['일자'] == 'Total', '기대수익'].values[0]
            self.make_로그(f'{s_일자} 완료\n'
                         f' - 당일수익 {n_당일수익:,.1f}%, 누적수익 {n_누적수익:,.1f}%, 기대수익 {n_기대수익:,.2f}')

    @staticmethod
    def _make_수익정보_수익금액(df_매매정보, n_초기자본):
        """ 매매정보 기준으로 수익금액 생성 후 리터 """
        # 기준정보 정의
        df_매매정보 = df_매매정보.sort_values(['일자', '경과일']).reset_index(drop=True)
        df_매매정보_매수 = df_매매정보[df_매매정보['경과일'] == 0]
        df_매매정보_매도 = df_매매정보[pd.notna(df_매매정보['매도일'])]
        li_대상일자 = sorted(df_매매정보['일자'].unique())

        # 일별 진행
        s_일자 = None
        n_일매수한도 = None
        dic_매매수량 = dict()
        dic_수익금액 = dict()
        li_dic수익금액 = list()
        for idx in df_매매정보.index:
            # 기준정보 생성
            n_예수금 = dic_수익금액.get('예수금', n_초기자본)
            n_일매수한도 = min(4000000, n_예수금) if s_일자 != df_매매정보.loc[idx, '일자'] else n_일매수한도
            s_일자 = df_매매정보.loc[idx, '일자']
            s_종목코드 = df_매매정보.loc[idx, '종목코드']
            s_매수일 = df_매매정보.loc[idx, '매수일']
            n_경과일 = df_매매정보.loc[idx, '경과일']
            n_시작금액 = dic_수익금액.get('종료금액', n_초기자본)
            df_매매정보_일별 = df_매매정보[df_매매정보['일자'] == s_일자]
            n_일매수건수 = len(df_매매정보_일별.loc[df_매매정보_일별['경과일'] == 0])
            # n_종목당매수한도 = int(n_시작금액 / n_일매수건수)
            n_종목당매수한도 = int(n_일매수한도 / n_일매수건수)
            # n_종료금액 = n_시작금액
            b_매수실행 = df_매매정보_일별.loc[idx, '경과일'] == 0
            b_매도실행 = df_매매정보_일별.loc[idx, '매도일'] is not None
            n_매수단가 = int(df_매매정보.loc[idx, '매수가'])
            n_매매수량 = int(n_종목당매수한도 / n_매수단가) if n_경과일 == 0 else dic_매매수량[s_매수일][s_종목코드]
            # n_매수금액 = n_매수단가 * n_매매수량 if b_매수실행 else 0
            n_매수금액 = n_매수단가 * n_매매수량
            n_매도단가 = int(df_매매정보.loc[idx, '매도가']) if b_매도실행 else None
            n_매도금액 = n_매도단가 * n_매매수량 if b_매도실행 else 0
            n_수익금액 = n_매도금액 - n_매수금액 if b_매도실행 else 0
            # n_종료금액 = n_시작금액 - n_매수금액 + n_매도금액
            n_종료금액 = n_시작금액 + n_수익금액
            n_누적수익률 = (n_종료금액 / n_초기자본 - 1) * 100
            n_예수금 = n_예수금 - n_매수금액 if b_매수실행 and n_경과일 == 0 else n_예수금
            n_예수금 = n_예수금 + n_매도금액 if b_매도실행 else n_예수금
            # n_예수금 = n_예수금 + n_매도금액 if n_경과일 > 0 and n_수익금액 != 0 else n_예수금 - n_매수금액 + n_매도금액

            # 매매수량 보존
            dic_매매수량[s_매수일] = dict() if s_매수일 not in dic_매매수량 else dic_매매수량[s_매수일]
            dic_매매수량[s_매수일][s_종목코드] = n_매매수량

            # 데이터 생성
            dic_수익금액 = {컬럼 : df_매매정보.loc[idx, 컬럼] for 컬럼 in df_매매정보.columns}
            dic_수익금액.update(일매수한도=n_일매수한도, 일매수건수=n_일매수건수, 종목당매수한도=n_종목당매수한도,
                            매수단가=n_매수단가, 매도단가=n_매도단가, 매매수량=n_매매수량, 매수금액=n_매수금액, 매도금액=n_매도금액,
                            시작금액=n_시작금액, 수익금액=n_수익금액, 종료금액=n_종료금액, 누적수익률=n_누적수익률, 예수금=n_예수금)

            # 매수할 때의 매매수량을 그대로 가져와야 함

            # 데이터 추가
            li_dic수익금액.append(dic_수익금액)

        # 데이터 정리
        df_수익금액 = pd.DataFrame(li_dic수익금액)

        return df_수익금액


def run():
    """ 실행 함수 """
    a = AnalyzerBot(b_디버그모드=True, s_시작일자=None)
    # a.sync_소스파일()
    a.find_상승후보()
    a.make_매매정보()
    a.make_수익정보()

if __name__ == '__main__':
    try:
        run()
    except KeyboardInterrupt:
        print('\n### [ KeyboardInterrupt detected ] ###')
