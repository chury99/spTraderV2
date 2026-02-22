import os
import sys
import json
import re
import multiprocessing as mp

from fontTools.varLib.models import nonNone

# win용 디버거 설정
if sys.platform == 'win32':
    import matplotlib
    matplotlib.use('TkAgg')

import pandas as pd
from tqdm import tqdm
import matplotlib.pyplot as plt
from pandas.core.methods.selectn import SelectNSeries

import ut.로그maker, ut.폴더manager, ut.도구manager as Tool, ut.차트maker as Chart
import analyzer.logic_매수매도 as Logic


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
        self.folder_조회순위 = dic_폴더정보['데이터|조회순위']
        self.folder_백테스팅 = dic_폴더정보['분석|백테스팅']
        os.makedirs(self.folder_백테스팅, exist_ok=True)

        # 기준정보 정의
        self.s_오늘 = pd.Timestamp.now().strftime('%Y%m%d')
        self.s_시작일자 = '20260101' if s_시작일자 is None else s_시작일자
        self.b_디버그모드 = b_디버그모드
        self.n_멀티코어수 = mp.cpu_count() - 3
        self.dic_args = dict()
        self.li_매도사유 = ['수익달성', '추세이탈', '손실한계', '타임아웃']

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

    def find_일봉확인(self):
        """ 초봉 데이터 수집된 종목 기준으로 일봉차트 확인 """
        # 기준정보 정의
        folder_소스 = os.path.join(self.folder_차트캐시, f'초봉1')
        file_소스 = f'dic_차트캐시'
        folder_타겟 = os.path.join(self.folder_백테스팅, '10_일봉확인')
        file_타겟 = f'df_일봉확인'
        os.makedirs(folder_타겟, exist_ok=True)

        # 대상일자 확인
        li_전체일자 = sorted(re.findall(r'\d{8}', 파일)[0] for 파일 in os.listdir(folder_소스)
                         if f'{file_소스}_1초봉' in 파일 and '.pkl' in 파일)
        # li_전체일자 = li_전체일자[-1:]
        li_완료일자 = [re.findall(r'\d{8}', 파일)[0] for 파일 in os.listdir(folder_타겟)
                   if file_타겟 in 파일 and '.pkl' in 파일]
        li_대상일자 = [일자 for 일자 in li_전체일자 if 일자 not in li_완료일자]

        # 일자별 매수매도 정보 생성
        for s_일자 in li_대상일자:
            # 소스파일 불러오기
            dic_초봉 = pd.read_pickle(os.path.join(folder_소스, f'{file_소스}_1초봉_{s_일자}.pkl'))
            li_대상종목_초봉 = sorted(dic_초봉.keys())

            # 추가 파일 불러오기
            dic_일봉 = pd.read_pickle(os.path.join(self.folder_차트캐시, '일봉1', f'dic_차트캐시_1일봉_{s_일자}.pkl'))

            # 종목별 조건 확인
            li_dic일봉확인 = list()
            for s_종목코드 in li_대상종목_초봉:
                # 기준정보 정의
                df_일봉 = dic_일봉[s_종목코드]
                df_일봉['전일고가3봉'] = df_일봉['고가'].shift(1).rolling(window=3).max()
                df_일봉['신호_고가3봉'] = df_일봉['종가'] > df_일봉['전일고가3봉']
                df_일봉['전일고가20봉'] = df_일봉['고가'].shift(1).rolling(window=20).max()
                df_일봉['신호_고가20봉'] = df_일봉['종가'] > df_일봉['전일고가20봉']
                if len(df_일봉) < 2: continue
                dt_전일 = df_일봉.index[-2]
                n_전일종가 = df_일봉.loc[dt_전일, '종가']
                n_전일60 = df_일봉.loc[dt_전일, '종가ma60']
                n_전일120 = df_일봉.loc[dt_전일, '종가ma120']
                n_전일바디 = (n_전일종가 - df_일봉.loc[dt_전일, '시가']) / df_일봉.loc[dt_전일, '전일종가'] * 100

                # 조건 확인 - 전일 기준
                li_조건확인 = list()
                li_조건확인.append(True if n_전일종가 > n_전일60 > n_전일120 else False)
                li_조건확인.append(True if sum(df_일봉['신호_고가3봉'].values[-6:-1]) > 0 else False)
                li_조건확인.append(True if df_일봉['신호_고가20봉'].values[-2] == True else False)

                # 결과 생성
                dic_일봉확인 = df_일봉.iloc[-1].to_dict()
                dic_일봉확인.update(전일종가=n_전일종가, 전일60=n_전일60, 전일120=n_전일120, 전일바디=n_전일바디,
                                전일조건=sum(li_조건확인)==len(li_조건확인),
                                전일정배열=li_조건확인[0], 전일추세5일=li_조건확인[1], 전일추세20봉=li_조건확인[2])
                li_dic일봉확인.append(dic_일봉확인)

            # df 생성
            df_일봉확인 = pd.DataFrame(li_dic일봉확인) if len(li_dic일봉확인) > 0 else pd.DataFrame()

            # 결과파일 저장
            Tool.df저장(df=df_일봉확인, path=os.path.join(folder_타겟, f'{file_타겟}_{s_일자}'))

            # 로그 기록
            self.make_로그(f'{s_일자} 완료\n'
                         f' - 전체 {len(df_일봉확인):,.0f}종목, 전일조건 {df_일봉확인['전일조건'].sum():,.0f}종목')

    def make_매매신호(self, n_봉수):
        """ 초봉 데이터 기준 매수/매도 신호 생성 """
        # 기준정보 정의
        folder_소스 = os.path.join(self.folder_백테스팅, '10_일봉확인')
        file_소스 = f'df_일봉확인'
        folder_타겟 = os.path.join(self.folder_백테스팅, '20_매매신호')
        file_타겟 = f'dic_매매신호'
        os.makedirs(folder_타겟, exist_ok=True)

        # 대상일자 확인
        li_전체일자 = sorted(re.findall(r'\d{8}', 파일)[0] for 파일 in os.listdir(folder_소스)
                        if file_소스 in 파일 and '.pkl' in 파일)
        li_완료일자 = [re.findall(r'\d{8}', 파일)[0] for 파일 in os.listdir(folder_타겟)
                        if file_타겟 in 파일 and f'{n_봉수}초봉'in 파일]
        li_대상일자 = [일자 for 일자 in li_전체일자 if 일자 not in li_완료일자 and 일자 >= self.s_시작일자]
        # li_대상일자 = li_대상일자[:3]

        # 일자별 매수매도 정보 생성
        for s_일자 in li_대상일자:
            # 소스파일 불러오기
            df_일봉확인 = pd.read_pickle(os.path.join(folder_소스, f'{file_소스}_{s_일자}.pkl'))
            dic_코드2종목 = df_일봉확인.set_index(['종목코드'])['종목명'].to_dict()
            li_일봉조건 = df_일봉확인.loc[df_일봉확인['전일조건'], '종목코드'].tolist()

            # 분석 대상종목 불러오기
            path_분석대상 = os.path.join(self.folder_대상종목, f'df_대상종목_{s_일자}.pkl')
            df_분석대상 = pd.read_pickle(path_분석대상) if os.path.exists(path_분석대상) else pd.DataFrame()
            li_대상종목 = [종목 for 종목 in li_일봉조건 if 종목 in df_분석대상['종목코드'].values]\
                            if len(df_분석대상) > 0 else li_일봉조건

            # 초봉캐시 불러오기
            folder_초봉 = os.path.join(self.folder_차트캐시, f'초봉{n_봉수}')
            dic_초봉 = pd.read_pickle(os.path.join(folder_초봉, f'dic_차트캐시_{n_봉수}초봉_{s_일자}.pkl'))

            # 1초봉 불러오기
            dic_1초봉 = pd.read_pickle(os.path.join(self.folder_차트캐시, '초봉1', f'dic_차트캐시_1초봉_{s_일자}.pkl'))\
                        if n_봉수 > 1 else dic_초봉

            # 일봉시가 생성
            dic_일봉 = pd.read_pickle(os.path.join(self.folder_차트캐시, '일봉1', f'dic_차트캐시_1일봉_{s_일자}.pkl'))
            dic_일봉시가 = {종목코드 : dic_일봉[종목코드]['시가'].values[-1] for 종목코드 in li_대상종목}

            # 매개변수 정의 - 종목별 함수 전달용
            li_매개변수 = [dict(s_종목코드=s_종목코드, s_종목명=dic_코드2종목[s_종목코드], n_봉수=n_봉수, s_일자=s_일자,
                            folder_타겟=folder_타겟, file_타겟=file_타겟, li_매도사유=self.li_매도사유, dic_일봉시가=dic_일봉시가,
                            df_초봉=dic_초봉.get(s_종목코드, pd.DataFrame()),
                            df_1초봉=dic_1초봉.get(s_종목코드, pd.DataFrame()))
                       for s_종목코드 in li_대상종목]

            # 종목별 매수매도 정보 생성
            li_df매매신호 = list()
            if self.b_디버그모드:
                for dic_매개변수 in tqdm(li_매개변수, desc=f'매매신호-{n_봉수}초봉-{s_일자}', file=sys.stdout):
                    li_df매매신호.append(self._make_매매신호_종목(dic_매개변수=dic_매개변수))
            else:
                with mp.Pool(processes=self.n_멀티코어수) as pool:
                    li_df매매신호 = list(tqdm(pool.imap_unordered(self._make_매매신호_종목, li_매개변수),
                                          total=len(li_매개변수), desc=f'매매신호-{n_봉수}초봉-{s_일자}', file=sys.stdout))
            dic_매매신호 = dict(li_df매매신호)

            # 결과파일 저장
            pd.to_pickle(dic_매매신호, os.path.join(folder_타겟, f'{file_타겟}_{s_일자}_{n_봉수}초봉.pkl'))

            # 로그 기록
            self.make_로그(f'{s_일자} 완료\n - {len(dic_매매신호):,.0f}개 종목')

    def make_매수매도(self, n_봉수):
        """ 매수/매도 신호 기준으로 보유시점의 데이터만 정리 """
        # 기준정보 정의
        folder_소스 = os.path.join(self.folder_백테스팅, '20_매매신호')
        file_소스 = f'dic_매매신호'
        folder_타겟 = os.path.join(self.folder_백테스팅, '30_매수매도')
        file_타겟 = f'df_매수매도'
        os.makedirs(folder_타겟, exist_ok=True)

        # 대상일자 확인
        li_전체일자 = sorted(re.findall(r'\d{8}', 파일)[0] for 파일 in os.listdir(folder_소스)
                        if file_소스 in 파일 and f'{n_봉수}초봉' in 파일)
        li_완료일자 = [re.findall(r'\d{8}', 파일)[0] for 파일 in os.listdir(folder_타겟)
                        if file_타겟 in 파일 and f'{n_봉수}초봉'in 파일 and '.pkl' in 파일]
        li_대상일자 = [일자 for 일자 in li_전체일자 if 일자 not in li_완료일자]

        # 일자별 매수매도 정보 생성
        for s_일자 in li_대상일자:
            # 소스파일 불러오기
            dic_매매신호 = pd.read_pickle(os.path.join(folder_소스, f'{file_소스}_{s_일자}_{n_봉수}초봉.pkl'))

            # 매수에서 매도까지 데이터 골라내기
            li_df매매신호 = [df.replace({None: ''}) for df in dic_매매신호.values()]
            df_매매신호_통합 = pd.concat(li_df매매신호, axis=0)
            df_매수매도 = df_매매신호_통합.loc[df_매매신호_통합['보유신호'] == True, :].copy().reset_index(drop=True)

            # 추가 데이터 생성
            df_매수매도['보유초'] = (pd.to_timedelta(df_매수매도['매도시간'])
                                - pd.to_timedelta(df_매수매도['매수시간'])).dt.total_seconds()

            # 결과파일 저장
            Tool.df저장(df=df_매수매도, path=os.path.join(folder_타겟, f'{file_타겟}_{s_일자}_{n_봉수}초봉'))

            # 로그 기록
            n_거래종목수 = len(df_매수매도['종목코드'].unique())
            n_매수신호 = df_매수매도['매수신호'].sum()
            n_매도신호 = df_매수매도['매도신호'].sum()
            self.make_로그(f'{s_일자} 완료\n - {n_거래종목수:,.0f}종목, 매수 {n_매수신호:,.0f}건, 매도 {n_매도신호:,.0f}건')

    def make_매매내역(self, n_봉수):
        """ 매수/매도 정보를 한줄로 정리 """
        # 기준정보 정의
        folder_소스 = os.path.join(self.folder_백테스팅, '30_매수매도')
        file_소스 = f'df_매수매도'
        folder_타겟 = os.path.join(self.folder_백테스팅, '40_매매내역')
        file_타겟 = f'df_매매내역'
        os.makedirs(folder_타겟, exist_ok=True)

        # 대상일자 확인
        li_전체일자 = sorted(re.findall(r'\d{8}', 파일)[0] for 파일 in os.listdir(folder_소스)
                        if file_소스 in 파일 and f'{n_봉수}초봉' in 파일 and '.pkl' in 파일)
        li_완료일자 = [re.findall(r'\d{8}', 파일)[0] for 파일 in os.listdir(folder_타겟)
                        if file_타겟 in 파일 and f'{n_봉수}초봉'in 파일 and '.pkl' in 파일]
        li_대상일자 = [일자 for 일자 in li_전체일자 if 일자 not in li_완료일자]

        # 일자별 매수매도 정보 생성
        for s_일자 in li_대상일자:
            # 소스파일 불러오기
            df_매수매도 = pd.read_pickle(os.path.join(folder_소스, f'{file_소스}_{s_일자}_{n_봉수}초봉.pkl'))

            # 매매내역만 골라내기
            df_매매내역 = df_매수매도.loc[df_매수매도['매도신호'] == True, :].copy().reset_index(drop=True)
            df_매매내역 = df_매매내역.astype({'매수가': int, '매도가': int, '보유초': int})

            # 결과파일 저장
            Tool.df저장(df=df_매매내역, path=os.path.join(folder_타겟, f'{file_타겟}_{s_일자}_{n_봉수}초봉'))

            # 리포트 생성 및 저장 - 일별
            df_일별리포트 = self._generate_일별리포트(df_매매내역=df_매매내역)
            folder_일별리포트 = f'{folder_타겟}_일별리포트'
            os.makedirs(folder_일별리포트, exist_ok=True)
            Tool.df저장(df=df_일별리포트, path=os.path.join(folder_일별리포트, f'{file_타겟}_일별리포트_{s_일자}_{n_봉수}초봉'))

            # 리포트 생성 - 누적, 수익금액
            dic_매개변수=dict(s_일자=s_일자, n_봉수=n_봉수, folder_타겟=folder_타겟, file_타겟=file_타겟)
            df_수익금액, df_누적리포트 = self._generate_누적리포트(dic_매개변수=dic_매개변수)

            # 수익금액 저장
            folder_수익금액 = f'{folder_타겟}_수익금액'
            os.makedirs(folder_수익금액, exist_ok=True)
            Tool.df저장(df=df_수익금액, path=os.path.join(folder_수익금액, f'{file_타겟}_수익금액_{s_일자}_{n_봉수}초봉'))

            # 누적 리포트 저장
            folder_누적리포트 = f'{folder_타겟}_누적리포트'
            os.makedirs(folder_누적리포트, exist_ok=True)
            Tool.df저장(df=df_누적리포트, path=os.path.join(folder_누적리포트, f'{file_타겟}_누적리포트_{s_일자}_{n_봉수}초봉'))

            # 로그 기록
            n_거래종목수 = len(df_매매내역['종목코드'].unique())
            n_거래건수 = len(df_매매내역)
            self.make_로그(f'{s_일자} 완료\n - {n_거래종목수:,.0f}종목, 거래 {n_거래건수:,.0f}건')

    def make_수익내역(self, n_봉수):
        """ 시간 흐름에 따라 매수/매도 시뮬레이션하여 중복 매매 제거 후 실제 수익 검증 """
        # 기준정보 정의
        folder_소스 = os.path.join(self.folder_백테스팅, '40_매매내역')
        file_소스 = f'df_매매내역'
        folder_타겟 = os.path.join(self.folder_백테스팅, '50_수익내역')
        file_타겟 = f'df_수익내역'
        os.makedirs(folder_타겟, exist_ok=True)

        # 대상일자 확인
        li_전체일자 = sorted(re.findall(r'\d{8}', 파일)[0] for 파일 in os.listdir(folder_소스)
                        if file_소스 in 파일 and f'{n_봉수}초봉' in 파일 and '.pkl' in 파일)
        li_완료일자 = [re.findall(r'\d{8}', 파일)[0] for 파일 in os.listdir(folder_타겟)
                        if file_타겟 in 파일 and f'{n_봉수}초봉'in 파일 and '.pkl' in 파일]
        li_대상일자 = [일자 for 일자 in li_전체일자 if 일자 not in li_완료일자]

        # 일자별 매수매도 정보 생성
        for s_일자 in li_대상일자:
            # 소스파일 불러오기
            df_매매내역 = pd.read_pickle(os.path.join(folder_소스, f'{file_소스}_{s_일자}_{n_봉수}초봉.pkl'))
            gr_매매내역 = df_매매내역.groupby('매수시간')

            # 조회순위 불러오기
            df_조회순위 = pd.read_csv(os.path.join(self.folder_조회순위, f'df_조회순위_{s_일자}.csv'),
                                  encoding='cp949', dtype=str)

            # 시간별 검증
            li_df수익내역 = list()
            s_매도시간 = '00:00:00'
            for s_매수시간, df_매매내역_시점 in gr_매매내역:
                # 매도시간 확인
                if s_매수시간 < s_매도시간:
                    continue

                # 감시종목 확인 - 조회순위 기준
                gr_조회순위_시점 = df_조회순위.loc[df_조회순위['시간'] <= s_매수시간, :].copy().groupby('시간')
                li_감시종목 = list()
                for s_시간, df_조회순위_시점 in gr_조회순위_시점:
                    df_조회순위_시점 = df_조회순위_시점.sort_values('빅데이터순위')
                    li_감시종목_조회 = df_조회순위_시점['종목코드'].tolist()
                    li_감시종목_이전 = [종목 for 종목 in li_감시종목 if 종목 not in li_감시종목_조회]
                    li_감시종목_전체 = li_감시종목_조회 + li_감시종목_이전
                    li_감시종목 = li_감시종목_전체[:100]

                # 매수종목 선정
                li_매수종목 = [종목 for 종목 in li_감시종목 if 종목 in df_매매내역_시점['종목코드'].values]
                s_매수종목 = li_매수종목[0] if len(li_매수종목) > 0 else None
                df_매매내역_시점_종목 = df_매매내역_시점.loc[df_매매내역_시점['종목코드'] == s_매수종목, :]
                li_df수익내역.append(df_매매내역_시점_종목)

                # 매도시간 업데이트
                s_매도시간 = df_매매내역_시점_종목['매도시간'].values[0] if not df_매매내역_시점_종목.empty else s_매도시간

            # 수익정리 생성
            df_수익내역 = pd.concat(li_df수익내역, axis=0) if len(li_df수익내역) > 0 else pd.DataFrame()

            # 결과파일 저장
            Tool.df저장(df=df_수익내역, path=os.path.join(folder_타겟, f'{file_타겟}_{s_일자}_{n_봉수}초봉'))

            # 리포트 생성 및 저장 - 일별
            df_일별리포트 = self._generate_일별리포트(df_매매내역=df_수익내역)
            folder_일별리포트 = f'{folder_타겟}_일별리포트'
            os.makedirs(folder_일별리포트, exist_ok=True)
            Tool.df저장(df=df_일별리포트, path=os.path.join(folder_일별리포트, f'{file_타겟}_일별리포트_{s_일자}_{n_봉수}초봉'))

            # 리포트 생성 - 누적, 수익금액
            dic_매개변수=dict(s_일자=s_일자, n_봉수=n_봉수, folder_타겟=folder_타겟, file_타겟=file_타겟)
            df_수익금액, df_누적리포트 = self._generate_누적리포트(dic_매개변수=dic_매개변수)

            # 수익금액 저장
            folder_수익금액 = f'{folder_타겟}_수익금액'
            os.makedirs(folder_수익금액, exist_ok=True)
            Tool.df저장(df=df_수익금액, path=os.path.join(folder_수익금액, f'{file_타겟}_수익금액_{s_일자}_{n_봉수}초봉'))

            # 누적 리포트 저장
            folder_누적리포트 = f'{folder_타겟}_누적리포트'
            os.makedirs(folder_누적리포트, exist_ok=True)
            Tool.df저장(df=df_누적리포트, path=os.path.join(folder_누적리포트, f'{file_타겟}_누적리포트_{s_일자}_{n_봉수}초봉'))

            # 로그 기록
            n_일수 = len(df_누적리포트) - 1
            n_일평균거래 = df_누적리포트['거래수'].values[0] / n_일수 if n_일수 > 0 else 0
            n_총거래수 = df_누적리포트['거래수'].values[0]
            n_일평균수익률 = df_누적리포트['수익률sum'].values[0] / n_일수 if n_일수 > 0 else 0
            n_기대수익 = df_누적리포트['기대수익'].values[0]
            n_수익금액 = df_누적리포트['수익금액'].values[0]
            n_금액수익률 = n_수익금액 / df_누적리포트['시작금액'].values[0] * 100
            self.make_로그(f'{s_일자} 완료\n'
                          f' - 총거래수 {n_총거래수:,.0f}건, 평균수익 {n_일평균수익률:,.1f}%, 기대수익 {n_기대수익:,.2f}\n'
                          f' - 수익금액 {n_수익금액:,.0f}원 ({n_금액수익률:,.1f}%)')

    def make_매매일보(self, n_봉수):
        """ 매매결과를 보고서 형태로 정리하여 저장 """
        # 기준정보 정의
        folder_소스 = os.path.join(self.folder_백테스팅, '50_수익내역')
        file_소스 = f'df_수익내역'
        folder_타겟 = os.path.join(self.folder_백테스팅, '60_매매일보')
        file_타겟 = f'매매일보'
        os.makedirs(folder_타겟, exist_ok=True)

        # 대상일자 확인
        li_전체일자 = sorted(re.findall(r'\d{8}', 파일)[0] for 파일 in os.listdir(folder_소스)
                        if file_소스 in 파일 and f'{n_봉수}초봉' in 파일 and '.pkl' in 파일)
        li_완료일자 = [re.findall(r'\d{8}', 파일)[0] for 파일 in os.listdir(folder_타겟)
                        if file_타겟 in 파일 and f'{n_봉수}초봉'in 파일 and '.svg' in 파일]
        li_대상일자 = [일자 for 일자 in li_전체일자 if 일자 not in li_완료일자]

        # 일자별 매수매도 정보 생성
        for s_일자 in li_대상일자:
            # 소스파일 불러오기
            dic_수익내역 = dict()
            for s_파일종류 in ['기본', '일별리포트', '수익금액', '누적리포트']:
                folder = f'{folder_소스}_{s_파일종류}' if s_파일종류 != '기본' else folder_소스
                file = f'{file_소스}_{s_파일종류}' if s_파일종류 != '기본' else file_소스
                dic_수익내역[s_파일종류] = pd.read_pickle(os.path.join(folder, f'{file}_{s_일자}_{n_봉수}초봉.pkl'))

            # 차트캐시 불러오기
            dic_일봉 = pd.read_pickle(os.path.join(self.folder_차트캐시, '일봉1', f'dic_차트캐시_1일봉_{s_일자}.pkl'))
            dic_분봉 = pd.read_pickle(os.path.join(self.folder_차트캐시, '분봉3', f'dic_차트캐시_3분봉_{s_일자}.pkl'))
            dic_초봉 = pd.read_pickle(os.path.join(self.folder_차트캐시, '초봉5', f'dic_차트캐시_5초봉_{s_일자}.pkl'))

            # 매매일보 생성
            df_일거래 = dic_수익내역['기본'].reset_index(drop=True)
            n_차트_가로 = 3
            n_차트_세로 = 1 + len(df_일거래)
            fig = plt.Figure(figsize=(16, n_차트_세로 * 3), tight_layout=False)

            # 기본요약 구성
            ax_승률손익비 = fig.add_subplot(n_차트_세로, n_차트_가로, 1)
            ax_기대수익 = fig.add_subplot(n_차트_세로, n_차트_가로, 2)
            ax_수익금액 = fig.add_subplot(n_차트_세로, n_차트_가로, 3)
            n_누적승률, n_누적손익비, n_기대수익 = self._ax_기대수익(ax_승률손익비=ax_승률손익비, ax_기대수익=ax_기대수익,
                                                            dic_데이터=dic_수익내역, n_이동평균=5)
            n_누적수익률 = self._ax_누적수익률(ax_누적수익률=ax_수익금액, dic_데이터=dic_수익내역)

            # 거래차트 구성
            for i in range(len(df_일거래)):
                # 기준정보 정의
                dic_거래 = df_일거래.loc[i].to_dict()
                s_종목코드 = df_일거래.loc[i, '종목코드']
                df_일봉 = dic_일봉[s_종목코드]
                df_분봉 = dic_분봉[s_종목코드]
                df_초봉 = dic_초봉[s_종목코드]

                # 차트 배치
                ax_거래일봉 = fig.add_subplot(n_차트_세로, n_차트_가로, 3 * (i + 1) + 1)
                ax_거래분봉 = fig.add_subplot(n_차트_세로, n_차트_가로, 3 * (i + 1) + 2)
                ax_거래초봉 = fig.add_subplot(n_차트_세로, n_차트_가로, 3 * (i + 1) + 3)

                # 차트 구성
                ret = self._ax_거래일봉(ax_거래일봉=ax_거래일봉, df_일봉=df_일봉, dic_거래=dic_거래)
                ret = self._ax_거래분봉(ax_거래분봉=ax_거래분봉, df_분봉=df_분봉, dic_거래=dic_거래)
                ret = self._ax_거래초봉(ax_거래초봉=ax_거래초봉, df_초봉=df_초봉, dic_거래=dic_거래)

            # 매매일보 저장
            # fig.savefig(os.path.join(folder_타겟, f'{file_타겟}_{s_일자}_{n_봉수}초봉.png'))
            # fig.savefig(os.path.join(folder_타겟, f'{file_타겟}_{s_일자}_{n_봉수}초봉.svg'))
            path_저장 = os.path.join(folder_타겟, f'{file_타겟}_{s_일자}_{n_봉수}초봉.svg')
            fig.savefig(path_저장)
            if sys.platform == 'darwin':
                os.system(f'xattr -d com.apple.quarantine {path_저장} 2>/dev/null')

            # 로그 기록
            self.make_로그(f'{s_일자} 완료\n'
                          f' - 일거래 {len(df_일거래)}건, 누적승률 {n_누적승률 * 100:,.0f}%, 누적손익비 {n_누적손익비:,.1f}\n'
                          f' - 기대수익 {n_기대수익:,.2f}, 누적수익률 {n_누적수익률:,.0f}%')

    @staticmethod
    def _make_매매신호_종목(dic_매개변수):
        """ 종목별 매수매도 정보 생성 후 리턴 """
        # 기준정보 정의
        s_종목코드 = dic_매개변수['s_종목코드']
        s_종목명 = dic_매개변수['s_종목명']
        folder_타겟 = dic_매개변수['folder_타겟']
        file_타겟 = dic_매개변수['file_타겟']
        li_매도사유 = dic_매개변수['li_매도사유']
        n_봉수 = dic_매개변수['n_봉수']
        s_일자 = dic_매개변수['s_일자']
        dic_일봉시가 = dic_매개변수['dic_일봉시가']
        df_초봉 = dic_매개변수['df_초봉'].loc[:, '종목코드':'매도횟수']
        df_1초봉 = dic_매개변수['df_1초봉']

        # 종목별 args 생성
        dic_args_종목 = dic_매개변수

        # 추가정보 생성
        df_초봉['종가1'] = df_초봉['종가'].shift(1)
        df_초봉['종가ma20'] = df_초봉['종가'].rolling(20).mean()
        df_초봉['종가ma60'] = df_초봉['종가'].rolling(60).mean()
        df_초봉['거래량ma5'] = df_초봉['거래량'].rolling(5).mean()
        df_초봉['거래량ma20'] = df_초봉['거래량'].rolling(20).mean()
        df_초봉['거래대금'] = df_초봉['종가'] * df_초봉['거래량']
        df_초봉['매수량ma5'] = df_초봉['매수량'].rolling(5).mean()
        df_초봉['매수량ma20'] = df_초봉['매수량'].rolling(20).mean()
        df_초봉['매수대금'] = df_초봉['종가'] * df_초봉['매수량']
        df_초봉['고가20'] = df_초봉['고가'].shift(1).rolling(20).max()
        df_초봉['고가40'] = df_초봉['고가'].shift(1).rolling(40).max()
        df_초봉['고가60'] = df_초봉['고가'].shift(1).rolling(60).max()
        sri_고가, sri_저가, sri_전일종가 = df_초봉['고가'], df_초봉['저가'], df_초봉['종가'].shift(1)
        li_atr산출 = [(sri_고가 - sri_저가), (sri_고가 - sri_전일종가).abs(), (sri_저가 - sri_전일종가).abs()]
        df_초봉['ATR14'] = pd.concat(li_atr산출, axis=1).max(axis=1).rolling(14).mean()
        df_초봉['ATR비율'] = df_초봉['ATR14'] / df_초봉['종가'] * 100
        df_초봉['ATR비율차이'] = df_초봉['ATR비율'] - df_초봉['ATR비율'].shift(1)
        df_초봉['일봉시가'] = dic_일봉시가[s_종목코드]
        df_초봉['봉수'] = n_봉수

        # 매수매도 정보 생성
        b_보유신호, b_매수신호, b_매도신호 = False, False, False
        dic_매매신호 = dict()
        for dt_시점 in df_초봉.index:
            # 기준정보 확인
            s_시점 = dt_시점.strftime('%H:%M:%S')
            n_현재가 = df_1초봉.loc[dt_시점, '종가'] if dt_시점 in df_1초봉.index else df_초봉.loc[dt_시점, '시가']

            # 기준봉 준비 - 현재시점 이전 봉 데이터
            n_idx = df_초봉.index.get_loc(dt_시점)
            df_기준봉전체 = df_초봉.iloc[:n_idx]
            df_기준봉전체 = df_기준봉전체.dropna(subset=['종가ma20', '종가ma60', '거래량ma5', '고가20', 'ATR14'])
            if df_기준봉전체.empty: continue
            df_기준봉 = df_기준봉전체[-1:]

            # 매수 검증
            if not b_보유신호:
                # 매수신호 생성
                dic_args_종목.update(매수봇_s_탐색시간=s_시점, 매수봇_n_현재가=n_현재가)
                dic_매수신호 = Logic.judge_매수신호(df_기준봉=df_기준봉)
                li_매수신호 = dic_매수신호['매수봇_li_매수신호']
                b_매수신호 = dic_매수신호['매수봇_b_매수신호'] if s_시점 < '15:00:00' else False

                # dic_args 업데이트
                dic_args_종목.update(dic_매수신호)

                # 매수정보 생성
                if b_매수신호:
                    dic_args_종목.update(매수봇_s_주문시간=s_시점, 매수봇_n_주문단가=n_현재가, 매수봇_n_주문수량=1)
                    b_보유신호 = True

            # 매도신호 생성
            if b_보유신호:
                dic_args_종목.update(매도봇_s_탐색시간=s_시점, 매도봇_n_현재가=n_현재가, 매도봇_df_기준봉전체=df_기준봉전체,
                                   매도봇_s_매수시간=dic_args_종목['매수봇_s_주문시간'],
                                   매도봇_n_매수단가=dic_args_종목['매수봇_n_주문단가'],
                                   매도봇_n_보유수량=dic_args_종목['매수봇_n_주문수량'])
                if dic_args_종목['매수봇_s_주문시간'] == dic_args_종목['매도봇_s_탐색시간']: continue
                dic_매도신호 = Logic.judge_매도신호(df_기준봉=df_기준봉, dic_args=dic_args_종목)
                li_매도신호 = dic_매도신호['매도봇_li_매도신호']
                b_매도신호 = dic_매도신호['매도봇_b_매도신호']
                li_신호종류 = dic_매도신호['매도봇_li_신호종류']

                # dic_args 업데이트
                dic_args_종목.update(dic_매도신호)

                # 매도정보 생성
                if b_매도신호:
                    dic_args_종목.update(매도봇_s_주문시간=s_시점, 매도봇_n_주문단가=n_현재가, 매도봇_n_주문수량=1,
                                       매도봇_s_매도사유=li_신호종류[li_매도신호.index(True)])

            # 결과 정리
            dic_매매신호_추가 = dict(일자=s_일자, 종목명=s_종목명)
            dic_매매신호_추가.update({컬럼: df_초봉.loc[dt_시점, 컬럼] for 컬럼 in df_초봉.columns})
            dic_매매신호_추가.update({f'매수_{신호종류}': dic_args_종목['매수봇_li_매수신호'][i]
                                    for i, 신호종류 in enumerate(dic_args_종목['매수봇_li_신호종류'])})
            dic_매매신호_추가.update({f'매도_{신호종류}': dic_args_종목['매도봇_li_매도신호'][i] if b_보유신호 else None
                                    for i, 신호종류 in enumerate(li_매도사유)})
            dic_매매신호_추가.update(현재시점=s_시점, 현재가=n_현재가, 매수신호=b_매수신호, 매도신호=b_매도신호, 보유신호=b_보유신호)
            dic_매매신호_추가.update(매수가=dic_args_종목['매도봇_n_매수단가'] if b_보유신호 else None,
                               매도가=dic_args_종목['매도봇_n_주문단가'] if b_매도신호 else None,
                               매수시간=dic_args_종목['매수봇_s_주문시간'] if b_보유신호 else None,
                               매도시간=dic_args_종목['매도봇_s_주문시간'] if b_매도신호 else None,
                               매도사유=dic_args_종목['매도봇_s_매도사유'] if b_매도신호 else None,
                               수익률=dic_args_종목['매도봇_n_수익률'] if b_보유신호 else None,
                               보유초=dic_args_종목['매도봇_n_경과시간'] if b_보유신호 else None,
                               리스크=dic_args_종목['매도봇_n_리스크'] if b_보유신호 else None)
            dic_매매신호_추가 = {key: [value] for key, value in dic_매매신호_추가.items()}
            dic_매매신호 = {key: dic_매매신호.get(key, list()) + dic_매매신호_추가.get(key, list())
                            for key in dic_매매신호_추가.keys()}

            # 신호 및 정보 업데이트 - 보유신호 업데이트 후 매수/매도신호 초기화
            b_보유신호 = False if b_매도신호 else b_보유신호
            b_매수신호, b_매도신호 = False, False

        # 결과 정리
        df_매매신호 = pd.DataFrame(dic_매매신호).sort_index()
        li_컬럼명_앞 = ['일자', '종목코드', '종목명']
        df_매매신호 = df_매매신호.loc[:, li_컬럼명_앞 + [컬럼 for 컬럼 in df_매매신호.columns if 컬럼 not in li_컬럼명_앞]]\
                    if not df_매매신호.empty else df_매매신호

        # csv 저장
        folder = os.path.join(f'{folder_타겟}_종목별', f'매매신호_{s_일자}')
        os.makedirs(folder, exist_ok=True)
        df_매매신호.to_csv(os.path.join(folder, f'{file_타겟}_{s_일자}_{n_봉수}초봉_{s_종목코드}_{s_종목명}.csv'),
                            index=False, encoding='cp949')

        return s_종목코드, df_매매신호

    def _generate_일별리포트(self, df_매매내역):
        """ 매매내역 데이터 기준으로 일별 리포트 생성 후 리턴 """
        # 기준정보 정의
        if df_매매내역.empty: return pd.DataFrame()
        s_일자 = df_매매내역['일자'].values[0]
        gr_매매내역 = df_매매내역.groupby('종목코드')
        li_종목코드 = ['Total'] + list(gr_매매내역.groups.keys())

        # 리포트 생성
        df_리포트 = self._summary_매매내역(df_매매내역=df_매매내역, s_구분자='종목코드')

        return df_리포트

    def _generate_누적리포트(self, dic_매개변수):
        """ 매매내역 데이터 기준으로 누적 리포트 생성 후 리턴 """
        # 기준정보 정의
        s_일자 = dic_매개변수['s_일자']
        n_봉수 = dic_매개변수['n_봉수']
        folder_타겟 = dic_매개변수['folder_타겟']
        file_타겟 = dic_매개변수['file_타겟']

        # 매매내역 불러오기
        li_파일명 = sorted(파일 for 파일 in os.listdir(folder_타겟) if file_타겟 in 파일 and f'{n_봉수}초봉' in 파일
                            and '.pkl' in 파일 and re.findall(r'\d{8}', 파일)[0] <= s_일자)
        dic_매매내역 = {re.findall(r'\d{8}', 파일)[0]: pd.read_pickle(os.path.join(folder_타겟, 파일))
                        for 파일 in li_파일명}
        df_매매내역_누적 = pd.concat(dic_매매내역.values(), axis=0) if dic_매매내역 else pd.DataFrame()

        # 수익금액 생성
        df_수익금액_누적 = self._generate_수익금액(df_매매내역=df_매매내역_누적)

        # 리포트 생성
        df_누적리포트 = self._summary_매매내역(df_매매내역=df_수익금액_누적, s_구분자='일자')

        return df_수익금액_누적, df_누적리포트

    def _summary_매매내역(self, df_매매내역, s_구분자):
        """ 입력된 매매내역을 구분자에 따라 데이터 가공하여 리포트 리턴 """
        # 기줁정보 정의
        if df_매매내역.empty: return pd.DataFrame()
        gr_매매내역 = df_매매내역.groupby(s_구분자)
        li_구분항목 = ['Total'] + sorted(gr_매매내역.groups.keys()) if s_구분자 == '종목코드' else\
                    ['Total'] + sorted(gr_매매내역.groups.keys(), reverse=True) if s_구분자 == '일자' else list()
        dic_코드2종목명 = df_매매내역.set_index('종목코드')['종목명'].to_dict()

        # 종목별 정리
        li_dic리포트 = list()
        for s_구분항목 in li_구분항목:
            # 기준정보 정의
            s_일자 = s_구분항목 if s_구분자 == '일자' else df_매매내역['일자'].values[0]
            s_종목코드 = s_구분항목 if s_구분자 == '종목코드' else None
            s_종목명 = dic_코드2종목명.get(s_종목코드, s_종목코드) if s_구분자 == '종목코드' else None

            # 데이터 정의
            df_매매내역_항목 = gr_매매내역.get_group(s_구분항목) if s_구분항목 != 'Total' else df_매매내역
            df_매매내역_항목_수익 = df_매매내역_항목.loc[df_매매내역_항목['수익률'] >= 0, :]
            df_매매내역_항목_손실 = df_매매내역_항목.loc[df_매매내역_항목['수익률'] < 0, :]

            # 리포트 생성
            dic_리포트 = dict(일자=s_일자, 종목코드=s_종목코드, 종목명=s_종목명,
                           거래수=len(df_매매내역_항목),
                           수익률sum=df_매매내역_항목['수익률'].sum() if not df_매매내역_항목.empty else None,
                           보유초mean=df_매매내역_항목['보유초'].mean())

            for s_매도사유 in self.li_매도사유:
                df_매매이력_종목_매도사유 = df_매매내역_항목[df_매매내역_항목['매도사유'] == s_매도사유]
                dic_리포트.update({
                    f'{s_매도사유}_거래수': len(df_매매이력_종목_매도사유),
                    f'{s_매도사유}_수익률sum': df_매매이력_종목_매도사유['수익률'].sum() if not df_매매이력_종목_매도사유.empty else None,
                    f'{s_매도사유}_수익률mean': df_매매이력_종목_매도사유['수익률'].mean() if not df_매매이력_종목_매도사유.empty else None,
                    f'{s_매도사유}_보유초mean': df_매매이력_종목_매도사유['보유초'].mean()
                })

            # 지표 추가
            dic_리포트.update(이익거래수=len(df_매매내역_항목_수익) if not df_매매내역_항목_수익.empty else 0,
                           평균이익률=df_매매내역_항목_수익['수익률'].mean() if not df_매매내역_항목_수익.empty else 0,
                           손실거래수=len(df_매매내역_항목_손실) if not df_매매내역_항목_손실.empty else 0,
                           평균손실률=df_매매내역_항목_손실['수익률'].mean() if not df_매매내역_항목_손실.empty else 0)

            # 수익금액 추가
            if s_구분자 == '일자':
                dic_리포트.update(시작금액=df_매매내역_항목['시작금액'].values[0],
                               종료금액=df_매매내역_항목['종료금액'].values[-1],
                               수익금액=df_매매내역_항목['수익금액'].sum())

            # df 변환 및 추가
            li_dic리포트.append(dic_리포트)

        # 결과 생성
        df_리포트 = pd.DataFrame(li_dic리포트) if li_dic리포트 else pd.DataFrame()

        # 기대수익 생성
        df_리포트['승률'] = df_리포트['이익거래수'] / df_리포트['거래수']
        df_리포트['손익비'] = df_리포트['평균이익률'] / df_리포트['평균손실률'].abs()
        df_리포트['기대수익'] = (df_리포트['승률'] * df_리포트['손익비']) - (1 - df_리포트['승률'])

        # 컬럼 재정렬
        if s_구분자 == '일자':
            li_마지막컬럼 = ['시작금액', '종료금액', '수익금액']
            li_컬럼명 = [컬럼 for 컬럼 in df_리포트.columns if 컬럼 not in li_마지막컬럼] + li_마지막컬럼
            df_리포트 = df_리포트.loc[:, li_컬럼명]

        return df_리포트

    @staticmethod
    def _generate_수익금액(df_매매내역):
        """ 매매내역 기준으로 수익금액 계산하여 리턴 """
        # 기준정보 정의
        n_자본금 = 1000 * 10**4
        n_한계리스크율 = 2
        df_매매내역 = df_매매내역.sort_values(['일자', '매수시간', '매도시간']).reset_index(drop=True)

        # 매매별 계산
        li_dic수익금액 = list()
        for idx in df_매매내역.index:
            # 기준정보 정의
            n_매수가 = df_매매내역.loc[idx, '매수가']
            n_매도가 = df_매매내역.loc[idx, '매도가']
            n_리스크 = df_매매내역.loc[idx, '리스크']
            n_한계리스크 = n_자본금 * n_한계리스크율 / 100
            n_매매수량 = int(n_한계리스크 / n_리스크)
            n_매수금액 = n_매수가 * n_매매수량
            n_매도금액 = n_매도가 * n_매매수량
            n_수익금액 = n_매도금액 - n_매수금액
            n_거래후자본금 = n_자본금 + n_수익금액

            # 데이터 생성
            dic_수익금액_항목 = {컬럼 : df_매매내역.loc[idx, 컬럼] for 컬럼 in df_매매내역.columns}
            dic_수익금액_항목.update(한계리스크율=n_한계리스크율, 한계리스크=n_한계리스크, 매매수량=n_매매수량,
                               매수금액=n_매수금액, 매도금액=n_매도금액, 시작금액=n_자본금, 종료금액=n_거래후자본금, 수익금액=n_수익금액)

            # list 추가
            li_dic수익금액.append(dic_수익금액_항목)
            n_자본금 = n_거래후자본금

        # df 생성
        df_수익금액 = pd.DataFrame(li_dic수익금액)

        return df_수익금액

    @staticmethod
    def _ax_기대수익(ax_승률손익비, ax_기대수익, dic_데이터, n_이동평균):
        """ 입력된 데이터 기준으로 승률손익비 및 기대수익 그래프 생성 후 리턴 """
        # 기준정보 정의
        dic_색상 = dict(파랑='C0', 주황='C1', 녹색='C2', 빨강='C3', 보라='C4', 고동='C5', 분홍='C6', 회색='C7', 풀색='C8', 하늘='C9')

        # 일별 데이터 정의
        df_일별 = dic_데이터['누적리포트'][1:].sort_values('일자')
        li_일자 = df_일별['일자'].values
        ary_일별승률 = df_일별['승률'].values
        ary_일별손익비 = df_일별['손익비'].values

        # 누적 데이터 정의
        df_거래별 = dic_데이터['수익금액'].sort_values('일자')
        li_dic누적 = list()
        for s_누적일자  in df_거래별['일자'].unique():
            df_누적_일자 = df_거래별.loc[df_거래별['일자'] <= s_누적일자]
            df_누적_일자_이익 = df_누적_일자.loc[df_누적_일자['수익률'] > 0]
            df_누적_일자_손실 = df_누적_일자.loc[df_누적_일자['수익률'] < 0]
            n_총거래수 = len(df_누적_일자)
            n_이익거래수 = len(df_누적_일자_이익)
            n_손실거래수 = len(df_누적_일자_손실)
            n_평균이익률 = df_누적_일자_이익['수익률'].mean() if n_이익거래수 > 0 else 0
            n_평균손실률 = df_누적_일자_손실['수익률'].mean() if n_손실거래수 > 0 else 0
            dic_누적 = dict(일자=s_누적일자, 승률=n_이익거래수 / n_총거래수, 손익비=n_평균이익률 / abs(n_평균손실률))
            dic_누적.update(기대수익=(dic_누적['승률'] * dic_누적['손익비']) - (1 - dic_누적['승률']))
            li_dic누적.append(dic_누적)
        df_누적 = pd.DataFrame(li_dic누적).sort_values('일자')

        # 그래프용 데이터 정의
        li_일자 = [f'{일자[4:6]}-{일자[6:8]}' for 일자  in df_누적['일자']]
        ary_누적승률 = df_누적['승률'].values
        ary_누적손익비 = df_누적['손익비'].values
        ary_누적기대수익 = df_누적['기대수익'].values
        ary_일별기대수익 = (ary_일별승률 * ary_일별손익비) - (1 - ary_일별승률)
        ary_일별기대수익ma = pd.Series(ary_일별기대수익).rolling(window=n_이동평균).mean().values

        # 그래프 설정 - 승률
        ax_승률 = ax_승률손익비
        ax_승률.plot(li_일자, ary_누적승률, label='누적승률', lw=2, alpha=1, color=dic_색상['녹색'])
        ax_승률.axhline(0.3, lw=2, alpha=0.5, linestyle='--', color=dic_색상['녹색'])

        # 그래프 설정 - 승률
        ax_손익비 = ax_승률손익비.twinx()
        ax_손익비.plot(li_일자, ary_누적손익비, label='누적손익비', lw=2, alpha=1, color=dic_색상['보라'])
        ax_손익비.axhline(3, lw=2, alpha=0.5, linestyle='--', color=dic_색상['보라'])

        # 그래프 설정 - 기대수익
        ax_기대수익.bar(li_일자, ary_일별기대수익, label='일별', lw=2, alpha=0.5, color=dic_색상['회색'])
        ax_기대수익.plot(li_일자, ary_일별기대수익ma, label=f'ma{n_이동평균}', lw=2, alpha=0.5, color=dic_색상['녹색'])
        ax_기대수익.plot(li_일자, ary_누적기대수익, label='누적', lw=2, alpha=1, color=dic_색상['주황'])
        ax_기대수익.axhline(0.2, lw=2, alpha=0.5, linestyle='--', color=dic_색상['주황'])

        # 스케일 설정 - 승률
        ax_승률.set_ylim(-0.3, 1.5)
        # ax_승률.set_yticks([-0.3, 0, 0.3, 0.6, 0.9, 1.2, 1.5], labels=[])
        # ax_승률.set_yticks([-0.3, 0, 0.3, 0.6, 0.9, 1.2, 1.5],
        #                  labels=['', '0%', '30%', '60%', '90%', '120%', ''])
        li_틱 = [-0.3, 0, 0.3, 0.6, 0.9, 1.2, 1.5]
        ax_승률.set_yticks(li_틱, labels=[f'{틱 * 100:.0f}%' if 틱 not in [li_틱[0], li_틱[-1]] else '' for 틱 in li_틱])

        # 스케일 설정 - 손익비
        ax_손익비.set_ylim(-1, 5)
        # ax_손익비.set_yticks([-1, 0, 1, 2, 3, 4, 5], labels=[])
        # ax_손익비.set_yticks([-1, 0, 1, 2, 3, 4, 5],
        #                   labels=['', '0', '1.0', '2.0', '3.0', '4.0', ''])
        li_틱 = [-1, 0, 1, 2, 3, 4, 5]
        ax_손익비.set_yticks(li_틱, labels=[f'{틱:.1f}' if 틱 not in [li_틱[0], li_틱[-1]] else '' for 틱 in li_틱])

        # 스케일 설정 - 기대수익
        ax_기대수익.set_ylim(-1.2, 1.2)
        # ax_기대수익.set_yticks([-1.2, -0.8, -0.4, 0, 0.4, 0.8, 1.2], labels=[])
        # ax_기대수익.set_yticks([-1.2, -0.8, -0.4, 0, 0.4, 0.8, 1.2],
        #                    labels=['', '-0.80', '-0.40', '0', '0.40', '0.80', ''])
        li_틱 = [-1.2, -0.8, -0.4, 0, 0.4, 0.8, 1.2]
        ax_기대수익.set_yticks(li_틱, labels=[f'{틱:.2f}' if 틱 not in [li_틱[0], li_틱[-1]] else '' for 틱 in li_틱])

        # 뷰 설정 - 승률손익비
        ax_승률손익비.set_title('[ 승률/손익비 ]', loc='left', fontsize=10, fontweight='bold')
        ax_승률손익비.grid(True, axis='y', color='gray', linestyle='--', linewidth=0.5, alpha=0.5)
        # ax_승률.tick_params(axis='y', length=0)
        ax_승률.tick_params(length=0, labelsize=8)
        ax_손익비.tick_params(length=0, labelsize=8)
        # ax_승률손익비.legend(loc='upper left', fontsize=8)
        ax_승률손익비.axhline(0, lw=0.5, alpha=1, color='black')
        ax_승률손익비.set_xticks([li_일자[0], li_일자[-1]])
        n_누적승률 = ary_누적승률[-1]
        n_누적손익비 = ary_누적손익비[-1]
        ax_승률손익비.text(li_일자[-1], 1.45, f'승률 {n_누적승률 * 100:.0f}%',
                      fontsize=9, fontweight='bold', color=dic_색상['녹색'], va='top', ha='right')
        ax_승률손익비.text(li_일자[-1], 1.33, f'손익비 {n_누적손익비:.1f}',
                      fontsize=9, fontweight='bold', color=dic_색상['보라'], va='top', ha='right')

        # 뷰 설정 - 기대수익
        ax_기대수익.set_title('[ 기대수익 ]', loc='left', fontsize=10, fontweight='bold')
        ax_기대수익.grid(True, axis='y', color='gray', linestyle='--', linewidth=0.5, alpha=0.5)
        # ax_기대수익.tick_params(axis='y', length=0)
        ax_기대수익.tick_params(length=0, labelsize=8)
        ax_기대수익.legend(loc='upper left', fontsize=8)
        ax_기대수익.set_xticks([li_일자[0], li_일자[-1]])
        ax_기대수익.axhline(0, lw=0.5, alpha=1, color='black')
        n_기대수익 = ary_누적기대수익[-1]
        ax_기대수익.text(li_일자[-1], 1.1, f'기대수익 {n_기대수익:.2f}',
                     fontsize=9, fontweight='bold', color=dic_색상['주황'], va='top', ha='right')

        return n_누적승률, n_누적손익비, n_기대수익

    @staticmethod
    def _ax_누적수익률(ax_누적수익률, dic_데이터):
        """ 입력된 데이터 기준으로 누적수익률 그래프 생성 후 리턴 """
        # 기준정보 정의
        dic_색상 = dict(파랑='C0', 주황='C1', 녹색='C2', 빨강='C3', 보라='C4', 고동='C5', 분홍='C6', 회색='C7', 풀색='C8', 하늘='C9')

        # 일별 데이터 정의
        df_일별 = dic_데이터['수익금액'].sort_values('일자')
        gr_일별 = df_일별.groupby('일자')
        n_시작금액 = df_일별['시작금액'].values[0]

        # 누적 데이터 정의
        li_dic누적 = list()
        for s_누적일자, df_일자 in gr_일별:
            df_일자 = df_일자.sort_values('매수시간')
            n_수익금액 = df_일자['수익금액'].sum()
            n_일시작금액 = df_일자['시작금액'].values[0]
            n_일종료금액 = df_일자['종료금액'].values[-1]
            n_일별수익률 = (n_일종료금액 / n_일시작금액 - 1) * 100
            n_누적수익률 = (n_일종료금액 / n_시작금액 - 1) * 100
            dic_누적 = dict(일자=s_누적일자, 시작금액=n_일시작금액, 수익금액=n_수익금액, 종료금액=n_일종료금액,
                          일별수익률=n_일별수익률, 누적수익률=n_누적수익률)
            li_dic누적.append(dic_누적)
        df_누적 = pd.DataFrame(li_dic누적).sort_values('일자')

        # 그래프용 데이터 정의
        li_일자 = [f'{일자[4:6]}-{일자[6:8]}' for 일자  in df_누적['일자']]
        ary_시작금액 = df_누적['시작금액'].values
        ary_수익금액 = df_누적['수익금액'].values
        ary_종료금액 = df_누적['종료금액'].values
        ary_일별수익률 = df_누적['일별수익률'].values
        ary_누적수익률 = df_누적['누적수익률'].values

        # 그래프 설정
        ax_누적수익률.bar(li_일자, ary_일별수익률, label='일별', lw=1, alpha=0.5, color=dic_색상['회색'])
        ax_누적수익률.plot(li_일자, ary_누적수익률, label='누적', lw=1, alpha=1, color=dic_색상['고동'])
        ax_누적수익률.axhline(50, lw=2, alpha=0.5, linestyle='--', color=dic_색상['고동'])

        # 스케일 설정
        ax_누적수익률.set_ylim(-75, 75)
        # ax_누적수익률.set_yticks([-150, -100, -50, 0, 50, 100, 150], labels=[])
        li_틱 = [-75, -50, -25, 0, 25, 50, 75]
        ax_누적수익률.set_yticks(li_틱, labels=[f'{틱:.0f}%' if 틱 not in [li_틱[0], li_틱[-1]] else '' for 틱 in li_틱])
        # ax_누적수익률.set_yticks([-75, -50, -25, 0, 25, 50, 75],
        #                     labels=['', '-50%', '-50%', '0', '50%', '100%', ''])
        # ax_누적수익률.tick_params(axis='y', length=0, labelsize=8)

        # 뷰 설정
        ax_누적수익률.set_title('[ 누적수익률 ]', loc='left', fontsize=10, fontweight='bold')
        ax_누적수익률.grid(True, axis='y', color='gray', linestyle='--', linewidth=0.5, alpha=0.5)
        ax_누적수익률.tick_params(length=0, labelsize=8)
        ax_누적수익률.legend(loc='upper left', fontsize=8)
        ax_누적수익률.axhline(0, lw=0.5, alpha=1, color='black')
        ax_누적수익률.set_xticks([li_일자[0], li_일자[-1]])
        n_누적수익률 = ary_누적수익률[-1]
        ax_누적수익률.text(li_일자[-1], 70, f'누적수익률 {n_누적수익률:.0f}%',
                      fontsize=9, fontweight='bold', color=dic_색상['고동'], va='top', ha='right')

        return n_누적수익률

    @staticmethod
    def _ax_거래일봉(ax_거래일봉, df_일봉, dic_거래):
        """ 입력된 데이터 기준으로 거래일봉 그래프 생성 후 리턴 """
        # 기준정보 정의
        dic_색상 = dict(파랑='C0', 주황='C1', 녹색='C2', 빨강='C3', 보라='C4', 고동='C5', 분홍='C6', 회색='C7', 풀색='C8', 하늘='C9')
        s_종목명, s_종목코드 = dic_거래['종목명'], dic_거래['종목코드']
        n_매수가, n_매도가 = dic_거래['매수가'], dic_거래['매도가']
        s_매도사유, n_수익률, n_리스크 = dic_거래['매도사유'], dic_거래['수익률'], dic_거래['리스크']
        s_거래일자 = f'{dic_거래['일자'][4:6]}-{dic_거래['일자'][6:8]}'

        # 차트 생성
        li_일시 = Chart.make_캔들차트(ax=ax_거래일봉, df_차트=df_일봉, s_봉구분='일봉', s_차트구분='캔들')

        # 뷰 설정
        ax_거래일봉.set_title(f'[일봉] {s_종목명}({s_종목코드}) | {s_매도사유}({n_수익률:.1f}%)',
                          loc='left', fontsize=10, fontweight='bold')
        ax_거래일봉.tick_params(length=0, labelsize=8)
        ax_거래일봉.set_xticks([li_일시[0], li_일시[-1]])

        # 거래정보 설정
        ax_거래일봉.axvline(s_거래일자, lw=5, alpha=0.1, color=dic_색상['분홍'])
        ax_거래일봉.axhline(n_매수가, lw=2, alpha=0.4, color=dic_색상['분홍'])
        ax_거래일봉.axhline(n_매도가, lw=2, alpha=0.4, color=dic_색상['하늘'])

        return ax_거래일봉

    @staticmethod
    def _ax_거래분봉(ax_거래분봉, df_분봉, dic_거래):
        """ 입력된 데이터 기준으로 거래분봉 그래프 생성 후 리턴 """
        # 기준정보 정의
        dic_색상 = dict(파랑='C0', 주황='C1', 녹색='C2', 빨강='C3', 보라='C4', 고동='C5', 분홍='C6', 회색='C7', 풀색='C8', 하늘='C9')
        s_종목명, s_종목코드 = dic_거래['종목명'], dic_거래['종목코드']
        n_매수가, n_매도가 = dic_거래['매수가'], dic_거래['매도가']
        s_매도사유, n_수익률, n_리스크 = dic_거래['매도사유'], dic_거래['수익률'], dic_거래['리스크']
        s_매수시점 = pd.Timestamp(dic_거래['매수시간']).floor('3min').strftime('%H:%M')
        s_매도시점 = pd.Timestamp(dic_거래['매도시간']).floor('3min').strftime('%H:%M')
        s_타임아웃시점 = (pd.Timestamp(s_매수시점) + pd.Timedelta(seconds=600)).floor('3min').strftime('%H:%M')

        # 분봉 잘라내기
        s_시작시점 = (pd.Timestamp(s_매수시점) - pd.Timedelta(minutes=3 * 10)).strftime('%H:%M:%S')
        s_종료시점 = (pd.Timestamp(s_타임아웃시점) + pd.Timedelta(minutes=3 * 10)).strftime('%H:%M:%S')
        df_분봉 = df_분봉.loc[df_분봉['시간'].between(s_시작시점, s_종료시점)]

        # 차트 생성
        li_일시 = Chart.make_캔들차트(ax=ax_거래분봉, df_차트=df_분봉, s_봉구분='3분봉', s_차트구분='캔들', b_legend=False)

        # 뷰 설정
        ax_거래분봉.set_title(f'[3분봉] {s_종목명} | {s_매도사유}({n_수익률:.1f}%) | 리스크 {n_리스크:,.0f}원',
                          loc='left', fontsize=10, fontweight='bold')
        ax_거래분봉.tick_params(length=0, labelsize=8)
        ax_거래분봉.set_xticks([s_매수시점, s_매도시점])

        # 거래정보 설정
        ax_거래분봉.axvline(s_매수시점, lw=5, alpha=0.1, color=dic_색상['분홍'])
        ax_거래분봉.axvline(s_매도시점, lw=5, alpha=0.1, color=dic_색상['하늘'])
        ax_거래분봉.axhline(n_매수가, lw=2, alpha=0.4, color=dic_색상['분홍'])
        ax_거래분봉.axhline(n_매도가, lw=2, alpha=0.4, color=dic_색상['하늘'])

        # 타임아웃정보 설정
        ax_거래분봉.axvspan(li_일시[0], s_매수시점, alpha=0.3, color=dic_색상['회색'])
        ax_거래분봉.axvspan(s_타임아웃시점, li_일시[-1], alpha=0.3, color=dic_색상['회색'])

        return ax_거래분봉

    @staticmethod
    def _ax_거래초봉(ax_거래초봉, df_초봉, dic_거래):
        """ 입력된 데이터 기준으로 거래초봉 그래프 생성 후 리턴 """
        # 기준정보 정의
        dic_색상 = dict(파랑='C0', 주황='C1', 녹색='C2', 빨강='C3', 보라='C4', 고동='C5', 분홍='C6', 회색='C7', 풀색='C8', 하늘='C9')
        s_종목명, s_종목코드 = dic_거래['종목명'], dic_거래['종목코드']
        n_매수가, n_매도가 = dic_거래['매수가'], dic_거래['매도가']
        s_매도사유, n_수익률, n_리스크 = dic_거래['매도사유'], dic_거래['수익률'], dic_거래['리스크']
        s_매수시점 = pd.Timestamp(dic_거래['매수시간']).floor('5s').strftime('%H:%M:%S')
        s_매도시점 = pd.Timestamp(dic_거래['매도시간']).floor('5s').strftime('%H:%M:%S')
        s_타임아웃시점 = (pd.Timestamp(s_매수시점) + pd.Timedelta(seconds=600)).floor('5s').strftime('%H:%M:%S')

        # 이동평균 생성
        df_초봉['종가ma5'] = df_초봉['종가'].rolling(5).mean()
        df_초봉['종가ma10'] = df_초봉['종가'].rolling(10).mean()
        df_초봉['종가ma20'] = df_초봉['종가'].rolling(20).mean()
        df_초봉['종가ma60'] = df_초봉['종가'].rolling(60).mean()
        df_초봉['종가ma120'] = df_초봉['종가'].rolling(120).mean()
        df_초봉['거래량ma5'] = df_초봉['거래량'].rolling(5).mean()
        df_초봉['거래량ma20'] = df_초봉['거래량'].rolling(20).mean()
        df_초봉['거래량ma60'] = df_초봉['거래량'].rolling(60).mean()
        df_초봉['거래량ma120'] = df_초봉['거래량'].rolling(120).mean()

        # 분봉 잘라내기
        s_시작시점 = (pd.Timestamp(s_매수시점) - pd.Timedelta(seconds=5 * 20)).strftime('%H:%M:%S')
        s_종료시점 = (pd.Timestamp(s_타임아웃시점) + pd.Timedelta(seconds=5 * 10)).strftime('%H:%M:%S')
        df_초봉 = df_초봉.loc[df_초봉['체결시간'].between(s_시작시점, s_종료시점)]

        # 차트 생성
        li_일시 = Chart.make_캔들차트(ax=ax_거래초봉, df_차트=df_초봉, s_봉구분='5초봉', s_차트구분='캔들', b_legend=False)
        # s_매수시점, s_매도시점, s_타임아웃시점 = s_매수시점[3:], s_매도시점[3:], s_타임아웃시점[3:]

        # 뷰 설정
        ax_거래초봉.set_title(f'[5초봉] {s_종목명} | {s_매도사유}({n_수익률:.1f}%) | 리스크 {n_리스크:,.0f}원',
                          loc='left', fontsize=10, fontweight='bold')
        ax_거래초봉.tick_params(length=0, labelsize=8)
        ax_거래초봉.set_xticks([s_매수시점, s_매도시점])

        # 거래정보 설정
        ax_거래초봉.axvline(s_매수시점, lw=5, alpha=0.1, color=dic_색상['분홍'])
        ax_거래초봉.axvline(s_매도시점, lw=5, alpha=0.1, color=dic_색상['하늘'])
        ax_거래초봉.axhline(n_매수가, lw=2, alpha=0.4, color=dic_색상['분홍'])
        ax_거래초봉.axhline(n_매도가, lw=2, alpha=0.4, color=dic_색상['하늘'])

        # 타임아웃정보 설정
        ax_거래초봉.axvspan(li_일시[0], s_매수시점, alpha=0.3, color=dic_색상['회색'])
        ax_거래초봉.axvspan(s_타임아웃시점, li_일시[-1], alpha=0.3, color=dic_색상['회색'])

        return ax_거래초봉


# noinspection PyNoneFunctionAssignment,NonAsciiCharacters,PyPep8Naming
def run():
    """ 실행 함수 """
    a = AnalyzerBot(b_디버그모드=False, s_시작일자='20251001')
    li_봉수 = [5]
    # ret = a.sync_소스파일()
    ret = a.find_일봉확인()
    ret = [a.make_매매신호(n_봉수=봉수) for 봉수 in li_봉수] # 매수매도 logic에 따른 신호 생성
    ret = [a.make_매수매도(n_봉수=봉수) for 봉수 in li_봉수] # 매수매도 신호 존재하는 종목의 데이터만 수집
    ret = [a.make_매매내역(n_봉수=봉수) for 봉수 in li_봉수] # 매매내역 정보만 한줄로 표기
    ret = [a.make_수익내역(n_봉수=봉수) for 봉수 in li_봉수] # 시간중복 제거 후 실매매 정보 표기
    ret = [a.make_매매일보(n_봉수=봉수) for 봉수 in li_봉수] # 시간중복 제거 후 실매매 정보 표기

if __name__ == '__main__':
    try:
        run()
    except KeyboardInterrupt:
        print('\n### [ KeyboardInterrupt detected ] ###')
