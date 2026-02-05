import os
import sys
import json
import re
import multiprocessing as mp

# win용 디버거 설정
if sys.platform == 'win32':
    import matplotlib
    matplotlib.use('TkAgg')

import pandas as pd
from tqdm import tqdm

import ut.로그maker, ut.폴더manager, ut.도구manager as Tool, ut.차트maker
import analyzer.logic_매수매도 as Logic


# noinspection NonAsciiCharacters,SpellCheckingInspection,PyPep8Naming
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
        li_전체일자 = li_전체일자[-3:]
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

            # 매개변수 정의 - 종목별 함수 전달용
            li_매개변수 = [dict(s_종목코드=s_종목코드, s_종목명=dic_코드2종목[s_종목코드], n_봉수=n_봉수, s_일자=s_일자,
                            folder_타겟=folder_타겟, file_타겟=file_타겟, li_매도사유=self.li_매도사유,
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

            # 리포트 생성
            df_리포트 = self._report_매매이력(df_매매이력=df_매매내역)

            # 결과파일 저장
            Tool.df저장(df=df_매매내역, path=os.path.join(folder_타겟, f'{file_타겟}_{s_일자}_{n_봉수}초봉'))

            # 리포트 저장
            folder_리포트 = f'{folder_타겟}_리포트'
            os.makedirs(folder_리포트, exist_ok=True)
            Tool.df저장(df=df_리포트, path=os.path.join(folder_리포트, f'{file_타겟}_리포트_{s_일자}_{n_봉수}초봉'))

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

            # 리포트 생성
            df_리포트 = self._report_매매이력(df_매매이력=df_수익내역)

            # 결과파일 저장
            Tool.df저장(df=df_수익내역, path=os.path.join(folder_타겟, f'{file_타겟}_{s_일자}_{n_봉수}초봉'))

            # 리포트 저장
            folder_리포트 = f'{folder_타겟}_리포트'
            os.makedirs(folder_리포트, exist_ok=True)
            Tool.df저장(df=df_리포트, path=os.path.join(folder_리포트, f'{file_타겟}_리포트_{s_일자}_{n_봉수}초봉'))

            # 로그 기록
            n_거래종목수 = len(df_수익내역['종목코드'].unique()) if len(df_수익내역) > 0 else 0
            n_거래건수 = len(df_수익내역) if len(df_수익내역) > 0 else 0
            n_수익률 = df_수익내역['수익률'].sum() if len(df_수익내역) > 0 else 0
            self.make_로그(f'{s_일자} 완료\n - {n_거래종목수:,.0f}종목, 거래 {n_거래건수:,.0f}건, 수익 {n_수익률:,.1f}%')

    def make_수익누적(self, n_봉수):
        """ 일별 수익 요약 및 리포트 발행 """
        # 기준정보 정의
        folder_소스 = os.path.join(self.folder_백테스팅, '50_수익내역')
        file_소스 = f'df_수익내역'
        folder_타겟 = os.path.join(self.folder_백테스팅, '50_수익내역_리포트누적')
        file_타겟 = f'df_수익누적'
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
            li_일자 = sorted(re.findall(r'\d{8}', 파일)[0] for 파일 in os.listdir(folder_소스) if '.pkl' in 파일)
            dic_수익내역 = {일자: pd.read_pickle(os.path.join(folder_소스, f'{file_소스}_{일자}_{n_봉수}초봉.pkl'))
                            for 일자 in li_일자 if 일자 <= s_일자}

            # 기준정보 정의
            n_일수 = len(dic_수익내역)
            li_dic수익누적 = list()

            # 수익누적 생성 - 전체
            df_수익내역_전체 = pd.concat(dic_수익내역.values(), axis=0)
            dic_수익누적_전체 = dict(일자='Total', 종목코드='Total', 종목명='Total',
                               거래수=len(df_수익내역_전체),
                               수익률sum=df_수익내역_전체['수익률'].sum() if not df_수익내역_전체.empty else None,
                               보유초mean=df_수익내역_전체['보유초'].mean())
            for s_매도사유 in self.li_매도사유:
                df_수익내역_전체_매도사유 = df_수익내역_전체[df_수익내역_전체['매도사유'] == s_매도사유]
                dic_수익누적_전체.update({
                    f'{s_매도사유}_거래수': len(df_수익내역_전체_매도사유),
                    f'{s_매도사유}_수익률sum': df_수익내역_전체_매도사유['수익률'].sum() if not df_수익내역_전체_매도사유.empty else None,
                    f'{s_매도사유}_보유초mean': df_수익내역_전체_매도사유['보유초'].mean()
                })
            li_dic수익누적.append(dic_수익누적_전체)

            # 수익누적 생성 - 전체 일평균
            dic_수익누적_전체일평균 = dict(일자='Total', 종목코드='일평균', 종목명=f'{n_일수}일평균',
                               거래수=dic_수익누적_전체['거래수'] / n_일수,
                               수익률sum=dic_수익누적_전체['수익률sum'] / n_일수,
                               보유초mean=dic_수익누적_전체['보유초mean'])
            for s_매도사유 in self.li_매도사유:
                dic_수익누적_전체일평균.update({
                    f'{s_매도사유}_거래수': dic_수익누적_전체[f'{s_매도사유}_거래수'] / n_일수
                                            if dic_수익누적_전체[f'{s_매도사유}_거래수'] is not None else 0,
                    f'{s_매도사유}_수익률sum': dic_수익누적_전체[f'{s_매도사유}_수익률sum'] / n_일수
                                            if dic_수익누적_전체[f'{s_매도사유}_수익률sum'] is not None else 0,
                    f'{s_매도사유}_보유초mean': dic_수익누적_전체[f'{s_매도사유}_보유초mean']
                })
            li_dic수익누적.append(dic_수익누적_전체일평균)

            # 수익누적 생성 - 일별
            for 일자_일별, df_수익내역_일별 in dic_수익내역.items():
                dic_수익누적_일별 = dict(일자=일자_일별, 종목코드='일별', 종목명='일별',
                                   거래수=len(df_수익내역_일별),
                                   수익률sum=df_수익내역_일별['수익률'].sum() if not df_수익내역_일별.empty else 0,
                                   보유초mean=df_수익내역_일별['보유초'].mean() if not df_수익내역_일별.empty else None)
                for s_매도사유 in self.li_매도사유:
                    df_수익내역_일별_매도사유 = df_수익내역_일별[df_수익내역_일별['매도사유'] == s_매도사유] if not df_수익내역_일별.empty else pd.DataFrame()
                    dic_수익누적_일별.update({
                        f'{s_매도사유}_거래수': len(df_수익내역_일별_매도사유),
                        f'{s_매도사유}_수익률sum': df_수익내역_일별_매도사유['수익률'].sum() if not df_수익내역_일별_매도사유.empty else 0,
                        f'{s_매도사유}_보유초mean': df_수익내역_일별_매도사유['보유초'].mean() if not df_수익내역_일별_매도사유.empty else None
                    })
                li_dic수익누적.append(dic_수익누적_일별)

            # 수익누적 생성
            df_수익누적 = pd.DataFrame(li_dic수익누적)

            # 결과파일 저장
            Tool.df저장(df=df_수익누적, path=os.path.join(folder_타겟, f'{file_타겟}_{s_일자}_{n_봉수}초봉'))

            # 로그 기록
            n_거래건수 = df_수익누적['거래수'].values[0]
            n_수익률 = df_수익누적['수익률sum'].values[0]
            self.make_로그(f'{s_일자} 완료\n - 총거래 {n_거래건수:,.0f}건, 총수익 {n_수익률:,.1f}%')

    def make_자금운영(self, n_봉수):
        """ 수익내역 기준으로 자금운영 기법 적용하여 손익 확인 """
        # 기준정보 정의
        folder_소스 = os.path.join(self.folder_백테스팅, '50_수익내역')
        file_소스 = f'df_수익내역'
        folder_타겟 = os.path.join(self.folder_백테스팅, '60_자금운영')
        file_타겟 = f'df_자금운영'
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

            # 리포트 생성
            df_리포트 = self._report_매매이력(df_매매이력=df_수익내역)

            # 결과파일 저장
            Tool.df저장(df=df_수익내역, path=os.path.join(folder_타겟, f'{file_타겟}_{s_일자}_{n_봉수}초봉'))

            # 리포트 저장
            folder_리포트 = f'{folder_타겟}_리포트'
            os.makedirs(folder_리포트, exist_ok=True)
            Tool.df저장(df=df_리포트, path=os.path.join(folder_리포트, f'{file_타겟}_리포트_{s_일자}_{n_봉수}초봉'))

            # 로그 기록
            n_거래종목수 = len(df_수익내역['종목코드'].unique()) if len(df_수익내역) > 0 else 0
            n_거래건수 = len(df_수익내역) if len(df_수익내역) > 0 else 0
            n_수익률 = df_수익내역['수익률'].sum() if len(df_수익내역) > 0 else 0
            self.make_로그(f'{s_일자} 완료\n - {n_거래종목수:,.0f}종목, 거래 {n_거래건수:,.0f}건, 수익 {n_수익률:,.1f}%')

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
        df_초봉 = dic_매개변수['df_초봉'].loc[:, '종목코드':'매도횟수']
        df_1초봉 = dic_매개변수['df_1초봉']

        # 종목별 args 생성
        dic_args_종목 = dic_매개변수

        # 추가정보 생성
        df_초봉['종가ma20'] = df_초봉['종가'].rolling(20).mean()
        df_초봉['종가ma60'] = df_초봉['종가'].rolling(60).mean()
        df_초봉['거래량ma5'] = df_초봉['거래량'].rolling(5).mean()
        df_초봉['고가20'] = df_초봉['고가'].shift(1).rolling(20).max()
        sri_고가, sri_저가, sri_전일종가 = df_초봉['고가'], df_초봉['저가'], df_초봉['종가'].shift(1)
        li_atr산출 = [(sri_고가 - sri_저가), (sri_고가 - sri_전일종가).abs(), (sri_저가 - sri_전일종가).abs()]
        df_초봉['ATR14'] = pd.concat(li_atr산출, axis=1).max(axis=1).rolling(14).mean()
        df_초봉['ATR비율'] = df_초봉['ATR14'] / df_초봉['종가'] * 100
        df_초봉['ATR비율차이'] = df_초봉['ATR비율'] - df_초봉['ATR비율'].shift(1)

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
                               경과시간=dic_args_종목['매도봇_n_경과시간'] if b_보유신호 else None)
            dic_매매신호_추가 = {key: [value] for key, value in dic_매매신호_추가.items()}
            dic_매매신호 = {key: dic_매매신호.get(key, list()) + dic_매매신호_추가.get(key, list())
                            for key in dic_매매신호_추가.keys()}

            # 신호 및 정보 업데이트 - 보유신호 업데이트 후 매수/매도신호 초기화
            b_보유신호 = False if b_매도신호 else b_보유신호
            b_매수신호, b_매도신호 = False, False

        # 결과 정리
        df_매매신호 = pd.DataFrame(dic_매매신호).sort_index()
        li_컬럼명_앞 = ['일자', '종목코드', '종목명']
        df_매매신호 = df_매매신호.loc[:, li_컬럼명_앞 + [컬럼 for 컬럼 in df_매매신호.columns if 컬럼 not in li_컬럼명_앞]]

        # csv 저장
        folder = os.path.join(f'{folder_타겟}_종목별', f'매매신호_{s_일자}')
        os.makedirs(folder, exist_ok=True)
        df_매매신호.to_csv(os.path.join(folder, f'{file_타겟}_{s_일자}_{n_봉수}초봉_{s_종목코드}_{s_종목명}.csv'),
                            index=False, encoding='cp949')

        return s_종목코드, df_매매신호

    def _report_매매이력(self, df_매매이력):
        """ 매매이력 데이터 기준으로 리포트 생성 후 저장 """
        if df_매매이력.empty: return pd.DataFrame()
        # 기준정보 정의
        s_일자 = df_매매이력['일자'].values[0]
        gr_매매이력 = df_매매이력.groupby('종목코드')
        li_종목코드 = ['Total'] + list(gr_매매이력.groups.keys())

        # 종목별 정리
        li_df리포트 = list()
        for s_종목코드 in li_종목코드:
            # 데이터 정의
            df_매매이력_종목 = gr_매매이력.get_group(s_종목코드) if s_종목코드 != 'Total' else df_매매이력
            s_종목명 = df_매매이력_종목['종목명'].values[0] if s_종목코드 != 'Total' else 'Total'

            # 리포트 생성
            dic_리포트 = dict(일자=s_일자, 종목코드=s_종목코드, 종목명=s_종목명,
                           거래수=len(df_매매이력_종목),
                           수익률sum=df_매매이력_종목['수익률'].sum() if not df_매매이력_종목.empty else None,
                           보유초mean=df_매매이력_종목['보유초'].mean())

            for s_매도사유 in self.li_매도사유:
                df_매매이력_종목_매도사유 = df_매매이력_종목[df_매매이력_종목['매도사유'] == s_매도사유]
                dic_리포트.update({
                    f'{s_매도사유}_거래수': len(df_매매이력_종목_매도사유),
                    f'{s_매도사유}_수익률sum': df_매매이력_종목_매도사유['수익률'].sum() if not df_매매이력_종목_매도사유.empty else None,
                    f'{s_매도사유}_보유초mean': df_매매이력_종목_매도사유['보유초'].mean()
                })

            # df 변환 및 추가
            li_df리포트.append(pd.DataFrame({key: [value] for key, value in dic_리포트.items()}))

        # 결과 생성
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            df_리포트 = pd.concat(li_df리포트, axis=0) if li_df리포트 else pd.DataFrame()

        return df_리포트


# noinspection PyNoneFunctionAssignment,NonAsciiCharacters,PyPep8Naming
def run():
    """ 실행 함수 """
    a = AnalyzerBot(b_디버그모드=True, s_시작일자='20251001')
    li_봉수 = [5]
    # ret = a.sync_소스파일()
    ret = a.find_일봉확인()
    ret = [a.make_매매신호(n_봉수=봉수) for 봉수 in li_봉수] # 매수매도 logic에 따른 신호 생성
    ret = [a.make_매수매도(n_봉수=봉수) for 봉수 in li_봉수] # 매수매도 신호 존재하는 종목의 데이터만 수집
    ret = [a.make_매매내역(n_봉수=봉수) for 봉수 in li_봉수] # 매매내역 정보만 한줄로 표기       # 서브폴더 3개 생성 리포트, 리포트누적, 종목차트
    ret = [a.make_수익내역(n_봉수=봉수) for 봉수 in li_봉수] # 시간중복 제거 후 실매매 정보 표기  # 서브폴더 3개 생성 리포트, 리포트누적, 종목차트
    ret = [a.make_수익누적(n_봉수=봉수) for 봉수 in li_봉수] # 이후꺼는 모두 삭제하고 위에랑 통합 (기대수익, 자금운영 모두 위에서 표기)
    ret = [a.make_자금운영(n_봉수=봉수) for 봉수 in li_봉수] # 이후꺼는 모두 삭제하고 위에랑 통합 (기대수익, 자금운영 모두 위에서 표기)

if __name__ == '__main__':
    try:
        run()
    except KeyboardInterrupt:
        print('\n### [ KeyboardInterrupt detected ] ###')
