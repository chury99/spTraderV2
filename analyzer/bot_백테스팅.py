import os
import sys
import json
import time
import re
import multiprocessing as mp

# win용 디버거 설정
if sys.platform == 'win32':
    import matplotlib
    matplotlib.use('TkAgg')

import numpy as np
import pandas as pd
from tqdm import tqdm
import matplotlib.pyplot as plt
import paramiko

import ut.로그maker, ut.폴더manager, ut.도구manager as Tool, ut.차트maker
import analyzer.logic_매수매도 as Logic
import xapi.RestAPI_kiwoom, xapi.WebsocketAPI_kiwoom


# noinspection NonAsciiCharacters,SpellCheckingInspection,PyPep8Naming
class AnalyzerBot:
    def __init__(self, b_디버그모드=False):
        # config 읽어 오기
        self.folder_베이스 = os.path.dirname(os.path.abspath(__file__))
        self.folder_프로젝트 = os.path.dirname(self.folder_베이스)
        self.s_파일명 = os.path.basename(__file__).replace('.py', '')
        # dic_config = json.load(open(os.path.join(self.folder_프로젝트, 'config.json'), mode='rt', encoding='utf-8'))
        dic_config = ut.도구manager.config로딩()

        # 로그 설정
        log = ut.로그maker.LogMaker(s_파일명=self.s_파일명, s_로그명='로그이름_analyzer')
        sys.stderr = ut.로그maker.StderrHook(path_에러로그=log.path_에러)
        self.make_로그 = log.make_로그

        # 폴더 정의
        dic_폴더정보 = ut.폴더manager.define_폴더정보()
        self.folder_work = dic_폴더정보['folder_work']
        self.folder_차트캐시 = dic_폴더정보['데이터|차트캐시']
        self.folder_전체종목 = dic_폴더정보['데이터|전체종목']
        self.folder_조회순위 = dic_폴더정보['데이터|조회순위']
        self.folder_백테스팅 = dic_폴더정보['분석|백테스팅']
        os.makedirs(self.folder_차트캐시, exist_ok=True)
        os.makedirs(self.folder_전체종목, exist_ok=True)
        os.makedirs(self.folder_조회순위, exist_ok=True)
        os.makedirs(self.folder_백테스팅, exist_ok=True)

        # 추가 폴더 정의
        self.folder_매매신호 = os.path.join(self.folder_백테스팅, '10_매매신호')
        self.folder_매수매도 = os.path.join(self.folder_백테스팅, '20_매수매도')
        self.folder_매매내역 = os.path.join(self.folder_백테스팅, '30_매매내역')
        self.folder_수익내역 = os.path.join(self.folder_백테스팅, '40_수익내역')
        self.folder_수익요약 = os.path.join(self.folder_백테스팅, '50_수익요약')
        os.makedirs(self.folder_매매신호, exist_ok=True)
        os.makedirs(self.folder_매수매도, exist_ok=True)
        os.makedirs(self.folder_매매내역, exist_ok=True)
        os.makedirs(self.folder_수익내역, exist_ok=True)
        os.makedirs(self.folder_수익요약, exist_ok=True)

        # 기준정보 정의
        self.s_오늘 = pd.Timestamp.now().strftime('%Y%m%d')
        self.b_디버그모드 = b_디버그모드
        self.n_멀티코어수 = mp.cpu_count() - 2
        self.dic_args = dict()

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
        # li_대상폴더 = [self.folder_전체종목, self.folder_조회순위]
        # li_대상폴더_초봉 = [os.path.join(self.folder_차트캐시, 폴더) for 폴더 in os.listdir(self.folder_차트캐시) if '초봉' in 폴더]
        # li_로컬폴더 = sorted(li_대상폴더 + li_대상폴더_초봉)
        li_대상폴더 = [self.folder_전체종목, self.folder_조회순위, self.folder_차트캐시]

        # 폴더별 동기화
        li_동기화파일명 = list()
        for s_로컬폴더 in li_대상폴더:
            # 기준정보 정의
            s_서버폴더 = f'{self.dic_서버폴더['server_work']}{s_로컬폴더.replace(self.folder_work, '')}'
            s_서버폴더 = s_서버폴더.replace('\\', '/')

            # 파일 동기화
            # li_동기화파일명_개별 = Tool.sftp_동기화_파일명(folder_로컬=s_로컬폴더, folder_서버=s_서버폴더, s_모드='서버2로컬',
            #                                         s_기준일='20251001')
            li_동기화파일명_개별 = Tool.sftp폴더동기화(folder_로컬=s_로컬폴더, folder_서버=s_서버폴더, s_모드='서버2로컬',
                                                    s_기준일='20251001')
            li_동기화파일명 = li_동기화파일명 + li_동기화파일명_개별

        # 로그 기록
        s_동기화파일명 = ''.join(f' - {파일명}\n' for 파일명 in li_동기화파일명)
        self.make_로그(f'{len(li_동기화파일명):,.0f}개 파일 완료\n'
                      f'{s_동기화파일명}')

    def make_매매신호(self, n_봉수):
        """ 초봉 데이터 기준 매수/매도 신호 생성 """
        # 기준정보 정의
        folder_소스 = os.path.join(self.folder_차트캐시, f'초봉{n_봉수}')
        folder_타겟 = self.folder_매매신호
        file_소스 = f'dic_차트캐시'
        file_타겟 = f'dic_매매신호'

        # 대상일자 확인
        li_전체일자 = sorted(re.findall(r'\d{8}', 파일)[0] for 파일 in os.listdir(folder_소스)
                        if file_소스 in 파일 and f'{n_봉수}초봉' in 파일)
        li_완료일자 = [re.findall(r'\d{8}', 파일)[0] for 파일 in os.listdir(folder_타겟)
                        if file_타겟 in 파일 and f'{n_봉수}초봉'in 파일]
        li_대상일자 = [일자 for 일자 in li_전체일자 if 일자 not in li_완료일자]
        li_대상일자 = li_대상일자[:2]

        # 일자별 매수매도 정보 생성
        for s_일자 in li_대상일자:
            # 전체종목 불러오기
            df_전체종목 = pd.read_pickle(os.path.join(self.folder_전체종목, f'df_전체종목_{s_일자}.pkl'))
            dic_코드2종목 = df_전체종목.set_index(['종목코드'])['종목명'].to_dict()

            # 초봉캐시 불러오기
            dic_초봉 = pd.read_pickle(os.path.join(folder_소스, f'{file_소스}_{n_봉수}초봉_{s_일자}.pkl'))
            li_대상종목 = list(dic_초봉.keys())

            # 1초봉 불러오기
            dic_1초봉 = pd.read_pickle(os.path.join(folder_소스, f'{file_소스}_1초봉_{s_일자}.pkl')) if n_봉수 > 1 else dic_초봉

            # 매개변수 정의 - 종목별 함수 전달용
            self.dic_args = dict(n_봉수=n_봉수, s_일자=s_일자, dic_초봉=dic_초봉, dic_코드2종목=dic_코드2종목, dic_1초봉=dic_1초봉,
                                 file_타겟=file_타겟)

            # 종목별 매수매도 정보 생성
            li_df매매신호 = list()
            if self.b_디버그모드:
                for s_종목코드 in tqdm(li_대상종목, desc=f'매수매도-{n_봉수}초봉-{s_일자}', file=sys.stdout):
                    li_df매매신호.append(self._make_매매신호_종목(s_종목코드=s_종목코드))
            else:
                with mp.Pool(processes=self.n_멀티코어수) as pool:
                    li_df매매신호 = list(tqdm(pool.imap(self._make_매매신호_종목, li_대상종목),
                                          total=len(li_대상종목), desc=f'매수매도-{n_봉수}초봉-{s_일자}', file=sys.stdout))
            dic_매매신호 = dict(zip(li_대상종목, li_df매매신호))

            # 결과파일 저장
            pd.to_pickle(dic_매매신호, os.path.join(folder_타겟, f'{file_타겟}_{s_일자}_{n_봉수}초봉.pkl'))

            # 로그 기록
            self.make_로그(f'{s_일자} 완료\n - {len(dic_매매신호):,.0f}개 종목')

    def make_매수매도(self, n_봉수):
        """ 매수/매도 신호 기준으로 보유시점의 데이터만 정리 """
        # 기준정보 정의
        folder_소스 = self.folder_매매신호
        folder_타겟 = self.folder_매수매도
        file_소스 = f'dic_매매신호'
        file_타겟 = f'df_매수매도'

        # 대상일자 확인
        li_전체일자 = sorted(re.findall(r'\d{8}', 파일)[0] for 파일 in os.listdir(folder_소스)
                        if file_소스 in 파일 and f'{n_봉수}초봉' in 파일)
        li_완료일자 = [re.findall(r'\d{8}', 파일)[0] for 파일 in os.listdir(folder_타겟)
                        if file_타겟 in 파일 and f'{n_봉수}초봉'in 파일]
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
        folder_소스 = self.folder_매수매도
        folder_타겟 = self.folder_매매내역
        file_소스 = f'df_매수매도'
        file_타겟 = f'df_매매내역'

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
        folder_소스 = self.folder_매매내역
        folder_타겟 = self.folder_수익내역
        file_소스 = f'df_매매내역'
        file_타겟 = f'df_수익내역'

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
            n_거래종목수 = len(df_수익내역['종목코드'].unique())
            n_거래건수 = len(df_수익내역)
            n_수익률 = df_수익내역['수익률'].sum()
            self.make_로그(f'{s_일자} 완료\n - {n_거래종목수:,.0f}종목, 거래 {n_거래건수:,.0f}건, 수익 {n_수익률:,.1f}%')

    def make_수익요약(self):
        """ 수익 요약 및 리포트 발행 """
        pass

    def _make_매매신호_종목(self, s_종목코드):
        """ 종목별 매수매도 정보 생성 후 리턴 """
        # 기준정보 정의
        file_타겟 = self.dic_args['file_타겟']
        n_봉수 = self.dic_args['n_봉수']
        s_일자 = self.dic_args['s_일자']
        dic_코드2종목 = self.dic_args['dic_코드2종목']
        s_종목명 = dic_코드2종목.get(s_종목코드, None)
        df_초봉 = self.dic_args['dic_초봉'].get(s_종목코드, pd.DataFrame())
        df_1초봉 = self.dic_args['dic_1초봉'].get(s_종목코드, pd.DataFrame())

        # 데이터 미존재 처리
        if df_초봉.empty or df_1초봉.empty:
            return pd.DataFrame()

        # df_매매신호 생성
        df_매매신호 = df_초봉.copy()
        df_매매신호['일자'] = s_일자
        df_매매신호['종목명'] = s_종목명
        df_매매신호['현재가'] = df_1초봉['종가']
        df_매매신호['현재시점'] = df_매매신호.index.strftime('%H:%M:%S')

        # 매수매도 신호 생성
        df_기준봉 = df_초봉.shift(1)
        df_매매신호['dic_매수신호'] = [Logic.judge_매수신호(df_기준봉=df_기준봉.loc[[idx]])
                                    for idx in df_매매신호.index]

        df_매매신호['dic_매도신호'] = [Logic.judge_매도신호(df_기준봉=df_기준봉.loc[[idx]], dic_args=dic_args)
                                    for idx in df_매매신호.index]

        dic_args_종목.update(매도봇_s_탐색시간=s_시점, 매도봇_n_현재가=n_현재가,
                           매도봇_s_매수시간=dic_args_종목['매수봇_s_주문시간'],
                           매도봇_n_매수단가=dic_args_종목['매수봇_n_주문단가'],
                           매도봇_n_보유수량=dic_args_종목['매수봇_n_주문수량'])


        dic_매수신호_기준 = df_매매신호['dic_매수신호'].values[0]

        dic_매도신호 = Logic.judge_매도신호(df_기준봉=df_기준봉, dic_args=dic_args)

        # 매수매도 신호 상세 추가
        for i, 신호종류 in enumerate(dic_매수신호_기준['매수봇_li_신호종류']):
            df_매매신호[f'매수_{신호종류}'] = df_매매신호['dic_매수신호'].apply(lambda dic: dic['매수봇_li_매수신호'][i])

        df_매매신호['매수신호'] = df_매매신호['dic_매수신호'].apply(lambda x: x['매수봇_b_매수신호'])

        # 결과 정리
        # dic_매매신호_추가 = dict(일자=s_일자, 종목명=s_종목명)
        # dic_매매신호_추가.update({컬럼: df_초봉.loc[dt_시점, 컬럼] for 컬럼 in df_초봉.columns})
        # dic_매매신호_추가.update({f'매수_{신호종류}': dic_args_종목['매수봇_li_매수신호'][i]
        #                     for i, 신호종류 in enumerate(dic_args_종목['매수봇_li_신호종류'])})
        dic_매매신호_추가.update({f'매도_{신호종류}': dic_args_종목['매도봇_li_매도신호'][i] if b_보유신호 else None
                            for i, 신호종류 in enumerate(['매수금액', '매수횟수', '하락한계', '타임아웃'])})
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


        df_매매신호['매수신호'] = df_매매신호['매수봇_b_매수신호'] & (df_매매신호['현재시점'] < '15:00:00')

        # 3. 보유/매수/매도 시점 정보 생성 (벡터화)
        df_매매신호['매수블록'] = (df_매매신호['매수신호'] != df_매매신호['매수신호'].shift(1)).cumsum()
        df_매매신호['보유신호'] = df_매매신호.groupby('매수블록')['매수신호'].transform('cummax')
        df_매매신호.loc[df_매매신호['매수신호'], '보유신호'] = True

        df_매매신호['매수시간'] = df_매매신호.groupby('매수블록')['현재시점'].transform('first')
        df_매매신호['매수가'] = df_매매신호.groupby('매수블록')['현재가'].transform('first')
        df_매매신호.loc[~df_매매신호['보유신호'], ['매수시간', '매수가']] = None

        # 4. 매도신호 생성 (벡터화)
        dic_args = {
            'n_봉수': n_봉수,
            '매도봇_s_매수시간': df_매매신호['매수시간'],
            '매도봇_s_탐색시간': df_매매신호['현재시점'],
            '매도봇_n_매수단가': df_매매신호['매수가'],
            '매도봇_n_현재가': df_매매신호['현재가']
        }
        dic_매도신호 = Logic.judge_매도신호(df_기준봉=df_기준봉, dic_args=dic_args)
        for key, value in dic_매도신호.items():
            df_매매신호[key] = value

        df_매매신호['매도신호'] = df_매매신호['매도봇_b_매도신호'] & df_매매신호['보유신호']

        # 5. 매도 시점 정보 생성 (벡터화)
        df_매매신호['매도블록'] = (df_매매신호['매도신호'] != df_매매신호['매도신호'].shift(1)).cumsum()
        df_매매신호['매도시간'] = df_매매신호.loc[df_매매신호['매도신호']].groupby('매수블록')['현재시점'].transform('first')
        df_매매신호['매도가'] = df_매매신호.loc[df_매매신호['매도신호']].groupby('매수블록')['현재가'].transform('first')
        df_매매신호['매도사유'] = df_매매신호.loc[df_매매신호['매도신호']].groupby('매수블록')['매도봇_li_신호종류'].transform('first')

        # 6. 최종 결과 정리
        df_매매신호['매수시간'] = df_매매신호['매수시간'].ffill()
        df_매매신호['매수가'] = df_매매신호['매수가'].ffill()
        df_매매신호['매도시간'] = df_매매신호.groupby('매수블록')['매도시간'].ffill()
        df_매매신호['매도가'] = df_매매신호.groupby('매수블록')['매도가'].ffill()
        df_매매신호['매도사유'] = df_매매신호.groupby('매수블록')['매도사유'].ffill()

        # 보유신호 재정의 (매도 발생 후에는 보유 해제)
        df_매매신호['매도발생'] = df_매매신호.groupby('매수블록')['매도신호'].transform('cummax')
        df_매매신호.loc[df_매매신호['매도발생'], '보유신호'] = False

        # 컬럼 정리
        df_매매신호['일자'] = s_일자
        df_매매신호['종목명'] = s_종목명
        li_컬럼명_앞 = ['일자', '종목코드', '종목명']
        df_매매신호 = df_매매신호.loc[:, li_컬럼명_앞 + [컬럼 for 컬럼 in df_매매신호.columns if 컬럼 not in li_컬럼명_앞]]

        # csv 저장
        folder = os.path.join(f'{self.folder_매매신호}_종목별', f'매매신호_{s_일자}')
        os.makedirs(folder, exist_ok=True)
        df_매매신호.to_csv(os.path.join(folder, f'{file_타겟}_{s_일자}_{n_봉수}초봉_{s_종목코드}_{s_종목명}.csv'), index=False, encoding='cp949')

        return df_매매신호

    def _make_매매신호_종목_느린버전(self, s_종목코드):
        """ 종목별 매수매도 정보 생성 후 리턴 """
        # 기준정보 정의
        file_타겟 = self.dic_args['file_타겟']
        n_봉수 = self.dic_args['n_봉수']
        s_일자 = self.dic_args['s_일자']
        dic_코드2종목 = self.dic_args['dic_코드2종목']
        dic_초봉 = self.dic_args['dic_초봉']
        dic_1초봉 = self.dic_args['dic_1초봉']
        s_종목명 = dic_코드2종목.get(s_종목코드, None)
        df_초봉 = dic_초봉.get(s_종목코드, pd.DataFrame())
        df_1초봉 = dic_1초봉.get(s_종목코드, pd.DataFrame())

        # 종목별 args 생성
        dic_args_종목 = self.dic_args.get(s_종목코드, dict())
        dic_args_종목.update(s_종목코드= s_종목코드, s_종목명=s_종목명, n_봉수=n_봉수, s_일자=s_일자, df_초봉=df_초봉)

        # 매수매도 정보 생성
        b_보유신호, b_매수신호, b_매도신호 = False, False, False
        dic_매매신호 = dict()
        for dt_시점 in df_초봉.index:
            # 기준정보 확인
            s_시점 = dt_시점.strftime('%H:%M:%S')
            n_현재가 = df_1초봉.loc[dt_시점, '종가']

            # 기준봉 준비 - 현재시점 이전 봉 데이터
            df_기준봉 = df_초봉[df_초봉.index < dt_시점].copy()
            df_기준봉 = df_기준봉[-1:]

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
                dic_args_종목.update(매도봇_s_탐색시간=s_시점, 매도봇_n_현재가=n_현재가,
                                   매도봇_s_매수시간=dic_args_종목['매수봇_s_주문시간'],
                                   매도봇_n_매수단가=dic_args_종목['매수봇_n_주문단가'],
                                   매도봇_n_보유수량=dic_args_종목['매수봇_n_주문수량'])
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
                                    for i, 신호종류 in enumerate(['매수금액', '매수횟수', '하락한계', '타임아웃'])})
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
            self.dic_args[s_종목코드] = dic_args_종목

        # 결과 정리
        df_매매신호 = pd.DataFrame(dic_매매신호).sort_index()
        li_컬럼명_앞 = ['일자', '종목코드', '종목명']
        df_매매신호 = df_매매신호.loc[:, li_컬럼명_앞 + [컬럼 for 컬럼 in df_매매신호.columns if 컬럼 not in li_컬럼명_앞]]

        # csv 저장
        folder = os.path.join(f'{self.folder_매매신호}_종목별', f'매매신호_{s_일자}')
        os.makedirs(folder, exist_ok=True)
        df_매매신호.to_csv(os.path.join(folder, f'{file_타겟}_{s_일자}_{n_봉수}초봉_{s_종목코드}_{s_종목명}.csv'),
                            index=False, encoding='cp949')

        return df_매매신호

    def _report_매매이력(self, df_매매이력):
        """ 매매이력 데이터 기준으로 리포트 생성 후 저장 """
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

            for s_매도사유 in ['매수금액', '매수횟수', '하락한계', '타임아웃']:
                df_매매이력_종목_매도사유 = df_매매이력_종목[df_매매이력_종목['매도사유'] == s_매도사유]
                dic_리포트.update({
                    f'{s_매도사유}_거래수': len(df_매매이력_종목_매도사유),
                    f'{s_매도사유}_수익률sum': df_매매이력_종목_매도사유['수익률'].sum() if not df_매매이력_종목_매도사유.empty else None,
                    f'{s_매도사유}_보유초mean': df_매매이력_종목_매도사유['보유초'].mean()
                })

            # df 변환 및 추가
            li_df리포트.append(pd.DataFrame({key: [value] for key, value in dic_리포트.items()}))

        # 결과 생성
        df_리포트 = pd.concat(li_df리포트, axis=0)

        return df_리포트


# noinspection PyNoneFunctionAssignment,NonAsciiCharacters
def run():
    """ 실행 함수 """
    a = AnalyzerBot(b_디버그모드=True)
    ret = a.sync_소스파일()
    ret = [a.make_매매신호(n_봉수=봉수) for 봉수 in [1]]
    ret = [a.make_매수매도(n_봉수=봉수) for 봉수 in [1]]
    ret = [a.make_매매내역(n_봉수=봉수) for 봉수 in [1]]
    ret = [a.make_수익내역(n_봉수=봉수) for 봉수 in [1]]
    ret = a.make_수익요약()

if __name__ == '__main__':
    try:
        run()
    except KeyboardInterrupt:
        print('\n### [ KeyboardInterrupt detected ] ###')
