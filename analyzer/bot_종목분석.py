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

import pandas as pd
from fontTools.ttLib.tables.otTables import DeltaSetIndexMap
from tqdm import tqdm

import ut.로그maker, ut.폴더manager, ut.도구manager as Tool, ut.차트maker


# noinspection NonAsciiCharacters,SpellCheckingInspection,PyPep8Naming
class AnalyzerBot:
    def __init__(self, b_디버그모드=False, s_시작일자=None):
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
        self.folder_대상종목 = dic_폴더정보['데이터|대상종목']
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
        # folder_소스 = os.path.join(self.folder_차트캐시, f'초봉1')
        # file_소스 = f'dic_차트캐시'
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
            # dic_초봉 = pd.read_pickle(os.path.join(folder_소스, f'{file_소스}_1초봉_{s_일자}.pkl'))
            # li_대상종목 = sorted(dic_초봉.keys())
            df_조회순위 = pd.read_csv(os.path.join(folder_소스, f'{file_소스}_{s_일자}.csv'), encoding='cp949', dtype=str)
            li_대상종목 = sorted(df_조회순위.dropna(subset='종목코드')['종목코드'].unique().tolist())

            # 일봉 파일 불러오기
            path_일봉 = os.path.join(self.folder_차트캐시, '일봉1', f'dic_차트캐시_1일봉_{s_일자}.pkl')
            if os.path.exists(path_일봉):
                dic_일봉 = pd.read_pickle(path_일봉)
            else:
                continue

            # 종목별 조건 확인
            li_dic상승후보 = list()
            for s_종목코드 in li_대상종목:
                # 기준정보 정의
                df_일봉 = dic_일봉[s_종목코드]
                df_일봉['전일고가3봉'] = df_일봉['고가'].shift(1).rolling(window=3).max()
                df_일봉['추세신호'] = df_일봉['종가'] > df_일봉['전일고가3봉']
                if len(df_일봉) < 2: continue
                dt_전일 = df_일봉.index[-2]
                n_전일종가 = df_일봉.loc[dt_전일, '종가']
                n_전일60 = df_일봉.loc[dt_전일, '종가ma60']
                n_전일120 = df_일봉.loc[dt_전일, '종가ma120']
                n_전일바디 = (n_전일종가 - df_일봉.loc[dt_전일, '시가']) / df_일봉.loc[dt_전일, '전일종가'] * 100

                # 조건 확인 - 전일 기준
                li_조건확인 = list()
                li_조건확인.append(True if n_전일종가 > n_전일60 > n_전일120 else False)
                li_조건확인.append(True if sum(df_일봉['추세신호'].values[-6:-1]) > 0 else False)

                # 결과 생성
                dic_상승후보 = df_일봉.iloc[-1].to_dict()
                dic_상승후보.update(전일종가=n_전일종가, 전일60=n_전일60, 전일120=n_전일120, 전일바디=n_전일바디,
                                전일조건=sum(li_조건확인)==len(li_조건확인), 전일정배열=li_조건확인[0], 전일추세5일=li_조건확인[1])
                li_dic상승후보.append(dic_상승후보)

            # df 생성
            df_상승후보 = pd.DataFrame(li_dic상승후보) if len(li_dic상승후보) > 0 else pd.DataFrame()
            df_상승후보_후보만 = df_상승후보.loc[(df_상승후보['전일조건'])
                                                & (df_상승후보['전일바디'] > 0) & (df_상승후보['전일바디'] < 2)]

            # 데이터 저장
            Tool.df저장(df=df_상승후보, path=os.path.join(folder_타겟, f'{file_타겟}_{s_일자}'))

            # 로그 기록
            self.make_로그(f'{s_일자} 완료\n - 전체 {len(df_상승후보):,.0f}종목, 상승후보 {len(df_상승후보_후보만):,.0f}종목')

    def make_매매정보(self, n_포함일수=5):
        """ 상승후보 종목 대상으로 일봉기준 매매신호 생성 후 저장 """
        # 기준정보 정의
        folder_소스 = os.path.join(self.folder_종목분석, '10_상승후보')
        file_소스 = f'df_상승후보'
        folder_타겟 = os.path.join(self.folder_종목분석, '20_매매정보')
        file_타겟 = f'df_매매정보'
        os.makedirs(folder_타겟, exist_ok=True)

        # 대상일자 확인
        li_전체일자 = sorted(re.findall(r'\d{8}', 파일)[0] for 파일 in os.listdir(folder_소스) if '.pkl' in 파일)
        li_완료일자 = [re.findall(r'\d{8}', 파일)[0] for 파일 in os.listdir(folder_타겟) if '.pkl' in 파일]
        li_대상일자 = [일자 for 일자 in li_전체일자 if 일자 not in li_완료일자]

        # 일자별 데이터 생성
        for s_일자 in li_대상일자:
            # 소스파일 불러오기
            df_상승후보 = pd.read_pickle(os.path.join(folder_소스, f'{file_소스}_{s_일자}.pkl'))
            # dic_코드2종목 = df_상승후보.set_index(['종목코드'])['종목명'].to_dict()
            df_후보만 = df_상승후보.loc[(df_상승후보['전일조건']) & (df_상승후보['전일바디'] > 0) & (df_상승후보['전일바디'] < 2)]
            li_상승후보 = df_후보만['종목코드'].tolist()

            # 일봉 불러오기
            dic_일봉 = pd.read_pickle(os.path.join(self.folder_차트캐시, '일봉1', f'dic_차트캐시_1일봉_{s_일자}.pkl'))

            # 이전 데이터 불러오기
            li_파일 = [파일 for 파일 in os.listdir(folder_타겟) if '.pkl' in 파일 and re.findall(r'\d{8}', 파일)[0] < s_일자]
            df_매매정보_전일 = pd.read_pickle(os.path.join(folder_타겟, max(li_파일))) if len(li_파일) > 0 else pd.DataFrame()
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

                # 보유종목 정보 불러오기
                b_보유종목 = s_종목코드 in df_보유종목['종목코드'].values if len(df_보유종목) > 0 else False
                df_보유종목_종목 = df_보유종목.loc[df_보유종목['종목코드'] == s_종목코드] if b_보유종목 else pd.DataFrame()
                s_보유종목_매수일 = df_보유종목_종목['매수일'].values[0] if b_보유종목 else None
                n_보유종목_매수가 = df_보유종목_종목['매수가'].values[0] if b_보유종목 else None
                n_보유종목_경과일 = df_보유종목_종목['경과일'].values[0] if b_보유종목 else None

                # 매수정보 생성
                n_매수가 = n_시가 if not b_보유종목 else n_보유종목_매수가
                s_매수일 = s_일자 if not b_보유종목 else s_보유종목_매수일
                n_경과일 = 0 if not b_보유종목 else n_보유종목_경과일 + 1

                # 매도정보 생성
                n_수익률 = (n_종가 / n_매수가 - 1) * 100 - 0.2
                n_매도가 = n_종가 if n_수익률 > 0 or n_경과일 >= 5 else None
                s_매도일 = s_일자 if n_매도가 is not None else None

                # 매매정보 정리
                dic_매매정보 = dict(일자=s_일자, 종목코드=s_종목코드, 종목명=s_종목명,
                                시가=n_시가, 고가=n_고가, 저가=n_저가, 종가=n_종가, 전일종가=n_전일종가,
                                매수일=s_매수일, 매도일=s_매도일, 경과일=n_경과일, 매수가=n_매수가, 매도가=n_매도가, 수익률=n_수익률)
                li_dic매매정보.append(dic_매매정보)

            # 데이터 정리
            df_매매정보 = pd.DataFrame(li_dic매매정보).sort_values(['매수일', '종목코드'])

            # 데이터 저장
            Tool.df저장(df=df_매매정보, path=os.path.join(folder_타겟, f'{file_타겟}_{s_일자}'))

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
        folder_소스 = os.path.join(self.folder_종목분석, '20_매매정보')
        file_소스 = f'df_매매정보'
        folder_타겟 = os.path.join(self.folder_종목분석, '30_수익정보')
        file_타겟 = f'df_수익정보'
        os.makedirs(folder_타겟, exist_ok=True)

        # 대상일자 확인
        li_전체일자 = sorted(re.findall(r'\d{8}', 파일)[0] for 파일 in os.listdir(folder_소스) if '.pkl' in 파일)
        li_완료일자 = [re.findall(r'\d{8}', 파일)[0] for 파일 in os.listdir(folder_타겟) if '.pkl' in 파일]
        li_대상일자 = [일자 for 일자 in li_전체일자 if 일자 not in li_완료일자]

        # 일자별 데이터 생성
        for s_일자 in li_대상일자:
            # 소스파일 불러오기
            df_매매정보 = pd.read_pickle(os.path.join(folder_소스, f'{file_소스}_{s_일자}.pkl'))

            # 수익정보 생성
            dic_수익정보 = dict(일자=s_일자,
                            전체종목=len(df_매매정보),
                            매수종목=len(df_매매정보.loc[df_매매정보['매수일'] == s_일자]),
                            매도종목=len(df_매매정보.loc[df_매매정보['매도일'] == s_일자]),
                            잔여종목 = len(df_매매정보.loc[pd.isna(df_매매정보['매도일'])]))
            df_매매정보_매도 = df_매매정보.loc[df_매매정보['매도일'] == s_일자]
            dic_수익정보.update(수익=df_매매정보_매도['수익률'].sum(),
                            수익0일=df_매매정보_매도.loc[df_매매정보['경과일'] == 0]['수익률'].sum(),
                            수익1일=df_매매정보_매도.loc[df_매매정보['경과일'] == 1]['수익률'].sum(),
                            수익2일=df_매매정보_매도.loc[df_매매정보['경과일'] == 2]['수익률'].sum(),
                            수익3일=df_매매정보_매도.loc[df_매매정보['경과일'] == 3]['수익률'].sum(),
                            수익4일=df_매매정보_매도.loc[df_매매정보['경과일'] == 4]['수익률'].sum(),
                            수익5일=df_매매정보_매도.loc[df_매매정보['경과일'] == 5]['수익률'].sum(),
                            잔여종목수익률=df_매매정보.loc[pd.isna(df_매매정보['매도일'])]['수익률'].sum())

            # 데이터 정리
            li_파일 = [파일 for 파일 in os.listdir(folder_타겟) if '.pkl' in 파일 and re.findall(r'\d{8}', 파일)[0] < s_일자]
            df_수익정보_이전 = pd.read_pickle(os.path.join(folder_타겟, max(li_파일))) if len(li_파일) > 0 else pd.DataFrame()
            df_수익정보_당일 = pd.DataFrame([dic_수익정보])
            df_수익정보 = pd.concat([df_수익정보_당일, df_수익정보_이전], axis=0)

            # 데이터 저장
            Tool.df저장(df=df_수익정보, path=os.path.join(folder_타겟, f'{file_타겟}_{s_일자}'))

            # 로그 기록
            n_당일수익 = df_수익정보_당일['수익'].sum()
            n_누적수익 = df_수익정보['수익'].sum()
            self.make_로그(f'{s_일자} 완료\n'
                         f' - 당일수익 {n_당일수익:,.1f}%, 누적수익 {n_누적수익:,.1f}%')


def run():
    """ 실행 함수 """
    a = AnalyzerBot(b_디버그모드=True, s_시작일자=None)
    a.sync_소스파일()
    a.find_상승후보()
    a.make_매매정보()
    a.make_수익정보()

if __name__ == '__main__':
    try:
        run()
    except KeyboardInterrupt:
        print('\n### [ KeyboardInterrupt detected ] ###')
