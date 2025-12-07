import os
import sys
import json
import time

import pandas as pd
import re
import sqlite3
from tqdm import tqdm

from pandas.core.methods.selectn import SelectNSeries

import collector.logic_데이터변환 as Logic
import ut.로그maker, ut.폴더manager, ut.도구manager as Tool
import xapi.RestAPI_kiwoom


# noinspection NonAsciiCharacters,SpellCheckingInspection,PyPep8Naming,PyAttributeOutsideInit
class CollectorBot:
    def __init__(self, s_시작일자=None):
        # config 읽어 오기
        self.folder_베이스 = os.path.dirname(os.path.abspath(__file__))
        self.folder_프로젝트 = os.path.dirname(self.folder_베이스)
        self.s_파일명 = os.path.basename(__file__).replace('.py', '')
        dic_config = json.load(open(os.path.join(self.folder_프로젝트, 'config.json'), mode='rt', encoding='utf-8'))

        # 로그 설정
        log = ut.로그maker.LogMaker(s_파일명=self.s_파일명, s_로그명='로그이름_collector')
        sys.stderr = ut.로그maker.StderrHook(path_에러로그=log.path_에러)
        self.make_로그 = log.make_로그

        # 폴더 정의
        dic_폴더정보 = ut.폴더manager.define_폴더정보()
        self.folder_차트수집 = dic_폴더정보['데이터|차트수집']
        self.folder_주식체결 = dic_폴더정보['데이터|주식체결']
        self.folder_차트캐시 = dic_폴더정보['데이터|차트캐시']
        os.makedirs(self.folder_차트캐시, exist_ok=True)

        # 추가 폴더 정의
        self.folder_일봉 = os.path.join(self.folder_차트수집, '일봉')
        self.folder_분봉 = os.path.join(self.folder_차트수집, '분봉')

        # 시작일자 정의
        n_보관기간 = int(dic_config['파일보관기간(일)_collector'])
        s_보관일자 = (pd.Timestamp.now() - pd.DateOffset(days=n_보관기간)).strftime('%Y%m%d')
        self.s_시작일자 = s_시작일자 if s_시작일자 is not None else s_보관일자

        # 기준정보 정의
        self.s_오늘 = pd.Timestamp.now().strftime('%Y%m%d')

        # 로그 기록
        self.make_로그(f'구동 시작')

    def make_일봉캐시(self):
        """ 차트수집 db 데이터를 변환하여 일봉 캐시 생성 후 저장 """
        # 전체일자 확인
        li_li테이블 = [Tool.sql불러오기(path=os.path.join(self.folder_일봉, 파일)) for 파일 in os.listdir(self.folder_일봉)
                        if 파일 >= f'ohlcv_일봉_{self.s_시작일자[:4]}.db']
        li_년월 = [re.findall(r'\d{6}', 테이블)[0] for li_테이블 in li_li테이블 for 테이블 in li_테이블]
        li_df전체 = [Tool.sql불러오기(path=os.path.join(self.folder_일봉, f'ohlcv_일봉_{년월[:4]}.db'),
                                    s_테이블명=f'ohlcv_일봉_{년월}') for 년월 in li_년월 if 년월 >= self.s_시작일자[:6]]
        df_전체 = pd.concat(li_df전체, axis=0) if len(li_df전체) > 0 else pd.DataFrame()
        li_전체일자 = sorted(일자 for 일자 in df_전체['일자'].unique() if 일자 >= self.s_시작일자) if len(df_전체) > 0 else list()

        # 일봉 캐시 생성
        for n_봉수 in [1]:
            # 기준정보 정의
            folder_캐시 = os.path.join(self.folder_차트캐시, f'일봉{n_봉수}')
            os.makedirs(folder_캐시, exist_ok=True)

            # 대상일자 확인
            li_완료일자 = [re.findall(r'\d{8}', 파일)[0] for 파일 in os.listdir(folder_캐시) if '.pkl' in 파일]
            li_대상일자 = [일자 for 일자 in li_전체일자 if 일자 not in li_완료일자]

            # 일자별 캐시 생성
            for s_일자 in li_대상일자:
                # 캐시 생성
                dic_캐시 = self._캐시생성(s_봉구분='일봉', n_봉수=n_봉수, s_일자=s_일자)

                # 캐시 저장
                pd.to_pickle(dic_캐시, os.path.join(folder_캐시, f'dic_차트캐시_{n_봉수}일봉_{s_일자}.pkl'))

                # 로그 기록
                self.make_로그(f'저장 완료 - {n_봉수}일봉 - {s_일자} - {len(dic_캐시):,.0f}종목')

    def make_분봉캐시(self):
        """ 차트수집 db 데이터를 변환하여 분봉 캐시 생성 후 저장 """
        # 전체일자 확인
        li_li테이블 = [Tool.sql불러오기(path=os.path.join(self.folder_분봉, 파일)) for 파일 in os.listdir(self.folder_분봉)
                        if 파일 >= f'ohlcv_분봉_{self.s_시작일자[:6]}.db']
        li_년월일 = [re.findall(r'\d{8}', 테이블)[0] for li_테이블 in li_li테이블 for 테이블 in li_테이블]
        li_df전체 = [Tool.sql불러오기(path=os.path.join(self.folder_분봉, f'ohlcv_분봉_{년월일[:4]}_{년월일[4:6]}.db'),
                                    s_테이블명=f'ohlcv_분봉_{년월일}') for 년월일 in li_년월일 if 년월일 >= self.s_시작일자]
        df_전체 = pd.concat(li_df전체, axis=0) if len(li_df전체) > 0 else pd.DataFrame()
        li_전체일자 = sorted(일자 for 일자 in df_전체['일자'].unique() if 일자 >= self.s_시작일자) if len(df_전체) > 0 else list()

        # 일봉 캐시 생성
        for n_봉수 in [1, 3, 5, 10]:
            # 기준정보 정의
            folder_캐시 = os.path.join(self.folder_차트캐시, f'분봉{n_봉수}')
            os.makedirs(folder_캐시, exist_ok=True)

            # 대상일자 확인
            li_완료일자 = [re.findall(r'\d{8}', 파일)[0] for 파일 in os.listdir(folder_캐시) if '.pkl' in 파일]
            li_대상일자 = [일자 for 일자 in li_전체일자 if 일자 not in li_완료일자]

            # 일자별 캐시 생성
            for s_일자 in li_대상일자:
                # 캐시 생성
                dic_캐시 = self._캐시생성(s_봉구분='분봉', n_봉수=n_봉수, s_일자=s_일자)

                # 캐시 저장
                pd.to_pickle(dic_캐시, os.path.join(folder_캐시, f'dic_차트캐시_{n_봉수}분봉_{s_일자}.pkl'))

                # 로그 기록
                self.make_로그(f'저장 완료 - {n_봉수}분봉 - {s_일자} - {len(dic_캐시):,.0f}종목')

    def make_초봉캐시(self):
        """ 주식체결 데이터를 변환하여 초봉 캐시 생성 후 저장 """
        # 전체일자 확인
        li_전체일자 = sorted(re.findall(r'\d{8}', 파일)[0] for 파일 in os.listdir(self.folder_주식체결)
                         if '.csv' in 파일) if os.path.exists(self.folder_주식체결) else list()
        li_전체일자 = [일자 for 일자 in li_전체일자 if 일자 >= self.s_시작일자]

        # 초봉 캐시 생성
        for n_봉수 in [1, 2, 3, 5, 10, 12, 15, 20, 30]:
            # 기준정보 정의
            folder_캐시 = os.path.join(self.folder_차트캐시, f'초봉{n_봉수}')
            os.makedirs(folder_캐시, exist_ok=True)

            # 대상일자 확인
            li_완료일자 = [re.findall(r'\d{8}', 파일)[0] for 파일 in os.listdir(folder_캐시) if '.pkl' in 파일]
            li_대상일자 = [일자 for 일자 in li_전체일자 if 일자 not in li_완료일자]

            # 일자별 캐시 생성
            for s_일자 in li_대상일자:
                # 주식체결 읽어오기
                path_주식체결 = os.path.join(self.folder_주식체결, f'주식체결_{s_일자}.csv')
                df_주식체결 = pd.read_csv(path_주식체결, encoding='cp949', dtype=str)
                df_주식체결 = df_주식체결[df_주식체결['장구분'] == '장중']

                # 캐시 데이터 생성
                dic_캐시 = Logic.make_초봉데이터(df_주식체결=df_주식체결, s_일자=s_일자, n_봉수=n_봉수)

                # 데이터 미 존재 시 처리
                if len(dic_캐시) == 0:
                    continue

                # 캐시 저장
                pd.to_pickle(dic_캐시, os.path.join(folder_캐시, f'dic_차트캐시_{n_봉수}초봉_{s_일자}.pkl'))

                # 로그 기록
                self.make_로그(f'저장 완료 - {n_봉수}초봉 - {s_일자} - {len(dic_캐시):,.0f}종목')

    def _캐시생성(self, s_봉구분, n_봉수, s_일자):
        """ db 조회하여 캐시데이터 생성 후 리턴 """
        # 기준일자 설정
        s_기준일자 = (pd.Timestamp(s_일자) - pd.DateOffset(months=7)).strftime('%Y%m%d') if s_봉구분 == '일봉' else\
                    ((pd.Timestamp(s_일자) - pd.DateOffset(months=1)).strftime('%Y%m%d')) if s_봉구분 == '분봉' else None

        # db 테이블 불러오기
        li_df차트 = list()
        if s_봉구분 == '일봉':
            li_li테이블 = [Tool.sql불러오기(path=os.path.join(self.folder_일봉, 파일)) for 파일 in os.listdir(self.folder_일봉)
                        if f'ohlcv_일봉_{s_기준일자[:4]}.db' <= 파일 <= f'ohlcv_일봉_{s_일자[:4]}.db']
            li_년월 = [re.findall(r'\d{6}', 테이블)[0] for li_테이블 in li_li테이블 for 테이블 in li_테이블
                        if f'ohlcv_일봉_{s_기준일자[:6]}' <= 테이블 <= f'ohlcv_일봉_{s_일자[:6]}']
            li_df차트 = [Tool.sql불러오기(path=os.path.join(self.folder_일봉, f'ohlcv_일봉_{년월[:4]}.db'),
                                        s_테이블명=f'ohlcv_일봉_{년월}') for 년월 in li_년월]
        if s_봉구분 == '분봉':
            li_li테이블 = [Tool.sql불러오기(path=os.path.join(self.folder_분봉, 파일)) for 파일 in os.listdir(self.folder_분봉)
                        if f'ohlcv_분봉_{s_기준일자[:4]}_{s_기준일자[4:6]}.db' <= 파일 <= f'ohlcv_분봉_{s_일자[:4]}_{s_일자[4:6]}.db']
            li_년월일 = [re.findall(r'\d{8}', 테이블)[0] for li_테이블 in li_li테이블 for 테이블 in li_테이블
                        if f'ohlcv_분봉_{s_기준일자}' <= 테이블 <= f'ohlcv_분봉_{s_일자}']
            li_df차트 = [Tool.sql불러오기(path=os.path.join(self.folder_분봉, f'ohlcv_분봉_{년월일[:4]}_{년월일[4:6]}.db'),
                                        s_테이블명=f'ohlcv_분봉_{년월일}') for 년월일 in li_년월일 if s_기준일자 <= 년월일 <= s_일자]

        # 차트데이터 생성
        df_차트 = pd.concat(li_df차트, axis=0)
        df_차트 = df_차트[df_차트['일자'] >= s_기준일자]
        df_차트 = df_차트[df_차트['일자'] <= s_일자]
        gr_차트 = df_차트.groupby('종목코드')

        # 분봉 전일종가용 데이터 준비
        dic_일봉 = dict()
        if s_봉구분 == '분봉':
            path_일봉 = os.path.join(self.folder_차트캐시, '1일봉', f'dic_차트캐시_1일봉_{s_일자}')
            dic_일봉 = pd.read_pickle(path_일봉) if os.path.exists(path_일봉) else dict()

        # 종목별 캐시 생성
        dic_캐시 = dict()
        for s_종목코드, df_종목 in tqdm(gr_차트, desc=f'캐시생성|{n_봉수}{s_봉구분}-{s_일자}', file=sys.stdout):
            # df 정리 - 오름차순
            df_종목 = df_종목.drop_duplicates().sort_values('일자') if s_봉구분 == '일봉' else\
                    df_종목.drop_duplicates().sort_values(['일자', '시간']) if s_봉구분 == '분봉' else pd.DataFrame()

            # 인덱스 설정
            df_종목['인덱스'] = pd.to_datetime(df_종목['일자'], format='%Y%m%d') if s_봉구분 == '일봉' else\
                                pd.to_datetime(df_종목['일자'] + ' ' + df_종목['시간'], format='%Y%m%d %H:%M:%S')\
                                                                            if s_봉구분 == '분봉' else None
            df_종목 = df_종목.set_index('인덱스')

            # 봉수 변환
            s_리샘플 = f'{n_봉수}D' if s_봉구분 == '일봉' else\
                        f'{n_봉수}min' if s_봉구분 == '분봉' else None
            df_리샘플 = df_종목.resample(s_리샘플)
            df_종목 = df_리샘플.first().copy()
            df_종목['시가'] = df_리샘플['시가'].first()
            df_종목['고가'] = df_리샘플['고가'].max()
            df_종목['저가'] = df_리샘플['저가'].min()
            df_종목['종가'] = df_리샘플['종가'].last()
            df_종목['거래량'] = df_리샘플['거래량'].sum()
            df_종목 = df_종목.dropna(subset=['일자']) if s_봉구분 == '일봉' else\
                        df_종목.dropna(subset=['시간']) if s_봉구분 == '분봉' else pd.DataFrame()
            if s_봉구분 == '분봉':
                df_종목['시간'] = df_종목.index.strftime('%H:%M:%S')

            # 분봉 전일종가용 데이터 준비
            dic_전일종가 = dict()
            if s_봉구분 == '분봉':
                df_일봉 = dic_일봉[s_종목코드] if s_종목코드 in dic_일봉.keys() else pd.DataFrame()
                dic_전일종가 = df_일봉.set_index('일자')['전일종가'].to_dict() if len(df_일봉) > 0 else dict()

            # 추가 데이터 생성 - 전일종가
            df_종목['전일종가'] = df_종목['종가'].shift(1) if s_봉구분 == '일봉' else\
                                dic_전일종가[s_일자] if s_봉구분 == '분봉' and len(dic_전일종가) > 0 else None
            df_종목['전일대비(%)'] = (df_종목['종가'] / df_종목['전일종가'] - 1) * 100

            # 추가 데이터 생성 - 이동평균
            df_종목['종가ma5'] = df_종목['종가'].rolling(5).mean()
            df_종목['종가ma10'] = df_종목['종가'].rolling(10).mean()
            df_종목['종가ma20'] = df_종목['종가'].rolling(20).mean()
            df_종목['종가ma60'] = df_종목['종가'].rolling(60).mean()
            df_종목['종가ma120'] = df_종목['종가'].rolling(120).mean()
            df_종목['거래량ma5'] = df_종목['거래량'].rolling(5).mean()
            df_종목['거래량ma20'] = df_종목['거래량'].rolling(20).mean()
            df_종목['거래량ma60'] = df_종목['거래량'].rolling(60).mean()
            df_종목['거래량ma120'] = df_종목['거래량'].rolling(120).mean()

            # 해당 일자 골라내기
            df_종목 = df_종목[-25:] if s_봉구분 == '일봉' else\
                        df_종목[df_종목['일자'] == s_일자] if s_봉구분 == '분봉' else None

            # dic에 추가
            dic_캐시[s_종목코드] = df_종목

        return dic_캐시


def run():
    """ 실행 함수 """
    c = CollectorBot(s_시작일자=None)
    c.make_일봉캐시()
    c.make_분봉캐시()
    c.make_초봉캐시()


if __name__ == '__main__':
    try:
        run()
    except KeyboardInterrupt:
        print('\n### [ KeyboardInterrupt detected ] ###')
