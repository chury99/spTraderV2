import os
import sys
import json

import pandas as pd
from tqdm import tqdm


# noinspection PyPep8Naming,SpellCheckingInspection,NonAsciiCharacters
def make_초봉데이터(df_주식체결, s_일자, n_봉수, n_거래량제외기준=10):
    """ 입력받은 데이터를 초봉 데이터로 변환 후 df 리턴 """
    # 데이터 변환
    df_주식체결 = df_주식체결.set_index(pd.to_datetime(s_일자 + ' ' + df_주식체결['체결시간'], format='%Y%m%d %H%M%S'))
    # df_주식체결 = df_주식체결.astype({컬럼: float for 컬럼 in ['등락율', '체결강도', '전일거래량대비비율', '매수비율']})
    # df_주식체결 = df_주식체결.astype({컬럼: int for 컬럼 in ['현재가', '거래량', '누적거래량', '누적거래대금', '시가', '고가', '저가',
    #                                                     '매도체결량', '매수체결량', '매도체결건수', '매수체결건수']})
    df_주식체결 = df_주식체결.astype({컬럼: int for 컬럼 in ['현재가', '거래량']})

    # 기준 이하 거래량 제외
    df_주식체결_보정 = df_주식체결[(df_주식체결['거래량'].abs() > n_거래량제외기준)]

    # 종목별 초봉데이터 생성
    gr_주식체결 = df_주식체결_보정.groupby('종목코드')
    dic_초봉 = dict()
    for s_종목코드 in tqdm(gr_주식체결.groups, desc=f'캐시생성|{n_봉수}초봉-{s_일자}', file=sys.stdout):
        # 기준 정보 정의
        df_주식체결_종목 = gr_주식체결.get_group(s_종목코드).copy()

        # 매수매도 분리
        df_주식체결_종목_매수 = df_주식체결_종목[df_주식체결_종목['거래량'] > 0].copy()
        df_주식체결_종목_매도 = df_주식체결_종목[df_주식체결_종목['거래량'] < 0].copy()

        # 거래량 부호 제거
        df_주식체결_종목['거래량'] = df_주식체결_종목['거래량'].abs()
        df_주식체결_종목_매수['거래량'] = df_주식체결_종목_매수['거래량'].abs()
        df_주식체결_종목_매도['거래량'] = df_주식체결_종목_매도['거래량'].abs()

        # 리샘플 생성
        df_리샘플 = df_주식체결_종목.resample(f'{n_봉수}s')
        df_리샘플_매수 = df_주식체결_종목_매수.resample(f'{n_봉수}s')
        df_리샘플_매도 = df_주식체결_종목_매도.resample(f'{n_봉수}s')

        # 초봉 생성
        df_초봉 = df_리샘플.first().loc[:, ['종목코드', '체결시간']].copy()
        df_초봉['시가'] = df_리샘플['현재가'].first()
        df_초봉['고가'] = df_리샘플['현재가'].max()
        df_초봉['저가'] = df_리샘플['현재가'].min()
        df_초봉['종가'] = df_리샘플['현재가'].last()
        df_초봉['거래량'] = df_리샘플['거래량'].sum()
        df_초봉['매수량'] = df_리샘플_매수['거래량'].sum()
        df_초봉['매도량'] = df_리샘플_매도['거래량'].sum()
        df_초봉['체결횟수'] = df_리샘플['거래량'].count()
        df_초봉['매수횟수'] = df_리샘플_매수['거래량'].count()
        df_초봉['매도횟수'] = df_리샘플_매도['거래량'].count()

        # nan 처리
        df_초봉['종목코드'] = s_종목코드
        df_초봉['체결시간'] = df_초봉.index.strftime('%H:%M:%S')
        df_초봉['종가'] = df_초봉['종가'].ffill().astype(int)
        for s_컬럼명 in ['시가', '고가', '저가']:
            df_초봉[s_컬럼명] = df_초봉[s_컬럼명].fillna(df_초봉['종가']).astype(int)
        for s_컬럼명 in ['거래량', '매수량', '매도량', '체결횟수', '매수횟수', '매도횟수']:
            df_초봉[s_컬럼명] = df_초봉[s_컬럼명].fillna(0).astype(int)

        # dic_초봉에 추가
        dic_초봉[s_종목코드] = df_초봉

    return dic_초봉
