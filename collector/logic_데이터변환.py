import os
import sys
import json

import pandas as pd
from tqdm import tqdm


# noinspection PyPep8Naming,SpellCheckingInspection,NonAsciiCharacters
def make_초봉데이터(df_주식체결, s_일자, n_봉수):
    """ 입력받은 데이터를 초봉 데이터로 변환 후 df 리턴 """
    # 데이터 변환 - 거래량은 매수매도 구분 후 부호 제거
    df_주식체결 = df_주식체결.set_index(pd.to_datetime(s_일자 + ' ' + df_주식체결['체결시간'], format='%Y%m%d %H%M%S'))
    df_주식체결 = df_주식체결.astype({컬럼: int for 컬럼 in ['현재가', '거래량']})
    df_주식체결[['현재가']] = df_주식체결[['현재가']].abs()

    # 종목별 초봉데이터 생성
    gr_주식체결 = df_주식체결.groupby('종목코드')
    dic_초봉 = dict()
    for s_종목코드, df_주식체결_종목 in tqdm(gr_주식체결, desc=f'캐시생성|{n_봉수}초봉-{s_일자}', file=sys.stdout):
        # 매수매도 분리
        df_전체 = df_주식체결_종목.copy()
        df_매수 = df_전체[df_전체['거래량'] > 0].copy()
        df_매도 = df_전체[df_전체['거래량'] < 0].copy()

        # 거래량 부호 제거
        df_전체['거래량'] = df_전체['거래량'].abs()
        df_매수['거래량'] = df_매수['거래량'].abs()
        df_매도['거래량'] = df_매도['거래량'].abs()

        # 리샘플 생성
        df_리샘플_전체 = df_전체.resample(f'{n_봉수}s')
        df_리샘플_매수 = df_매수.resample(f'{n_봉수}s')
        df_리샘플_매도 = df_매도.resample(f'{n_봉수}s')

        # 거래량별 리샘플 생성
        dic_리샘플_거래량 = dict()
        for n_거래량 in [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]:
            dic_리샘플_거래량[f'전체{n_거래량}'] = df_전체[df_전체['거래량'] == n_거래량].resample(f'{n_봉수}s')
            dic_리샘플_거래량[f'매수{n_거래량}'] = df_매수[df_매수['거래량'] == n_거래량].resample(f'{n_봉수}s')
            dic_리샘플_거래량[f'매도{n_거래량}'] = df_매도[df_매도['거래량'] == n_거래량].resample(f'{n_봉수}s')

        # 초봉 생성
        df_초봉 = df_리샘플_전체.first().loc[:, ['종목코드', '체결시간']].copy()
        df_초봉['시가'] = df_리샘플_전체['현재가'].first()
        df_초봉['고가'] = df_리샘플_전체['현재가'].max()
        df_초봉['저가'] = df_리샘플_전체['현재가'].min()
        df_초봉['종가'] = df_리샘플_전체['현재가'].last()
        df_초봉['거래량'] = df_리샘플_전체['거래량'].sum()
        df_초봉['매수량'] = df_리샘플_매수['거래량'].sum()
        df_초봉['매도량'] = df_리샘플_매도['거래량'].sum()
        df_초봉['체결횟수'] = df_리샘플_전체['거래량'].count()
        df_초봉['매수횟수'] = df_리샘플_매수['거래량'].count()
        df_초봉['매도횟수'] = df_리샘플_매도['거래량'].count()
        for n_거래량 in [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]:
            df_리샘플_거래량_전체 = dic_리샘플_거래량[f'전체{n_거래량}']
            df_리샘플_거래량_매수 = dic_리샘플_거래량[f'매수{n_거래량}']
            df_리샘플_거래량_매도 = dic_리샘플_거래량[f'매도{n_거래량}']
            df_초봉[f'거래량{n_거래량}'] = df_리샘플_거래량_전체['거래량'].sum()
            df_초봉[f'매수량{n_거래량}'] = df_리샘플_거래량_매수['거래량'].sum()
            df_초봉[f'매도량{n_거래량}'] = df_리샘플_거래량_매도['거래량'].sum()
            df_초봉[f'체결횟수{n_거래량}'] = df_리샘플_거래량_전체['거래량'].count()
            df_초봉[f'매수횟수{n_거래량}'] = df_리샘플_거래량_매수['거래량'].count()
            df_초봉[f'매도횟수{n_거래량}'] = df_리샘플_거래량_매도['거래량'].count()

        # nan 처리
        df_초봉['종목코드'] = s_종목코드
        df_초봉['체결시간'] = df_초봉.index.strftime('%H:%M:%S')
        df_초봉['종가'] = df_초봉['종가'].ffill().astype(int)
        for s_컬럼명 in ['시가', '고가', '저가']:
            df_초봉[s_컬럼명] = df_초봉[s_컬럼명].fillna(df_초봉['종가']).astype(int)
        for s_컬럼명 in [컬럼 for 컬럼 in df_초봉.columns if 컬럼 not in ['종목코드', '체결시간', '시가', '고가', '저가', '종가']]:
            df_초봉[s_컬럼명] = df_초봉[s_컬럼명].fillna(0).astype(int)

        # dic_초봉에 추가
        dic_초봉[s_종목코드] = df_초봉

    return dic_초봉
