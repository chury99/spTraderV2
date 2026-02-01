import os
import pandas as pd
from fontTools.varLib.models import nonNone


# noinspection PyPep8Naming,NonAsciiCharacters,SpellCheckingInspection
def judge_매수신호(df_기준봉):
    """ 입력받은 초봉 데이터 기준으로 매수신호 생성 후 리턴 - 입력 데이터는 현시점 대비 1봉 전 데이터 """# 매수신호 정의
    li_신호종류 = ['배열검증', '돌파검증', '물량검증']
    li_매수신호 = list()
    dic_매수신호 = dict(매수봇_li_신호종류=li_신호종류, 매수봇_li_매수신호=li_매수신호, 매수봇_b_매수신호=False,
                    매수봇_df_기준봉=df_기준봉,
                    매수봇_n_매수금액=None, 매수봇_n_매수횟수=None, 매수봇_n_매도비율=None, 매수봇_n_매수량=None, 매수봇_n_매도량=None)

    # 데이터 존재 확인 - 미 존재 시 False 출력
    if df_기준봉.empty:
        dic_매수신호.update(매수봇_li_매수신호=[False] * len(li_신호종류))
        return dic_매수신호

    # 기준정보 정의
    n_시가 = df_기준봉['시가'].values[0]
    n_종가 = df_기준봉['종가'].values[0]
    n_거래량 = df_기준봉['거래량'].values[0]
    n_종가ma20 = df_기준봉['종가ma20'].values[0]
    n_종가ma60 = df_기준봉['종가ma60'].values[0]
    n_거래량ma5 = df_기준봉['거래량ma5'].values[0]
    n_고가20 = df_기준봉['고가20'].values[0]
    n_ATR14 = df_기준봉['ATR14'].values[0]
    n_ATR비율 = df_기준봉['ATR비율'].values[0]
    dic_매수신호.update(매수봇_n_ATR14=n_ATR14)

    # 배열검증 - 20정배열 + 60정배열
    b_20정배열 = n_종가 > n_종가ma20
    b_60정배열 = n_종가ma20 > n_종가ma60
    b_배열검증 = b_20정배열 and b_60정배열
    li_매수신호.append(b_배열검증)
    dic_매수신호.update(매수봇_b_정배열20=b_20정배열, 매수봇_b_정배열60=b_60정배열)

    # 돌파검증 - 고가20돌파 + 양봉
    b_고가20돌파 = n_종가 > n_고가20
    b_양봉 = n_종가 > n_시가
    b_돌파검증 = b_고가20돌파 and b_양봉
    li_매수신호.append(b_돌파검증)
    dic_매수신호.update(매수봇_b_고가20돌파=b_고가20돌파, 매수봇_b_양봉=b_양봉)

    # 물량검증 - ma5의 2배 초과 + ATR 0.5% 이상
    n_기준물량 = n_거래량ma5 * 2
    # b_물량검증 = n_거래량 > n_기준물량 or n_ATR비율 > 0.5
    b_물량검증 = n_ATR비율 > 0.5
    li_매수신호.append(b_물량검증)
    dic_매수신호.update(매수봇_n_기준물량=n_기준물량, 매수봇_n_ATR비율=n_ATR비율)

    # 매수신호 업데이트
    b_매수신호 = sum(li_매수신호) == len(li_매수신호)
    dic_매수신호.update(매수봇_li_매수신호=li_매수신호, 매수봇_b_매수신호=b_매수신호)

    return dic_매수신호


# noinspection PyPep8Naming,NonAsciiCharacters,SpellCheckingInspection,PyTypeChecker
def judge_매도신호(df_기준봉, dic_args):
    """ 입력받은 초봉 데이터 기준으로 매수신호 생성 후 리턴 - 입력 데이터는 현시점 대비 1봉 전 데이터 """# 매도신호 정의
    li_신호종류 = ['수익달성', '추세이탈', '손실한계', '타임아웃']
    li_매도신호 = list()
    dic_매도신호 = dict(매도봇_li_신호종류=li_신호종류, 매도봇_li_매도신호=li_매도신호, 매도봇_b_매도신호=False,
                    매도봇_df_기준봉=df_기준봉)

    # 데이터 존재 확인 - 미 존재 시 False 출력
    if df_기준봉.empty:
        dic_매도신호.update(매도봇_li_매도신호=[False] * len(li_신호종류))
        return dic_매도신호

    # 기준정보 정의
    n_종가 = df_기준봉['종가'].values[0]
    n_거래량 = df_기준봉['거래량'].values[0]
    n_ATR14 = df_기준봉['ATR14'].values[0]
    n_ATR비율 = df_기준봉['ATR비율'].values[0]
    n_ATR비율차이 = df_기준봉['ATR비율차이'].values[0]
    n_봉수 = dic_args['n_봉수']
    n_매수가 = dic_args['매도봇_n_매수단가']
    n_ATR14_매수 = dic_args['매수봇_n_ATR14']
    n_ATR비율_매수 = dic_args['매수봇_n_ATR비율']
    s_매수시간 = dic_args['매도봇_s_매수시간']
    s_탐색시간 = dic_args['매도봇_s_탐색시간']
    n_현재가 = dic_args['매도봇_n_현재가']
    df_기준봉전체 = dic_args['매도봇_df_기준봉전체']
    n_수익률 = (n_현재가 / n_매수가 - 1) * 100 - 0.2 if n_매수가 is not None else None
    dic_매도신호.update(매도봇_n_수익률=n_수익률)

    n_매수량 = df_기준봉['매수량'].values[0]
    n_매도량 = df_기준봉['매도량'].values[0]
    n_매수횟수 = df_기준봉['매수횟수'].values[0]
    n_매도횟수 = df_기준봉['매도횟수'].values[0]

    # 수익달성 - 매수가 + 변동폭 (하락변동의 3배)
    n_수익기준 = n_매수가 + 12 * n_ATR14_매수
    b_수익달성 = n_종가 > n_수익기준
    li_매도신호.append(b_수익달성)
    dic_매도신호.update(매도봇_n_수익기준=n_수익기준)

    # 추세이탈 - 매수이후 고가 - 변동폭
    n_고가보유 = df_기준봉전체.loc[df_기준봉전체['체결시간'] >= s_매수시간]['고가'].max()
    n_추세변동 = 2 * n_ATR14
    n_추세한계 = n_고가보유 - n_추세변동
    # b_추세이탈 = (n_종가 < n_추세한계 and n_수익률 > 0) or (n_ATR비율 < n_ATR비율_매수 and n_수익률 > 0)
    # b_추세이탈 = (n_종가 < n_추세한계 and n_수익률 > 0) or (n_ATR비율차이 < -0.1 and n_수익률 > 0)
    b_추세이탈 = (n_종가 < n_추세한계 and n_수익률 > 0) or (n_ATR비율차이 < -0.05 and n_수익률 > 0)
    li_매도신호.append(b_추세이탈)
    dic_매도신호.update(매도봇_n_고가보유=n_고가보유, 매도봇_n_추세변동=n_추세변동, 매도봇_n_추세한계=n_추세한계)

    # 손실한계 - 매수가 - 변동폭 (2% 기준)
    n_손실기준 = n_매수가 - 4 * n_ATR14_매수
    b_손실한계 = n_종가 < n_손실기준
    li_매도신호.append(b_손실한계)
    dic_매도신호.update(매도봇_n_손실기준=n_손실기준)

    # 타임아웃 - 180초 경과 or 장마감 시간 도래
    n_경과시간 = (pd.to_timedelta(s_탐색시간) - pd.to_timedelta(s_매수시간)).seconds
    b_타임아웃 = (n_경과시간 > 180 and n_거래량 > 0) or s_탐색시간 > '15:15:00'
    li_매도신호.append(b_타임아웃)
    dic_매도신호.update(매도봇_n_경과시간=n_경과시간)

    # 매도신호 업데이트
    b_매도신호 = sum(li_매도신호) > 0
    dic_매도신호.update(매도봇_li_매도신호=li_매도신호, 매도봇_b_매도신호=b_매도신호)
    dic_매도신호.update(매도봇_수익률=n_수익률)

    return dic_매도신호
