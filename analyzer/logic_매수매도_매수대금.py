import os
import pandas as pd
from fontTools.ttLib.tables.otTables import DeltaSetIndexMap
from fontTools.varLib.models import nonNone


# noinspection PyPep8Naming,NonAsciiCharacters,SpellCheckingInspection
def judge_매수신호(df_기준봉):
    """ 입력받은 초봉 데이터 기준으로 매수신호 생성 후 리턴 - 입력 데이터는 현시점 대비 1봉 전 데이터 """# 매수신호 정의
    li_신호종류 = ['매수검증', '전봉검증']
    li_매수신호 = list()
    dic_매수신호 = dict(매수봇_li_신호종류=li_신호종류, 매수봇_li_매수신호=li_매수신호, 매수봇_b_매수신호=False,
                    매수봇_df_기준봉=df_기준봉,
                    매수봇_n_매수금액=None, 매수봇_n_매수횟수=None, 매수봇_n_매도비율=None, 매수봇_n_매수량=None, 매수봇_n_매도량=None)

    # 데이터 존재 확인 - 미 존재 시 False 출력
    if df_기준봉.empty:
        dic_매수신호.update(매수봇_li_매수신호=[False] * len(li_신호종류))
        return dic_매수신호

    # 기준정보 정의
    s_봉수 = df_기준봉['봉수'].values[0]
    n_시가1 = df_기준봉['시가'].values[0]
    n_종가1 = df_기준봉['종가'].values[0]
    n_종가2 = df_기준봉['종가1'].values[0]
    n_거래량1 = df_기준봉['거래량'].values[0]
    n_거래대금1 = df_기준봉['거래대금'].values[0]
    n_매수횟수1 = df_기준봉['매수횟수'].values[0]
    n_매수횟수1ma60 = df_기준봉['매수횟수ma60'].values[0]
    n_체결횟수1 = df_기준봉['체결횟수'].values[0]
    n_매수대금1 = df_기준봉['매수대금'].values[0]
    n_매수대금1ma60 = df_기준봉['매수대금ma60'].values[0]
    n_종가1ma20 = df_기준봉['종가ma20'].values[0]
    n_종가1ma60 = df_기준봉['종가ma60'].values[0]
    n_종가1ma120 = df_기준봉['종가ma120'].values[0]
    n_거래량1ma5 = df_기준봉['거래량ma5'].values[0]
    n_거래량1ma20 = df_기준봉['거래량ma20'].values[0]
    n_고가1r5 = df_기준봉['고가5'].values[0]
    n_고가1r14 = df_기준봉['고가14'].values[0]
    n_고가1r20 = df_기준봉['고가20'].values[0]
    n_고가1r40 = df_기준봉['고가40'].values[0]
    n_고가1r60 = df_기준봉['고가60'].values[0]
    n_저가1r5 = df_기준봉['저가5'].values[0]
    n_ATR1 = df_기준봉['ATR'].values[0]
    n_ATR1r14 = df_기준봉['ATR14'].values[0]
    n_ATR비율1 = df_기준봉['ATR비율'].values[0]
    n_ATR비율차이1 = df_기준봉['ATR비율차이'].values[0]
    n_이격도1r20 = df_기준봉['이격도20'].values[0]
    n_일봉시가 = df_기준봉['일봉시가'].values[0]
    n_당일봉수 = df_기준봉['당일봉수'].values[0]
    dic_매수신호.update(매수봇_n_ATR14=n_ATR1r14,매수봇_n_일봉시가=n_일봉시가)

    # 매수검증
    b_매수급증 = n_매수대금1 > 5 * n_매수대금1ma60
    b_매수대금 = n_매수대금1 > 5 * 1000 * 1000
    b_매수검증 = b_매수급증 and b_매수대금
    li_매수신호.append(b_매수검증)
    dic_매수신호.update(매수봇_b_매수급증=b_매수급증, 매수봇_b_매수대금=b_매수대금,
                    매수봇_n_매수대금=n_매수대금1, 매수봇_n_매수대금ma60=n_매수대금1ma60)

    # 전봉검증
    n_전봉바디 = n_종가1 - n_시가1
    n_전봉상승율 = (n_종가1 / n_종가2 - 1) * 100 if n_종가2 != 0 else 0
    b_전봉양봉 = n_전봉바디 > 0
    b_전봉상승 = n_전봉상승율 > 0
    b_전봉검증 = b_전봉양봉
    li_매수신호.append(b_전봉검증)
    dic_매수신호.update(매수봇_b_전봉양봉=b_전봉양봉, 매수봇_b_전봉상승=b_전봉상승,
                    매수봇_n_전봉바디=n_전봉바디, 매수봇_n_전봉상승율=n_전봉상승율)

    # 매수신호 업데이트
    b_매수신호 = sum(li_매수신호) == len(li_매수신호)
    dic_매수신호.update(매수봇_li_매수신호=li_매수신호, 매수봇_b_매수신호=b_매수신호)

    return dic_매수신호


# noinspection PyPep8Naming,NonAsciiCharacters,SpellCheckingInspection,PyTypeChecker
def judge_매도신호(df_기준봉, dic_args):
    """ 입력받은 초봉 데이터 기준으로 매수신호 생성 후 리턴 - 입력 데이터는 현시점 대비 1봉 전 데이터 """
    # 매도신호 정의
    li_신호종류 = ['음봉등장', '수익달성', '손실한계', '타임아웃']
    li_매도신호 = list()
    dic_매도신호 = dict(매도봇_li_신호종류=li_신호종류, 매도봇_li_매도신호=li_매도신호, 매도봇_b_매도신호=False, 매도봇_df_기준봉=df_기준봉)

    # 데이터 존재 확인 - 미 존재 시 False 출력
    if df_기준봉.empty:
        dic_매도신호.update(매도봇_li_매도신호=[False] * len(li_신호종류))
        return dic_매도신호

    # 기준정보 정의
    n_시가1 = df_기준봉['시가'].values[0]
    n_종가1 = df_기준봉['종가'].values[0]
    n_거래량1 = df_기준봉['거래량'].values[0]
    n_저가1r3 = df_기준봉['저가3'].values[0]
    n_ATR1r14 = df_기준봉['ATR14'].values[0]
    n_ATR비율1 = df_기준봉['ATR비율'].values[0]
    n_ATR비율차이1 = df_기준봉['ATR비율차이'].values[0]
    n_매수횟수1 = df_기준봉['매수횟수'].values[0]
    n_매수횟수1ma60 = df_기준봉['매수횟수ma60'].values[0]
    s_봉수 = dic_args['s_봉수']
    n_매수가 = dic_args['매도봇_n_매수단가']
    n_ATR14_매수 = dic_args['매수봇_n_ATR14']
    s_매수시간 = dic_args['매도봇_s_매수시간']
    s_탐색시간 = dic_args['매도봇_s_탐색시간']
    n_현재가 = dic_args['매도봇_n_현재가']
    df_기준봉전체 = dic_args['매도봇_df_기준봉전체']
    df_1초봉 = dic_args['매도봇_df_1초봉시점']
    n_초봉1ma120 = df_1초봉['종가ma120'].values[-1]
    n_이격도초봉1ma120 = n_현재가 / n_초봉1ma120 * 100
    n_초봉고가보유 = df_1초봉.loc[df_1초봉['체결시간'] >= s_매수시간, '고가'].max()
    n_분봉고가보유 = df_1초봉.loc[(df_1초봉['체결시간'] >= s_매수시간) & (df_1초봉['체결시간'] <= df_기준봉['시간'].values[0]), '고가'].max()
    n_현재수익률 = (n_현재가 / n_매수가 - 1) * 100 - 0.2 if n_매수가 is not None else None
    n_초봉최고수익률 = (n_초봉고가보유 / n_매수가 - 1) * 100 - 0.2 if n_매수가 is not None else None
    n_분봉최고수익률 = (n_분봉고가보유 / n_매수가 - 1) * 100 - 0.2 if n_매수가 is not None else None
    n_리스크 = 2 * n_ATR14_매수
    n_리스크 = max(2.5 * (n_ATR14_매수 * 3.5), n_매수가 * 0.003)
    # n_리스크 = 1.5 * n_ATR14_매수 if n_매수가 is None or 1.5 * n_ATR14_매수 <= n_매수가 * 0.01 else n_매수가 * 0.008
    # n_리스크 = 2.5 * n_ATR14_매수
    # n_리스크 = 3 * n_ATR14_매수
    # n_리스크 = 4 * n_ATR14_매수
    dic_매도신호.update(매도봇_n_수익률=n_현재수익률, 매도봇_n_초봉최고수익률=n_초봉최고수익률, 매도봇_n_분봉최고수익률=n_분봉최고수익률,
                    매도봇_n_리스크=n_리스크)

    # 음봉등장 - 이전봉이 음봉이면 매도
    n_바디1 = n_종가1 - n_시가1
    # b_음봉등장 = (n_바디1 < 0) and (n_현재수익률 > 0)
    b_음봉등장 = n_바디1 < 0
    li_매도신호.append(b_음봉등장)
    dic_매도신호.update(매도봇_b_음봉등장=b_음봉등장, 매도봇_n_바디=n_바디1)

    # 수익달성 - 수익률 10% 이상이면 매도
    b_수익달성 = n_현재수익률 > 10
    li_매도신호.append(b_수익달성)
    dic_매도신호.update(매도봇_b_수익달성=b_수익달성)

    # 손실한계 - 0.5% 손실 시 매도
    n_손실기준 = n_매수가 * (100 - 0.5) / 100
    b_손실한계 = n_현재가 < n_손실기준
    li_매도신호.append(b_손실한계)
    dic_매도신호.update(매도봇_b_손실한계=b_손실한계, 매도봇_n_손실기준=n_손실기준)

    # 타임아웃 - 180초 경과 or 장마감 시간 도래
    n_경과시간 = (pd.to_timedelta(s_탐색시간) - pd.to_timedelta(s_매수시간)).seconds
    n_타임아웃 = 60
    b_타임아웃 = (n_경과시간 > n_타임아웃 and n_거래량1 > 0) or s_탐색시간 > '15:15:00'
    li_매도신호.append(b_타임아웃)
    dic_매도신호.update(매도봇_n_경과시간=n_경과시간, 매도봇_n_타임아웃=n_타임아웃)

    # 매도신호 업데이트
    b_매도신호 = sum(li_매도신호) > 0
    dic_매도신호.update(매도봇_li_매도신호=li_매도신호, 매도봇_b_매도신호=b_매도신호)
    dic_매도신호.update(매도봇_수익률=n_현재수익률)

    return dic_매도신호
