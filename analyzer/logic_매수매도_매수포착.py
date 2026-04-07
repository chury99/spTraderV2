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
    n_시가 = df_기준봉['시가'].values[0]
    n_종가 = df_기준봉['종가'].values[0]
    n_종가1 = df_기준봉['종가1'].values[0]
    n_거래량 = df_기준봉['거래량'].values[0]
    n_거래대금 = df_기준봉['거래대금'].values[0]
    n_매수횟수 = df_기준봉['매수횟수'].values[0]
    n_매수횟수ma60 = df_기준봉['매수횟수ma60'].values[0]
    n_체결횟수 = df_기준봉['체결횟수'].values[0]
    n_종가ma20 = df_기준봉['종가ma20'].values[0]
    n_종가ma60 = df_기준봉['종가ma60'].values[0]
    n_종가ma120 = df_기준봉['종가ma120'].values[0]
    n_거래량ma5 = df_기준봉['거래량ma5'].values[0]
    n_거래량ma20 = df_기준봉['거래량ma20'].values[0]
    n_고가5 = df_기준봉['고가5'].values[0]
    n_고가14 = df_기준봉['고가14'].values[0]
    n_고가20 = df_기준봉['고가20'].values[0]
    n_고가40 = df_기준봉['고가40'].values[0]
    n_고가60 = df_기준봉['고가60'].values[0]
    n_저가5 = df_기준봉['저가5'].values[0]
    n_ATR = df_기준봉['ATR'].values[0]
    n_ATR14 = df_기준봉['ATR14'].values[0]
    n_ATR비율 = df_기준봉['ATR비율'].values[0]
    n_ATR비율차이 = df_기준봉['ATR비율차이'].values[0]
    n_이격도20 = df_기준봉['이격도20'].values[0]
    n_일봉시가 = df_기준봉['일봉시가'].values[0]
    n_당일봉수 = df_기준봉['당일봉수'].values[0]
    dic_매수신호.update(매수봇_n_ATR14=n_ATR14,매수봇_n_일봉시가=n_일봉시가)

    # 매수검증
    b_매수급증 = n_매수횟수 > 5 * n_매수횟수ma60
    n_매수비중 = n_매수횟수 / n_체결횟수 if n_매수횟수 != 0 else 0
    # b_매수검증 = b_매수급증 and (0.5 < n_매수비중 < 0.7) and (n_매수횟수 > 20) # 12%
    b_매수검증 = b_매수급증 and (0.5 < n_매수비중 < 0.7) and (n_매수횟수 > 10) # 26%
    # b_매수검증 = b_매수급증 and (0.5 < n_매수비중 < 0.7) # 24%
    li_매수신호.append(b_매수검증)
    dic_매수신호.update(매수봇_n_매수횟수=n_매수횟수, 매수봇_n_매수횟수ma60=n_매수횟수ma60, 매수봇_b_매수급증=b_매수급증,
                    매수봇_n_매수비중=n_매수비중)

    # 전봉검증
    n_전봉상승율 = (n_종가 / n_종가1 - 1) * 100 if n_종가1 != 0 else 0
    b_전봉검증 = n_전봉상승율 > 0.3
    # b_전봉검증 = n_전봉상승율 > 0
    li_매수신호.append(b_전봉검증)
    dic_매수신호.update(매수봇_n_전봉상승율=n_전봉상승율)

    # 매수신호 업데이트
    b_매수신호 = sum(li_매수신호) == len(li_매수신호)
    dic_매수신호.update(매수봇_li_매수신호=li_매수신호, 매수봇_b_매수신호=b_매수신호)

    return dic_매수신호


# noinspection PyPep8Naming,NonAsciiCharacters,SpellCheckingInspection,PyTypeChecker
def judge_매도신호(df_기준봉, dic_args):
    """ 입력받은 초봉 데이터 기준으로 매수신호 생성 후 리턴 - 입력 데이터는 현시점 대비 1봉 전 데이터 """
    # 매도신호 정의
    li_신호종류 = ['초봉이탈', '고가이탈', '손실한계', '타임아웃']
    li_매도신호 = list()
    dic_매도신호 = dict(매도봇_li_신호종류=li_신호종류, 매도봇_li_매도신호=li_매도신호, 매도봇_b_매도신호=False, 매도봇_df_기준봉=df_기준봉)

    # 데이터 존재 확인 - 미 존재 시 False 출력
    if df_기준봉.empty:
        dic_매도신호.update(매도봇_li_매도신호=[False] * len(li_신호종류))
        return dic_매도신호

    # 기준정보 정의
    n_시가 = df_기준봉['시가'].values[0]
    n_종가 = df_기준봉['종가'].values[0]
    n_거래량 = df_기준봉['거래량'].values[0]
    n_저가3 = df_기준봉['저가3'].values[0]
    n_ATR14 = df_기준봉['ATR14'].values[0]
    n_ATR비율 = df_기준봉['ATR비율'].values[0]
    n_ATR비율차이 = df_기준봉['ATR비율차이'].values[0]
    n_매수횟수 = df_기준봉['매수횟수'].values[0]
    n_매수횟수ma60 = df_기준봉['매수횟수ma60'].values[0]
    s_봉수 = dic_args['s_봉수']
    n_매수가 = dic_args['매도봇_n_매수단가']
    n_ATR14_매수 = dic_args['매수봇_n_ATR14']
    # n_ATR비율_매수 = dic_args['매수봇_n_ATR비율']
    s_매수시간 = dic_args['매도봇_s_매수시간']
    s_탐색시간 = dic_args['매도봇_s_탐색시간']
    n_현재가 = dic_args['매도봇_n_현재가']
    df_기준봉전체 = dic_args['매도봇_df_기준봉전체']
    df_1초봉 = dic_args['매도봇_df_1초봉시점']
    n_초봉120 = df_1초봉['종가ma120'].values[-1]
    n_이격도초봉120 = n_현재가 / n_초봉120 * 100
    n_초봉고가보유 = df_1초봉.loc[df_1초봉['체결시간'] >= s_매수시간, '고가'].max()
    n_분봉고가보유 = df_1초봉.loc[(df_1초봉['체결시간'] >= s_매수시간) & (df_1초봉['체결시간'] <= df_기준봉['시간'].values[0]), '고가'].max()
    n_수익률 = (n_현재가 / n_매수가 - 1) * 100 - 0.2 if n_매수가 is not None else None
    n_초봉최고수익률 = (n_초봉고가보유 / n_매수가 - 1) * 100 - 0.2 if n_매수가 is not None else None
    n_분봉최고수익률 = (n_분봉고가보유 / n_매수가 - 1) * 100 - 0.2 if n_매수가 is not None else None
    n_리스크 = 2 * n_ATR14_매수
    n_리스크 = max(2.5 * (n_ATR14_매수 * 3.5), n_매수가 * 0.003)
    # n_리스크 = 1.5 * n_ATR14_매수 if n_매수가 is None or 1.5 * n_ATR14_매수 <= n_매수가 * 0.01 else n_매수가 * 0.008
    # n_리스크 = 2.5 * n_ATR14_매수
    # n_리스크 = 3 * n_ATR14_매수
    # n_리스크 = 4 * n_ATR14_매수
    dic_매도신호.update(매도봇_n_수익률=n_수익률, 매도봇_n_초봉최고수익률=n_초봉최고수익률, 매도봇_n_분봉최고수익률=n_분봉최고수익률,
                    매도봇_n_리스크=n_리스크)

    # # 초봉이탈 - 현재가가 1초봉 기준 ma120 밑으로 내려갈 때
    # b_초봉이탈 = (n_현재가 < n_초봉120) and (n_수익률 > 0.5)
    # # b_초봉이탈 = (n_이격도초봉120 < (100 - 0.5)) and (n_수익률 > 0)
    # # b_초봉이탈 = n_종가 < n_저가3
    # # b_초봉이탈 = False
    # li_매도신호.append(b_초봉이탈)
    # dic_매도신호.update(매도봇_n_초봉120=n_초봉120, 매도봇_n_이격도초봉120=n_이격도초봉120)

    # 매수소멸 - 매수횟수가 ma60보다 작아질 때
    b_매수소멸 = n_매수횟수 < n_매수횟수ma60
    li_매도신호.append(b_매수소멸)
    dic_매도신호.update(매도봇_n_매수횟수=n_매수횟수, 매도봇_n_매수횟수ma60=n_매수횟수ma60)

    # 고가이탈 - 매수이후 고가 - 변동폭
    n_고가변동 = 2 * n_ATR14
    n_고가한계 = n_초봉고가보유 - n_고가변동
    b_고가이탈 = (n_종가 < n_고가한계) and (n_초봉최고수익률 > 1.5)
    li_매도신호.append(b_고가이탈)
    dic_매도신호.update(매도봇_n_고가보유=n_초봉고가보유, 매도봇_n_고가변동=n_고가변동, 매도봇_n_고가한계=n_고가한계)

    # 손실한계 - 매수가 - 변동폭
    n_본전 = n_매수가 * 1.002
    n_손절 = n_매수가 - n_리스크
    n_손실기준 = max(n_손절, n_본전) if n_분봉최고수익률 > 1.5 else n_손절
    b_손실한계 = n_종가 < n_손실기준
    li_매도신호.append(b_손실한계)
    dic_매도신호.update(매도봇_n_손실기준=n_손실기준)

    # 타임아웃 - 180초 경과 or 장마감 시간 도래
    n_경과시간 = (pd.to_timedelta(s_탐색시간) - pd.to_timedelta(s_매수시간)).seconds
    n_타임아웃 = 60
    # n_타임아웃 = 30
    b_타임아웃 = (n_경과시간 > n_타임아웃 and n_거래량 > 0) or s_탐색시간 > '15:15:00'
    li_매도신호.append(b_타임아웃)
    dic_매도신호.update(매도봇_n_경과시간=n_경과시간, 매도봇_n_타임아웃=n_타임아웃)

    # 매도신호 업데이트
    b_매도신호 = sum(li_매도신호) > 0
    dic_매도신호.update(매도봇_li_매도신호=li_매도신호, 매도봇_b_매도신호=b_매도신호)
    dic_매도신호.update(매도봇_수익률=n_수익률)

    return dic_매도신호
