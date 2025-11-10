import os
import pandas as pd
from fontTools.varLib.models import nonNone


# noinspection PyPep8Naming,NonAsciiCharacters,SpellCheckingInspection
def judge_매수신호(df_기준봉):
    """ 입력받은 초봉 데이터 기준으로 매수신호 생성 후 리턴 - 입력 데이터는 현시점 대비 1봉 전 데이터 """
    # 매수신호 정의
    li_신호종류 = ['매수금액', '매수횟수', '매도비율']
    li_매수신호 = list()
    dic_매수신호 = dict(매수봇_li_신호종류=li_신호종류, 매수봇_li_매수신호=li_매수신호, 매수봇_b_매수신호=False,
                    매수봇_df_기준봉=df_기준봉,
                    매수봇_n_매수금액=None, 매수봇_n_매수횟수=None, 매수봇_n_매도비율=None, 매수봇_n_매수량=None, 매수봇_n_매도량=None)

    # 데이터 존재 확인 - 미 존재 시 False 출력
    if df_기준봉.empty:
        dic_매수신호.update(매수봇_li_매수신호=[False] * len(li_신호종류))
        return dic_매수신호

    # 기준정보 정의
    n_종가 = df_기준봉['종가'].values[0]
    n_매수량 = df_기준봉['매수량'].values[0]
    n_매도량 = df_기준봉['매도량'].values[0]
    n_매수횟수 = df_기준봉['매수횟수'].values[0]
    n_매도횟수 = df_기준봉['매도횟수'].values[0]

    # 매수금액 검증 - 1억 초과
    n_매수금액 = n_종가 * n_매수량 / (10 ** 6)
    b_매수금액 = n_매수금액 > 100
    li_매수신호.append(b_매수금액)
    dic_매수신호.update(매수봇_n_매수금액=n_매수금액)

    # 매수횟수 검증 - 10회 초과
    n_매수횟수 = n_매수횟수
    b_매수횟수 = n_매수횟수 > 10
    li_매수신호.append(b_매수횟수)
    dic_매수신호.update(매수봇_n_매수횟수=n_매수횟수)

    # 매도비율 검증 - 전체량의 20% 미만
    n_매도비율 = n_매도량 / (n_매수량 + n_매도량) * 100 if (n_매수량 + n_매도량) > 0 else 0
    b_매도비율 = n_매도비율 < 20
    li_매수신호.append(b_매도비율)
    dic_매수신호.update(매수봇_n_매도비율=n_매도비율, 매수봇_n_매수량=n_매수량, 매수봇_n_매도량=n_매도량)

    # 매수신호 업데이트
    b_매수신호 = sum(li_매수신호) == len(li_매수신호)
    dic_매수신호.update(매수봇_li_매수신호=li_매수신호, 매수봇_b_매수신호=b_매수신호)

    return dic_매수신호


# noinspection PyPep8Naming,NonAsciiCharacters,SpellCheckingInspection,PyTypeChecker
def judge_매도신호(df_기준봉, dic_args):
    """ 입력받은 초봉 데이터 기준으로 매수신호 생성 후 리턴 - 입력 데이터는 현시점 대비 1봉 전 데이터 """
    # 매도신호 정의
    li_신호종류 = ['매수금액', '매수횟수', '하락한계', '타임아웃']
    li_매도신호 = list()
    dic_매도신호 = dict(매도봇_li_신호종류=li_신호종류, 매도봇_li_매도신호=li_매도신호, 매도봇_b_매도신호=False,
                    매도봇_df_기준봉=df_기준봉)

    # 데이터 존재 확인 - 미 존재 시 False 출력
    if df_기준봉.empty and dic_args['매수봇_s_주문시간'] == dic_args['매도봇_s_탐색시간']:
        dic_매도신호.update(매도봇_li_매도신호=[False] * len(li_신호종류))
        return dic_매도신호

    # 기준정보 정의
    n_수익률기준 = 0.1
    n_종가 = df_기준봉['종가'].values[0]
    n_매수량 = df_기준봉['매수량'].values[0]
    n_매도량 = df_기준봉['매도량'].values[0]
    n_매수횟수 = df_기준봉['매수횟수'].values[0]
    n_매도횟수 = df_기준봉['매도횟수'].values[0]
    n_봉수 = dic_args['n_봉수']
    s_매수시간 = dic_args['매도봇_s_매수시간']
    s_탐색시간 = dic_args['매도봇_s_탐색시간']
    n_매수가 = dic_args['매도봇_n_매수단가']
    n_현재가 = dic_args['매도봇_n_현재가']
    n_수익률 = (n_현재가 / n_매수가 - 1) * 100 - 0.2 if n_매수가 is not None else None
    dic_매도신호.update(매도봇_n_수익률=n_수익률)

    # 매수금액 검증 - 1억 미만 + 수익구간
    n_매수금액 = n_종가 * n_매수량 / (10 ** 6)
    b_매수금액 = n_매수금액 < 100 * n_봉수 and n_수익률 > n_수익률기준
    li_매도신호.append(b_매수금액)
    dic_매도신호.update(매도봇_n_매수금액=n_매수금액)

    # 매수횟수 검증 - 10회 미만 + 수익구간
    n_매수횟수 = n_매수횟수
    b_매수횟수 = n_매수횟수 < 10 and n_수익률 > n_수익률기준
    li_매도신호.append(b_매수횟수)
    dic_매도신호.update(매도봇_n_매수횟수=n_매수횟수)

    # 타임아웃 검증 - 20초 경과 + 수익구간 or 장마감 시간 도래
    # n_매도비율 = n_매도량 / (n_매수량 + n_매도량) * 100 if (n_매수량 + n_매도량) > 0 else 0
    # b_매도비율 = n_매도비율 > 90 and n_수익률 > 0.1
    n_경과시간 = (pd.to_timedelta(s_탐색시간) - pd.to_timedelta(s_매수시간)).seconds
    b_타임아웃 = (n_경과시간 > 20 and n_수익률 > n_수익률기준) or (s_탐색시간 > '15:15:00')
    li_매도신호.append(b_타임아웃)
    dic_매도신호.update(매도봇_n_경과시간=n_경과시간)

    # 하락한계 검증 - 수익률 -0.5% 기준
    b_하락한계 = n_수익률 < -0.5
    li_매도신호.append(b_하락한계)

    # 매도신호 업데이트
    b_매도신호 = sum(li_매도신호) > 0
    dic_매도신호.update(매도봇_li_매도신호=li_매도신호, 매도봇_b_매도신호=b_매도신호)

    return dic_매도신호
