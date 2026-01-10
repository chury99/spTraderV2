import os
import pandas as pd
from fontTools.varLib.models import nonNone

import ut.폴더manager


# noinspection PyPep8Naming,NonAsciiCharacters,SpellCheckingInspection
def check_조회순위(s_일자):
    """ 입력받은 일자의 조회순위 데이터 기준으로 일봉 데이터 확인하여 검증결과 리턴 """
    # 폴더 정의
    dic_폴더정보 = ut.폴더manager.define_폴더정보()
    folder_차트캐시 = dic_폴더정보['데이터|차트캐시']
    folder_조회순위 = dic_폴더정보['데이터|조회순위']
    folder_대상종목 = dic_폴더정보['데이터|대상종목']

    # 일봉 불러오기
    dic_일봉 = pd.read_pickle(os.path.join(folder_차트캐시, '일봉1', f'dic_차트캐시_1일봉_{s_일자}.pkl'))

    # 조회순위 불러오기
    df_조회순위 = pd.read_csv(os.path.join(folder_조회순위, f'df_조회순위_{s_일자}.csv'), encoding='cp949', dtype=str)
    li_대상종목 = sorted(df_조회순위.dropna(subset='종목코드')['종목코드'].unique().tolist())

    # 분석대상종목 불러오기
    path_분석대상 = os.path.join(folder_대상종목, f'df_대상종목_{s_일자}.pkl')
    df_분석대상 = pd.read_pickle(path_분석대상) if os.path.exists(path_분석대상) else pd.DataFrame()
    li_대상종목 = [종목 for 종목 in li_대상종목 if 종목 in df_분석대상['종목코드'].values] if len(df_분석대상) > 0 else li_대상종목

    # 종목별 조건 확인 - 당일 기준
    li_dic상승후보 = list()
    for s_종목코드 in li_대상종목:
        # 기준정보 정의
        df_일봉 = dic_일봉[s_종목코드]
        df_일봉['전일고가3봉'] = df_일봉['고가'].shift(1).rolling(window=3).max()
        df_일봉['추세신호'] = df_일봉['종가'] > df_일봉['전일고가3봉']
        if len(df_일봉) < 2: continue
        dt_당일 = df_일봉.index[-1]
        n_당일종가 = df_일봉.loc[dt_당일, '종가']
        n_당일60 = df_일봉.loc[dt_당일, '종가ma60']
        n_당일120 = df_일봉.loc[dt_당일, '종가ma120']
        n_당일바디 = (n_당일종가 - df_일봉.loc[dt_당일, '시가']) / df_일봉.loc[dt_당일, '전일종가'] * 100

        # 조건 확인 - 당일 기준
        li_조건확인 = list()
        li_조건확인.append(True if n_당일종가 > n_당일60 > n_당일120 else False)
        li_조건확인.append(True if sum(df_일봉['추세신호'].values[-5:]) > 0 else False)

        # 결과 생성
        dic_상승후보 = df_일봉.iloc[-1].to_dict()
        dic_상승후보.update(당일종가=n_당일종가, 당일60=n_당일60, 당일120=n_당일120, 당일바디=n_당일바디,
                        당일조건=sum(li_조건확인) == len(li_조건확인), 당일정배열=li_조건확인[0], 당일추세5일=li_조건확인[1])
        li_dic상승후보.append(dic_상승후보)

    # 데이터 정리
    df_상승후보 = pd.DataFrame(li_dic상승후보) if len(li_dic상승후보) > 0 else pd.DataFrame()

    return df_상승후보


# noinspection PyPep8Naming,NonAsciiCharacters,SpellCheckingInspection
def check_조회순위_추세돌파(s_일자):
    """ 입력받은 일자의 조회순위 데이터 기준으로 일봉 데이터 확인하여 검증결과 리턴 """
    # 폴더 정의
    dic_폴더정보 = ut.폴더manager.define_폴더정보()
    folder_차트캐시 = dic_폴더정보['데이터|차트캐시']
    folder_조회순위 = dic_폴더정보['데이터|조회순위']
    folder_대상종목 = dic_폴더정보['데이터|대상종목']

    # 일봉 불러오기
    dic_일봉 = pd.read_pickle(os.path.join(folder_차트캐시, '일봉1', f'dic_차트캐시_1일봉_{s_일자}.pkl'))

    # 조회순위 불러오기
    df_조회순위 = pd.read_csv(os.path.join(folder_조회순위, f'df_조회순위_{s_일자}.csv'), encoding='cp949', dtype=str)
    li_대상종목 = sorted(df_조회순위.dropna(subset='종목코드')['종목코드'].unique().tolist())

    # 분석대상종목 불러오기
    path_분석대상 = os.path.join(folder_대상종목, f'df_대상종목_{s_일자}.pkl')
    df_분석대상 = pd.read_pickle(path_분석대상) if os.path.exists(path_분석대상) else pd.DataFrame()
    li_대상종목 = [종목 for 종목 in li_대상종목 if 종목 in df_분석대상['종목코드'].values] if len(df_분석대상) > 0 else li_대상종목

    # 종목별 조건 확인 - 당일 기준
    li_dic상승후보 = list()
    for s_종목코드 in li_대상종목:
        # 기준정보 정의
        df_일봉 = dic_일봉[s_종목코드]
        df_일봉['전일고가3봉'] = df_일봉['고가'].shift(1).rolling(window=3).max()
        # df_일봉['고가3봉대비종가'] = (df_일봉['종가'] / df_일봉['전일고가3봉'] - 1) * 100
        df_일봉['바디'] = ((df_일봉['종가'] - df_일봉['시가']) / df_일봉['전일종가']) * 100
        df_일봉['추세신호'] = df_일봉['종가'] > df_일봉['전일고가3봉']
        if len(df_일봉) < 3: continue
        dt_당일 = df_일봉.index[-1]
        dt_전일 = df_일봉.index[-2]
        dt_전전일 = df_일봉.index[-3]
        n_당일종가 = df_일봉.loc[dt_당일, '종가']
        n_당일60 = df_일봉.loc[dt_당일, '종가ma60']
        n_당일120 = df_일봉.loc[dt_당일, '종가ma120']
        # n_당일바디 = (n_당일종가 - df_일봉.loc[dt_당일, '시가']) / df_일봉.loc[dt_당일, '전일종가'] * 100
        # n_전일바디 = (df_일봉.loc[dt_전일, '종가'] - df_일봉.loc[dt_전일, '시가']) / df_일봉.loc[dt_전일, '전일종가'] * 100
        n_당일바디 = df_일봉.loc[dt_당일, '바디']
        n_전일바디 = df_일봉.loc[dt_전일, '바디']
        n_전전일바디 = df_일봉.loc[dt_전전일, '바디']
        n_전일고가3봉 = df_일봉.loc[dt_당일, '전일고가3봉']
        n_고가3봉대비종가 = (n_당일종가 / n_전일고가3봉 - 1) * 100

        # 조건 확인 - 당일 기준
        li_조건확인 = list()
        li_조건확인.append(True if n_당일종가 > n_당일60 > n_당일120 else False)
        # li_조건확인.append(True if sum(df_일봉['추세신호'].values[-5:]) > 0 else False)
        # li_조건확인.append(True if n_당일종가 > n_전일고가3봉 else False)
        li_조건확인.append(True if 0 < n_고가3봉대비종가 < 1 else False)
        li_조건확인.append(True if n_전일바디 > 0 and n_전전일바디 > 0 else False)

        # 결과 생성
        dic_상승후보 = df_일봉.iloc[-1].to_dict()
        dic_상승후보.update(당일종가=n_당일종가, 당일60=n_당일60, 당일120=n_당일120,
                        전일고가3봉=n_전일고가3봉, 고가3봉대비종가=n_고가3봉대비종가,
                        당일바디=n_당일바디, 전일바디=n_전일바디, 전전일바디=n_전전일바디,
                        당일조건=sum(li_조건확인) == len(li_조건확인),
                        당일정배열=li_조건확인[0], 고가3봉대비=li_조건확인[1], 바디양봉=li_조건확인[2])
        li_dic상승후보.append(dic_상승후보)

    # 데이터 정리
    df_상승후보 = pd.DataFrame(li_dic상승후보) if len(li_dic상승후보) > 0 else pd.DataFrame()

    return df_상승후보