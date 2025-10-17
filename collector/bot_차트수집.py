import os
import sys
import json
import time

import pandas as pd
import re
import sqlite3

import ut.로그maker, ut.폴더manager, ut.도구manager as Tool
import xapi.RestAPI_kiwoom


# noinspection NonAsciiCharacters,SpellCheckingInspection,PyPep8Naming,PyAttributeOutsideInit
class CollectorBot:
    def __init__(self):
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
        self.folder_전체종목 = dic_폴더정보['데이터|전체종목']
        self.folder_전체일자 = dic_폴더정보['데이터|전체일자']
        os.makedirs(self.folder_차트수집, exist_ok=True)

        # 추가 폴더 정의
        self.folder_임시 = os.path.join(self.folder_차트수집, '임시저장')
        self.folder_일봉 = os.path.join(self.folder_차트수집, '일봉')
        self.folder_분봉 = os.path.join(self.folder_차트수집, '분봉')
        os.makedirs(self.folder_임시, exist_ok=True)
        os.makedirs(self.folder_일봉, exist_ok=True)
        os.makedirs(self.folder_분봉, exist_ok=True)

        # 기준정보 정의
        self.s_오늘 = pd.Timestamp.now().strftime('%Y%m%d')

        # api 정의
        self.api = xapi.RestAPI_kiwoom.RestAPIkiwoom()
        self.n_tr딜레이 = self.api.n_tr딜레이

        # 로그 기록
        self.make_로그(f'구동 시작')

    def find_대상일자(self):
        """ db 파일에 저장된 데이터의 마지막 일자 확인 """
        # 최종일자 확인
        dic_최종일자 = dict(일봉=None, 분봉=None)
        for s_봉구분 in dic_최종일자.keys():
            # 최종파일 확인
            folder = self.folder_일봉 if s_봉구분 == '일봉' else self.folder_분봉 if s_봉구분 == '분봉' else None
            s_최종파일명 = max(파일 for 파일 in os.listdir(folder) if '.db' in 파일)
            path_최종파일 = os.path.join(folder, s_최종파일명)

            # 테이블명 확인
            li_테이블명 = Tool.sql불러오기(path=path_최종파일)
            s_테이블명_최종 = max(li_테이블명)

            # 최종일자 확인 - 일봉
            if s_봉구분 == '일봉':
                df_최종월 = Tool.sql불러오기(path=path_최종파일, s_테이블명=s_테이블명_최종)
                dic_최종일자['일봉'] = df_최종월['일자'].max()

            # 최종일자 확인 - 분봉
            elif s_봉구분 == '분봉':
                dic_최종일자['분봉'] = re.findall(r'\d{8}', s_테이블명_최종)[0]

        # 전체일자 확인
        s_파일명 = max(파일 for 파일 in os.listdir(self.folder_전체일자) if '.pkl' in 파일)
        li_전체일자 = pd.read_pickle(os.path.join(self.folder_전체일자, s_파일명))

        # 대상일자 확인
        dic_대상일자 = dict()
        for s_봉구분 in dic_최종일자.keys():
            dic_대상일자[s_봉구분] = [일자 for 일자 in li_전체일자 if 일자 > dic_최종일자[s_봉구분]]

        # 대상일자 등록
        self.dic_li대상일자 = dic_대상일자

        # 로그 기록
        self.make_로그(f'일봉-{dic_대상일자["일봉"]}, 분봉-{dic_대상일자["분봉"]}')

    def get_차트데이터(self):
        """ 전체 종목 대상으로 일봉, 분봉 데이터 조회하여 pkl 파일로 저장 """
        # 차트 데이터 수집
        for s_봉구분 in self.dic_li대상일자.keys():
            # 일자별 데이터 수집
            for s_대상일자 in self.dic_li대상일자[s_봉구분]:
                # 기준정보 정의
                path_차트정보 = os.path.join(self.folder_임시, f'dic_차트정보_{s_봉구분}_{s_대상일자}.pkl')

                # 차트정보 가져오기
                dic_차트정보 = pd.read_pickle(path_차트정보) if os.path.exists(path_차트정보)\
                                else dict(li_전체종목=list(), li_제외종목=list(), df_차트=pd.DataFrame())

                # 전체종목 가져오기
                df_전체종목 = pd.read_pickle(os.path.join(self.folder_전체종목, f'df_전체종목_{s_대상일자}.pkl'))
                dic_코드2종목명 = df_전체종목.set_index('종목코드')['종목명'].to_dict()
                dic_차트정보['li_전체종목'] = df_전체종목['종목코드'].to_list()

                # 진행현황 확인
                df_차트 = dic_차트정보['df_차트']
                li_전체종목 = dic_차트정보['li_전체종목']
                li_제외종목 = dic_차트정보['li_제외종목']
                li_완료종목 = df_차트['종목코드'].unique().tolist() if len(df_차트) > 0 else list()
                li_잔여종목 = [종목 for 종목 in li_전체종목 if 종목 not in li_제외종목 and 종목 not in li_완료종목]

                # 차트데이터 받아오기
                li_df차트 = list()
                for i, s_종목코드 in enumerate(li_잔여종목):
                    # tr 조회
                    if s_봉구분 == '일봉':
                        df_차트_종목별 = self.api.tr_주식일봉차트조회요청(s_종목코드=s_종목코드, s_시작일자=s_대상일자, s_종료일자=s_대상일자)
                        time.sleep(self.n_tr딜레이)
                    elif s_봉구분 == '분봉':
                        df_차트_종목별 = self.api.tr_주식분봉차트조회요청(s_종목코드=s_종목코드, s_시작일자=s_대상일자, s_종료일자=s_대상일자,
                                                           s_틱범위='1')
                        time.sleep(self.n_tr딜레이 - 0.1)
                    else:
                        df_차트_종목별 = pd.DataFrame()

                    # 데이터 존재 시 - 데이터 처리
                    s_종목명 = dic_코드2종목명[s_종목코드]
                    if len(df_차트_종목별) > 0:
                        df_차트_종목별['종목명'] = s_종목명
                        li_df차트.append(df_차트_종목별)

                    # 데이터 미존재 시 - 제외종목 등록
                    else:
                        dic_차트정보['li_제외종목'].append(s_종목코드)

                    # 데이터 병합 및 저장
                    if i % 100 == 0 or i == len(li_잔여종목) - 1:
                        # 데이터 미존재 시 통과
                        if len(li_df차트) == 0:
                            continue

                        # 수집한 데이터 처리
                        df_차트 = pd.concat(li_df차트, axis=0)
                        li_컬럼명_앞 = ['일자', '종목코드', '종목명']
                        li_컬럼명 = li_컬럼명_앞 + [컬럼 for 컬럼 in df_차트.columns if 컬럼 not in li_컬럼명_앞]
                        df_차트 = df_차트[li_컬럼명]

                        # 기존 데이터와 병합
                        df_기존 = pd.read_pickle(path_차트정보)['df_차트'] if os.path.exists(path_차트정보) else pd.DataFrame()
                        df_병합 = pd.concat([df_기존, df_차트], axis=0).drop_duplicates()
                        dic_차트정보['df_차트'] = df_병합.sort_values('종목코드').reset_index(drop=True)

                        # 데이터 저장
                        pd.to_pickle(dic_차트정보, path_차트정보)
                        li_df차트 = list()

                    # 로그 기록
                    n_전체종목 = len(dic_차트정보['li_전체종목'])
                    n_완료 = len(li_완료종목) + i + 1
                    n_진척률 = n_완료 / n_전체종목 * 100
                    s_데이터존재 = '수집' if len(df_차트_종목별) > 0 else '제외'
                    self.make_로그(f'{s_봉구분}-{s_대상일자}\n'
                                 f'  {n_진척률:.2f}%-{n_완료:,.0f}/{n_전체종목:,.0f}-{s_데이터존재}-{s_종목코드}_{s_종목명}')

    def update_db파일(self):
        """ 임시저장된 pkl 파일 읽어서 db 파일로 저장 """
        # 대상파일 확인
        li_대상파일 = sorted(파일 for 파일 in os.listdir(self.folder_임시) if 'dic_차트정보' in 파일 and '.pkl' in 파일)

        # db파일 업데이트
        for s_파일명 in li_대상파일:
            # 기준정보 정의
            dic_차트정보 =  pd.read_pickle(os.path.join(self.folder_임시, s_파일명))
            s_봉구분 = re.findall(r'.봉', s_파일명)[0]
            s_일자 = re.findall(r'\d{8}', s_파일명)[0]
            df_차트 = dic_차트정보['df_차트']
            li_전체종목 = dic_차트정보['li_전체종목']
            li_제외종목 = dic_차트정보['li_제외종목']
            li_수집종목 = df_차트['종목코드'].unique().tolist()

            # 신뢰성 검사 - 미충족 시 진행 종료
            if len(li_전체종목) != len(li_수집종목) + len(li_제외종목):
                continue

            # 데이터 정리
            df_차트 = df_차트[df_차트['시간'] <= '15:30:00'] if s_봉구분 == '분봉' else df_차트

            # db 정의
            path = os.path.join(self.folder_일봉, f'ohlcv_일봉_{s_일자[:4]}.db') if s_봉구분 == '일봉' else \
                os.path.join(self.folder_분봉, f'ohlcv_분봉_{s_일자[:4]}_{s_일자[4:6]}.db') if s_봉구분 == '분봉' else None
            s_테이블명 = f'ohlcv_일봉_{s_일자[:6]}' if s_봉구분 == '일봉' else \
                f'ohlcv_분봉_{s_일자}' if s_봉구분 == '분봉' else None

            # db 불러오기
            li_테이블명 = Tool.sql불러오기(path=path)
            df_기존 = Tool.sql불러오기(path=path, s_테이블명=s_테이블명) if s_테이블명 in li_테이블명 else pd.DataFrame()

            # db 업데이트
            li_정렬키 = ['일자', '종목코드'] if s_봉구분 == '일봉' else ['일자', '종목코드', '시간'] if s_봉구분 == '분봉' else None
            df_신규 = pd.concat([df_기존, df_차트], axis=0).drop_duplicates().sort_values(li_정렬키).reset_index(drop=True)

            # db 저장
            con = sqlite3.connect(path)
            df_신규.to_sql(name=s_테이블명, con=con, index=False, if_exists='replace')
            con.close()

            # 임시파일 삭제
            os.remove(os.path.join(self.folder_임시, s_파일명))

            # 로그 기록
            self.make_로그(f'{s_봉구분}-{s_일자}')


def run():
    """ 실행 함수 """
    c = CollectorBot()
    c.find_대상일자()
    c.get_차트데이터()
    c.update_db파일()


if __name__ == '__main__':
    run()
