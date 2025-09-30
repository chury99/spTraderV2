import _queue
import os
import sys
import json
import time

import pandas as pd
import asyncio
import multiprocessing as mp

from charset_normalizer.cli import query_yes_no

import ut.로그maker, ut.폴더manager, ut.도구manager
import xapi.WebsocketAPI_kiwoom, xapi.RestAPI_kiwoom, xapi.wsFID_kiwoom


# noinspection NonAsciiCharacters,SpellCheckingInspection,PyPep8Naming,PyTypeChecker,PyAttributeOutsideInit
class TraderBot:
    def __init__(self, queue_mp_실시간저장=None):
        # config 읽어 오기
        self.folder_베이스 = os.path.dirname(os.path.abspath(__file__))
        self.folder_프로젝트 = os.path.dirname(self.folder_베이스)
        self.s_파일명 = os.path.basename(__file__).replace('.py', '')
        dic_config = json.load(open(os.path.join(self.folder_프로젝트, 'config.json'), mode='rt', encoding='utf-8'))

        # 로그 설정
        log = ut.로그maker.LogMaker(s_파일명=self.s_파일명, s_로그명='로그이름_trader')
        sys.stderr = ut.로그maker.StderrHook(path_에러로그=log.path_에러)
        self.make_로그 = log.make_로그

        # 폴더 정의
        dic_폴더정보 = ut.폴더manager.define_폴더정보()
        self.folder_실시간 = dic_폴더정보['데이터|실시간']
        os.makedirs(self.folder_실시간, exist_ok=True)

        # queue 생성
        self.queue_mp_실시간저장 = queue_mp_실시간저장

        # 기준정보 정의
        self.s_오늘 = pd.Timestamp.now().strftime('%Y%m%d')
        # self.set_기준정보()

        # 로그 기록
        self.make_로그(f'구동 시작')

    # def set_기준정보(self):
    #     """ 기준정보 정의 """
    #     self.dic_코드2이름 = {'10':'현재가', '11':'전일대비', '12':'등락율', '13':'누적거래량', '14':'누적거래대금', '15':'거래량',
    #                       '16':'시가', '17':'고가', '18':'저가', '20':'체결시간', '25':'전일대비기호', '26':'전일거래량대비',
    #                       '27':'최우선매도호가', '28':'최우선매수호가', '29':'거래대금증감', '30':'전일거래량대비비율', '31':'거래회전율',
    #                       '32':'거래비용',
    #                       '228':'체결강도', '290':'장구분',
    #                       '302':'종목명', '311':'시가총액억',
    #                       '567':'상한가발생시간', '568':'하한가발생시간',
    #                       '620':'당일거래평균가', '691':'KO접근도',
    #                       '732':'CFD거래비용',
    #                       '851':'전일동시간거래량비율', '852':'대주거래비용',
    #                       '900': '주문수량', '901': '주문가격', '902': '미체결수량', '903': '체결누계금액', '904': '원주문번호',
    #                       '905': '주문구분', '906': '매매구분', '907': '매도수구분', '908': '주문체결시간', '909': '체결번호',
    #                       '910': '체결가', '911': '체결량', '912': '주문업무분류', '913': '주문상태', '914': '단위체결가',
    #                       '915': '단위체결량', '919': '거부사유', '920': '화면번호', '921': '터미널번호', '922': '신용구분',
    #                       '923': '대출일', '938': '당일매매수수료', '939': '당일매매세금',
    #                       '1030':'매도체결량', '1031':'매수체결량', '1032':'매수비율', '1071':'매도체결건수', '1072':'매수체결건수',
    #                       '1313':'순간거래대금', '1314':'순매수체결량', '1315':'매도체결량단건', '1316':'매수체결량단건',
    #                       '1497':'CFD증거금', '1498':'유지증거금',
    #                       '1890':'시가시간', '1891':'고가시간', '1892':'저가시간',
    #                       '9001':'종목코드', '9081':'거래소구분','9201':'계좌번호', '9203':'주문번호', '9205':'관리자사번',
    #                       '2134': '거래소구분주문', '2135': '거래소구분명', '2136': 'SOR여부',
    #                       '10010':'시간외단일가현재가'}
    #     self.dic_이름2코드 = {value: key for key, value in self.dic_코드2이름.items()}
    #     self.dic_장구분 = {'1': '장전시간외', '2': '장중', '3': '장후시간외'}
    #     self.dic_매도매수구분 = {'1': '매도', '2': '매수'}

    def exec_실시간저장(self):
        """ 웹소켓 API에서 수신받은 데이터를 저장 """
        # 기준정보 정의
        # n_배치_크기 = 500
        # li_배치데이터 = list()
        # path_주식체결 = os.path.join(self.folder_실시간, f'주식체결_{self.s_오늘}.csv')
        # li_컬럼명 = ['체결시간', '현재가', '등락율', '거래량', '누적거래량', '누적거래대금', '시가', '고가', '저가', '체결강도',
        #           '전일거래량대비비율', '고가시간', '저가시간', '매도체결량', '매수체결량', '매수비율',
        #           '매도체결건수', '매수체결건수', '장구분', '거래소구분']
        dic_n배치크기, dic_path, dic_li컬럼명 = self.set_기준정보_실시간저장()
        dic_li배치데이터 = {key: [] for key in dic_n배치크기}

        # 프로그램 시작 시 파일이 없으면 헤더를 미리 기록
        for s_항목명, path_실시간파일 in dic_path.items():
            if not os.path.exists(path_실시간파일):
                # s_컬럼명_헤더 = ','.join(['종목코드'] + li_컬럼명)
                s_컬럼명_헤더 = ','.join(['종목코드'] + dic_li컬럼명[s_항목명])
                with open(path_실시간파일, mode='wt', encoding='cp949') as f:
                    f.write(f'{s_컬럼명_헤더}\n')

        while True:
            li_수신데이터 = list()
            try:
                # 데이터 수신
                while True:
                    li_수신데이터 = li_수신데이터 + self.queue_mp_실시간저장.get_nowait()
            except _queue.Empty:
                # pass
                time.sleep(0.01)

            # 대량으로 인출한 데이터 묶음을 처리
            for dic_데이터 in li_수신데이터:
                s_항목명 = dic_데이터['name']
                s_종목코드 = dic_데이터['item']
                dic_데이터_변동 = dic_데이터['values']
                if s_항목명 == '주식체결':
                    fid = xapi.wsFID_kiwoom.fid_주식체결_0B()
                    li_배치데이터 = dic_li배치데이터[s_항목명]
                    # s_종목코드 = dic_데이터['item']
                    # dic_데이터_변동 = dic_데이터['values']
                    # li_데이터 = [dic_데이터_변동[self.dic_이름2코드[이름]] if 이름 != '장구분'
                    #           else self.dic_장구분[dic_데이터_변동[self.dic_이름2코드[이름]]]
                    #           for 이름 in li_컬럼명]
                    li_데이터 = [dic_데이터_변동[fid.dic_이름2코드[이름]] if 이름 != '장구분'
                              else fid.dic_장구분[dic_데이터_변동[fid.dic_이름2코드[이름]]]
                              for 이름 in dic_li컬럼명[s_항목명]]
                    # li_데이터 = [dic_데이터_변동[self.dic_이름2코드[이름]] if 이름 != '장구분'
                    #           else self.dic_장구분[dic_데이터_변동[self.dic_이름2코드[이름]]]
                    #           for 이름 in dic_li컬럼명[s_항목명]]
                    s_데이터_한줄 = ','.join([s_종목코드] + li_데이터)
                    li_배치데이터.append(s_데이터_한줄)

                    # 파일에 쓸 배치 크기에 도달하면 쓰기 실행
                    # if len(li_배치데이터) >= n_배치_크기:
                    if len(li_배치데이터) >= dic_n배치크기[s_항목명]:
                        s_데이터_블록 = '\n'.join(li_배치데이터) + '\n'
                        try:
                            # with open(path_주식체결, mode='at', encoding='cp949') as f:
                            with open(dic_path[s_항목명], mode='at', encoding='cp949') as f:
                                f.write(s_데이터_블록)
                            li_배치데이터.clear()
                        except Exception as e:
                            self.make_로그(f'파일 쓰기 - {e}')

    def set_기준정보_실시간저장(self):
        """ 실시간 데이터 저장을 위한 기준정보 정의 """
        # 파일 저장 단위 정의
        dic_n배치크기 = dict(
            주식체결=500
        )

        # 파일 저장 위치 지정
        dic_path = dict(
            주식체결=os.path.join(self.folder_실시간, f'주식체결_{self.s_오늘}.csv'),
        )

        # 수집할 데이터 항목 지정
        dic_li컬럼명 = dict(
            주식체결=['체결시간', '현재가', '등락율', '거래량', '누적거래량', '누적거래대금', '시가', '고가', '저가', '체결강도',
                  '전일거래량대비비율', '고가시간', '저가시간', '매도체결량', '매수체결량', '매수비율',
                  '매도체결건수', '매수체결건수', '장구분', '거래소구분']
        )

        return dic_n배치크기, dic_path, dic_li컬럼명


# noinspection SpellCheckingInspection,PyPep8Naming,NonAsciiCharacters
def run(queue_mp_실시간저장=None):
    c = TraderBot(queue_mp_실시간저장=queue_mp_실시간저장)
    c.exec_실시간저장()


if __name__ == '__main__':
    run()
