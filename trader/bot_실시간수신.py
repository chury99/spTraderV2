import os
import sys
import json
import time

import pandas as pd
import asyncio
import multiprocessing as mp

import ut.로그maker, ut.폴더manager, ut.도구manager
import xapi.WebsocketAPI_kiwoom, xapi.RestAPI_kiwoom
import trader.bot_실시간저장


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
        self.folder_조회순위 = dic_폴더정보['데이터|조회순위']
        self.folder_감시종목 = dic_폴더정보['데이터|감시종목']
        os.makedirs(self.folder_실시간, exist_ok=True)
        os.makedirs(self.folder_조회순위, exist_ok=True)
        os.makedirs(self.folder_감시종목, exist_ok=True)

        # api 정의
        self.api = xapi.WebsocketAPI_kiwoom.WebsocketAPIkiwoom()
        self.rest = xapi.RestAPI_kiwoom.RestAPIkiwoom()

        # queue 생성
        self.queue_실시간등록 = asyncio.Queue()
        self.queue_mp_실시간저장 = queue_mp_실시간저장

        # 기준정보 정의
        self.s_오늘 = pd.Timestamp.now().strftime('%Y%m%d')
        self.path_감시종목 = os.path.join(self.folder_감시종목, f'li_감시종목_{self.s_오늘}.pkl')
        self.li_감시종목 = pd.read_pickle(self.path_감시종목) if os.path.exists(self.path_감시종목) else list()
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

    async def exec_실시간등록(self):
        """ tr_실시간종목조회순위 조회하여 대상종목 생성 후 실시간 등록 """
        # 감시종목 등록 - 장중 재구동 대응
        if len(self.li_감시종목) > 0:
            res = await self.api.req_실시간등록(li_종목코드=self.li_감시종목, li_데이터타입=['주식체결'])
            self.make_로그(f'총 {len(self.li_감시종목)}개\n'
                         f'{res}')

        # 루프 구동
        s_데이터초 = None
        while True:
            # 조회주기 설정
            n_현재초 = int(pd.Timestamp.now().strftime('%S'))
            if n_현재초 % 30 == 1:
                # 데이터 재조회 검사
                if s_데이터초 == str(n_현재초):
                    continue

                # tr 조회 - 동기 작업을 별도 스레드에서 실행
                loop = asyncio.get_running_loop()
                df_조회순위 = await loop.run_in_executor(None, self.rest.tr_실시간종목조회순위)
                s_데이터초 = str(n_현재초)

                # 데이터 변환
                li_감시종목_조회 = [종목 for 종목 in df_조회순위['종목코드'] if 종목 != '']
                li_감시종목_이전 = [종목 for 종목 in self.li_감시종목 if 종목 not in li_감시종목_조회]
                li_감시종목_전체 = li_감시종목_조회 + li_감시종목_이전
                li_감시종목_신규 = li_감시종목_전체[:100]
                li_감시종목_추가 = [종목 for 종목 in li_감시종목_조회 if 종목 not in self.li_감시종목]
                li_감시종목_해지 = [종목 for 종목 in li_감시종목_전체 if 종목 not in li_감시종목_신규]

                # 100개 초과 시 종목 해지 및 등록
                # res_해지 = None
                # res_등록 = None
                # if len(li_감시종목_전체) != len(li_감시종목_신규):
                res_해지 = await self.api.req_실시간등록(li_종목코드=li_감시종목_해지, li_데이터타입=['주식체결'], b_등록해지=True)\
                            if len(li_감시종목_해지) > 0 else None
                res_등록 = await self.api.req_실시간등록(li_종목코드=li_감시종목_추가, li_데이터타입=['주식체결'])\
                            if len(li_감시종목_추가) > 0 else None
                self.li_감시종목 = li_감시종목_신규


                # # 데이터 변환
                # li_감시종목_조회 = [종목 for 종목 in df_조회순위['종목코드'] if 종목 != '']
                # li_감시종목_신규 = [종목 for 종목 in li_감시종목_조회 if 종목 not in self.li_감시종목]
                #
                # # 변수 생성
                # li_감시종목_해지 = list()
                # res_해지 = None
                # res_등록 = None
                #
                # # 실시간 등록 - 신규 감시종목 존재 시 - 100개 한정
                # res = '추가 등록 미진행'
                # if len(li_감시종목_신규) > 0:
                #     n_감시종목_기존 = len(self.li_감시종목)
                #     n_감시종목_신규 = len(li_감시종목_신규)
                #     n_감시종목_전체 = n_감시종목_기존 + n_감시종목_신규
                #
                #     # 100개 초과 시 제외
                #     if n_감시종목_전체 > 100:
                #         n_감시종목_해지 = n_감시종목_전체 - 100
                #         n_해지인덱스 = -1 * n_감시종목_해지
                #         li_감시종목_해지 = self.li_감시종목[n_해지인덱스:]
                #         res_해지 = await self.api.req_실시간등록(li_종목코드=li_감시종목_해지, li_데이터타입=['주식체결'], b_등록해지=True)
                #         self.li_감시종목 = self.li_감시종목[:n_해지인덱스]
                #
                #     # 신규 감지종목 등록
                #     res_등록 = await self.api.req_실시간등록(li_종목코드=li_감시종목_신규, li_데이터타입=['주식체결'])
                #     self.li_감시종목 = li_감시종목_신규 + self.li_감시종목
                #     # self.li_감시종목 = self.li_감시종목[:100]

                # 데이터 저장
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(None, self._조회순위저장, df_조회순위)
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(None, pd.to_pickle, self.li_감시종목, self.path_감시종목)

                # 로그기록
                self.make_로그(f'총{len(self.li_감시종목)} 종목\n'
                             f'해지 {len(li_감시종목_해지)} 종목 - {res_해지}\n'
                             f'등록 {len(li_감시종목_추가)} 종목 - {res_등록}')

                # 구동 주기 설정
                await asyncio.sleep(0.1)

            # 구동시간 외 대기시간 설정 - cpu 부하 저감용
            else:
                await asyncio.sleep(0.1)

    def _조회순위저장(self, df_조회순위):
        """ 'exec_조회순위저장'의 동기 파일 I/O 작업을 별도로 분리 """
        # 기준 데이터 정의
        s_일자 = df_조회순위['일자'].values[0]
        s_시간 = df_조회순위['시간'].values[0]
        path_조회순위 = os.path.join(self.folder_조회순위, f'df_조회순위_{s_일자}.csv')

        # 기존 데이터에 동일 시간 존재 시 종료
        s_기존데이터 = open(path_조회순위, mode='rt', encoding='cp949').read() if os.path.exists(path_조회순위) else ''
        if s_시간 in s_기존데이터:
            return

        # df를 문자열로 변환
        li_조회순위 = [','.join(ary) for ary in df_조회순위.values.astype(str)]
        s_조회순위 = '\n'.join(li_조회순위) + '\n'
        s_컬럼명 = ','.join(df_조회순위.columns) + '\n'

        # 데이터 저장 - 한 파일로 통합
        s_데이터 = s_조회순위 if os.path.exists(path_조회순위) else s_컬럼명 + s_조회순위
        with open(path_조회순위, mode='at', encoding='cp949') as f:
            f.write(s_데이터)

    async def exec_ui(self):
        """ 웹소켓 API에서 수신받은 데이터를 ui로 전달 """
        while True:
            # queue 데이터 수신
            li_데이터 = await self.api.queue_ui.get()

            # 데이터 순차 처리
            for dic_데이터 in li_데이터:
                s_데이터타입 = dic_데이터['name']
                s_종목코드 = dic_데이터['item']
                dic_데이터_변동 = dic_데이터['values']

            # 데이터 전달
            # print(f'ui - {li_데이터}')
            pass

    async def exec_콘솔(self):
        """ 웹소켓 API에서 수신받은 데이터를 콘솔에 출력 """
        while True:
            # queue 데이터 수신
            li_데이터 = await self.api.queue_콘솔.get()

            # 데이터 순차 처리
            for dic_데이터 in li_데이터:
                s_데이터타입 = dic_데이터['name']
                s_종목코드 = dic_데이터['item']
                dic_데이터_변동 = dic_데이터['values']

                # 데이터 출력
                print(f'{len(li_데이터)}개 수신 - {s_데이터타입} - {s_종목코드}|{dic_데이터_변동}')

            # 데이터 출력
            # print(f'{s_데이터타입} - {s_종목코드}|{li_데이터}')
            # print(f'{s_데이터타입} - {s_종목코드}')

    async def exec_저장(self):
        """ 웹소켓 API에서 수신받은 데이터를 queue_mp를 사용하여 전달 """
        loop = asyncio.get_running_loop()
        while True:
            # queue 데이터 수신
            dic_데이터 = await self.api.queue_저장.get()

            # queue_mp로 데이터 전달
            if self.queue_mp_실시간저장 is not None:
                # self.queue_mp_실시간저장.put(dic_데이터)
                await loop.run_in_executor(None, self.queue_mp_실시간저장.put, dic_데이터)

    async def run_실시간시세(self):
    # async def activate_실시간시세(self):
        """ exec 함수들을 비동기로 구동 """
        # 웹소켓 서버 접속 및 수신대기 설정
        await self.api.connent_서버()
        task_수신대기 = asyncio.create_task(self.api.receive_수신메세지())
        await asyncio.sleep(1)

        # 실시간 등록
        # res = await self.api.req_실시간등록(li_종목코드=li_종목코드, li_데이터타입=li_데이터타입)
        # task_exec_실시간등록 = asyncio.create_task(self.exec_실시간등록())

        # exec 함수 task 지정
        task_exec_실시간등록 = asyncio.create_task(self.exec_실시간등록())
        # task_exec_조회순위저장 = asyncio.create_task(self.exec_조회순위저장())
        task_exec_ui = asyncio.create_task(self.exec_ui())
        task_exec_콘솔 = asyncio.create_task(self.exec_콘솔())
        task_exec_저장 = asyncio.create_task(self.exec_저장())

        # task 활성화
        await asyncio.gather(task_수신대기,
                             task_exec_실시간등록,
                             task_exec_ui, task_exec_콘솔, task_exec_저장)


# noinspection SpellCheckingInspection,PyPep8Naming,NonAsciiCharacters
def run(queue_mp_실시간저장=None):
    c = TraderBot(queue_mp_실시간저장=queue_mp_실시간저장)
    asyncio.run(c.run_실시간시세())


if __name__ == '__main__':
    run()
