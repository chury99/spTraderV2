import os
import sys
import json

import pandas as pd
import asyncio

import ut.로그maker, ut.폴더manager, ut.도구manager
import xapi.WebsocketAPI_kiwoom, xapi.RestAPI_kiwoom


# noinspection NonAsciiCharacters,SpellCheckingInspection,PyPep8Naming,PyTypeChecker,PyAttributeOutsideInit
class TraderBot:
    def __init__(self, queue_mp_실시간저장=None):
        # config 읽어 오기
        self.folder_베이스 = os.path.dirname(os.path.abspath(__file__))
        self.folder_프로젝트 = os.path.dirname(self.folder_베이스)
        self.s_파일명 = os.path.basename(__file__).replace('.py', '')
        # dic_config = json.load(open(os.path.join(self.folder_프로젝트, 'config.json'), mode='rt', encoding='utf-8'))
        dic_config = ut.도구manager.config로딩()

        # 로그 설정
        log = ut.로그maker.LogMaker(s_파일명=self.s_파일명, s_로그명='로그이름_trader')
        sys.stderr = ut.로그maker.StderrHook(path_에러로그=log.path_에러)
        self.make_로그 = log.make_로그

        # 폴더 정의
        dic_폴더정보 = ut.폴더manager.define_폴더정보()
        self.folder_대상종목 = dic_폴더정보['데이터|대상종목']
        self.folder_조회순위 = dic_폴더정보['데이터|조회순위']
        self.folder_감시종목 = dic_폴더정보['매수매도|감시종목']
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
        path_대상종목 = os.path.join(self.folder_대상종목, f'df_대상종목_{self.s_오늘}.pkl')
        self.li_대상종목 = pd.read_pickle(path_대상종목)['종목코드'].to_list() if os.path.exists(path_대상종목) else None

        # 로그 기록
        self.make_로그(f'구동 시작')

    async def exec_실시간등록(self):
        """ tr_실시간종목조회순위 조회하여 대상종목 생성 후 실시간 등록 """
        # 감시종목 등록 - 장중 재구동 대응
        if len(self.li_감시종목) > 0:
            res = await self.api.req_실시간등록(li_종목코드=self.li_감시종목, li_데이터타입=['주문체결', '주식체결'])
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
                li_감시종목_조회 = [종목 for 종목 in df_조회순위['종목코드'] if 종목 != '' and 종목 in self.li_대상종목]\
                                    if self.li_대상종목 is not None else [종목 for 종목 in df_조회순위['종목코드'] if 종목 != '']
                li_감시종목_이전 = [종목 for 종목 in self.li_감시종목 if 종목 not in li_감시종목_조회]
                li_감시종목_전체 = li_감시종목_조회 + li_감시종목_이전
                li_감시종목_신규 = li_감시종목_전체[:100]
                li_감시종목_추가 = [종목 for 종목 in li_감시종목_조회 if 종목 not in self.li_감시종목]
                li_감시종목_해지 = [종목 for 종목 in li_감시종목_전체 if 종목 not in li_감시종목_신규]

                # 100개 초과 시 종목 해지 및 등록
                res_해지 = await self.api.req_실시간등록(li_종목코드=li_감시종목_해지, li_데이터타입=['주식체결'], b_등록해지=True)\
                            if len(li_감시종목_해지) > 0 else None
                res_등록 = await self.api.req_실시간등록(li_종목코드=li_감시종목_추가, li_데이터타입=['주문체결', '주식체결'])\
                            if len(li_감시종목_추가) > 0 else None
                self.li_감시종목 = li_감시종목_신규

                # 데이터 저장
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(None, self._조회순위저장, df_조회순위)
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(None, pd.to_pickle, self.li_감시종목, self.path_감시종목)

                # 로그기록
                s_로그 = (f'총 {len(self.li_감시종목)} 종목\n'
                        f'해지 {len(li_감시종목_해지)} - {res_해지}\n'
                        f'등록 {len(li_감시종목_추가)} - {res_등록}')
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(None, self.make_로그, s_로그)

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

        # 데이터 저장 - 기존 파일에 추가
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

    async def exec_저장(self):
        """ 웹소켓 API에서 수신받은 데이터를 queue_mp를 사용하여 전달 """
        while True:
            # queue 데이터 수신
            dic_데이터 = await self.api.queue_저장.get()

            # queue_mp로 데이터 전달
            if self.queue_mp_실시간저장 is not None:
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(None, self.queue_mp_실시간저장.put, dic_데이터)

    async def run_실시간시세(self):
        """ exec 함수들을 비동기로 구동 """
        # 웹소켓 서버 접속 및 수신대기 설정
        await self.api.ws_서버접속()
        task_수신대기 = asyncio.create_task(self.api.ws_메세지수신())
        await asyncio.sleep(1)

        # exec 함수 task 지정
        task_exec_실시간등록 = asyncio.create_task(self.exec_실시간등록())
        task_exec_ui = asyncio.create_task(self.exec_ui())
        task_exec_콘솔 = asyncio.create_task(self.exec_콘솔())
        task_exec_저장 = asyncio.create_task(self.exec_저장())

        # task 활성화
        await asyncio.gather(
            task_수신대기,
            task_exec_실시간등록,
            task_exec_ui,
            task_exec_콘솔,
            task_exec_저장
        )


# noinspection SpellCheckingInspection,PyPep8Naming,NonAsciiCharacters
def run(queue_mp_실시간저장=None):
    c = TraderBot(queue_mp_실시간저장=queue_mp_실시간저장)
    asyncio.run(c.run_실시간시세())


if __name__ == '__main__':
    try:
        run()
    except KeyboardInterrupt:
        print('\n### [ KeyboardInterrupt detected ] ###')
