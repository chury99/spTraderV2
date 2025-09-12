import os
import sys
import json
import pandas as pd
import asyncio
import multiprocessing as mp

import ut.로그maker, ut.폴더manager, ut.도구manager
import xapi.WebsocketAPI_kiwoom, xapi.RestAPI_kiwoom


# noinspection NonAsciiCharacters,SpellCheckingInspection,PyPep8Naming,PyTypeChecker
class TraderBot:
    def __init__(self):
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
        self.queue_조회순위 = asyncio.Queue()
        self.queue_실시간등록 = asyncio.Queue()

        # 기준정보 정의
        self.s_오늘 = pd.Timestamp.now().strftime('%Y%m%d')
        self.dic_이름2코드 = dict(체결시간='20', 현재가='10', 전일대비='11', 등락율='12', 최우선매도호가='27', 최우선매수호가='28',
                              거래량='15', 누적거래량='13', 누적거래대금='14', 시가='16', 고가='17', 저가='18', 전일대비기호='25',
                              전일거래량대비='26', 거래대금증감='29', 전일거래량대비비율='30', 거래회전율='31', 거래비용='32',
                              체결강도='228', 시가총액억='311', 장구분='290', KO접근도='691', 상한가발생시간='567', 하한가발생시간='568',
                              전일동시간거래량비율='851', 시가시간='1890', 고가시간='1891', 저가시간='1892',
                              매도체결량='1030', 매수체결량='1031', 매수비율='1032', 매도체결건수='1071', 매수체결건수='1072',
                              순간거래대금='1313', 매도체결량단건='1315', 매수체결량단건='1316', 순매수체결량='1314', CFD증거금='1497',
                              유지증거금='1498', 당일거래평균가='620', CFD거래비용='732', 대주거래비용='852', 거래소구분='9081')
        self.dic_장구분 = {'1': '장전시간외', '2': '장중', '3': '장후시간외'}
        self.path_감시종목 = os.path.join(self.folder_감시종목, f'li_감시종목_{self.s_오늘}.pkl')
        self.li_감시종목 = pd.read_pickle(self.path_감시종목) if os.path.exists(self.path_감시종목) else list()

        # 로그 기록
        self.make_로그(f'구동 시작')

    async def exec_실시간등록(self):
        """ tr_실시간종목조회순위 조회하여 대상종목 생성 후 실시간 등록 """
        # 감시종목 등록 - 장중 재구동 대응
        if len(self.li_감시종목) > 0:
            res = await self.api.req_실시간등록(li_종목코드=self.li_감시종목, li_데이터타입=['주식체결'])
            self.make_로그(f'총 {len(self.li_감시종목)}개 - 신규 {len(self.li_감시종목)}개 \n- {res}')

        # 루프 구동
        while True:
            # 조회주기 설정
            n_현재초 = int(pd.Timestamp.now().strftime('%S'))
            if n_현재초 % 30 == 1:
                # tr 조회 - 동기 작업을 별도 스레드에서 실행
                # df_조회순위 = self.rest.tr_실시간종목조회순위()
                loop = asyncio.get_running_loop()
                df_조회순위 = await loop.run_in_executor(None, self.rest.tr_실시간종목조회순위)
                await self.queue_조회순위.put(df_조회순위)

                # 데이터 변환
                li_감시종목_조회 = list(df_조회순위['종목코드'])
                li_감시종목_신규 = [종목 for 종목 in li_감시종목_조회 if 종목 not in self.li_감시종목]

                # 실시간 등록 - 신규 감시종목 존재 시
                res = '추가 등록 미진행'
                if len(li_감시종목_신규) > 0:
                    res = await self.api.req_실시간등록(li_종목코드=li_감시종목_신규, li_데이터타입=['주식체결'])
                    self.li_감시종목 = li_감시종목_신규 + self.li_감시종목

                # 로그기록
                self.make_로그(f'총 {len(self.li_감시종목)}개 - 신규 {len(li_감시종목_신규)}개 \n- {res}')

                # 구동 주기 설정
                await asyncio.sleep(1)

            # 구동시간 외 대기시간 설정 - cpu 부하 저감용
            else:
                await asyncio.sleep(1)


    async def exec_조회순위저장(self):
        """ 조회순위 조회 결과 받아서 저장 """
        while True:
            # queue 데이터 수신
            df_조회순위 = await self.queue_조회순위.get()

            # 추가 데이터 정리
            s_일자 = df_조회순위['일자'].values[0]
            s_시간 = df_조회순위['시간'].values[0].replace(':', '')

            # 데이터 저장 - 파일 미존재 시
            folder_조회순위_일자 = os.path.join(self.folder_조회순위, s_일자)
            os.makedirs(folder_조회순위_일자, exist_ok=True)
            path_조회순위 = os.path.join(folder_조회순위_일자, f'df_조회순위_{s_일자}_{s_시간}')
            if not os.path.exists(path_조회순위):
                ut.도구manager.df저장(df=df_조회순위, path=path_조회순위, li_타입=['csv'])

            # li_감시종목 저장
            pd.to_pickle(self.li_감시종목, self.path_감시종목)


    async def exec_ui(self):
        """ 웹소켓 API에서 수신받은 데이터를 ui로 전달 """
        while True:
            # queue 데이터 수신
            dic_데이터 = await self.api.queue_ui.get()

            # 추가 데이터 정의
            s_데이터타입 = dic_데이터['name']
            s_종목코드 = dic_데이터['item']
            dic_데이터_변동 = dic_데이터['values']

            # 데이터 전달
            # print(f'ui - {dic_데이터}')
            pass

    async def exec_콘솔(self):
        """ 웹소켓 API에서 수신받은 데이터를 콘솔에 출력 """
        while True:
            # queue 데이터 수신
            dic_데이터 = await self.api.queue_콘솔.get()

            # 추가 데이터 정의
            s_데이터타입 = dic_데이터['name']
            s_종목코드 = dic_데이터['item']
            dic_데이터_변동 = dic_데이터['values']

            # 데이터 출력
            print(f'{s_데이터타입} - {s_종목코드}|{dic_데이터}')

    async def exec_저장(self):
        """ 웹소켓 API에서 수신받은 데이터를 저장 """
        while True:
            # queue 데이터 수신
            dic_데이터 = await self.api.queue_저장.get()

            # 추가 데이터 정의
            s_데이터타입 = dic_데이터['name']
            s_종목코드 = dic_데이터['item']
            dic_데이터_변동 = dic_데이터['values']

            # 데이터 출력 - 0B | 주식체결
            if s_데이터타입 == '주식체결':
                # 데이터 정의
                li_컬럼명 = ['체결시간', '현재가', '등락율', '거래량', '누적거래량', '누적거래대금', '시가', '고가', '저가', '체결강도',
                          '전일거래량대비비율', '고가시간', '저가시간', '매도체결량', '매수체결량', '매수비율', '매도체결건수', '매수체결건수',
                          '장구분', '거래소구분']
                li_데이터 = [dic_데이터_변동[self.dic_이름2코드[이름]] if 이름 != '장구분'
                          else self.dic_장구분[dic_데이터_변동[self.dic_이름2코드[이름]]]
                          for 이름 in li_컬럼명]
                s_컬럼명 = ', '.join(['종목코드'] + li_컬럼명)
                s_데이터 = ', '.join([s_종목코드] + li_데이터)

                # 데이터 저장
                path_주식체결 = os.path.join(self.folder_실시간, f'주식체결_{self.s_오늘}.csv')
                s_주식체결 = s_데이터 if os.path.exists(path_주식체결) else s_컬럼명 + '\n' + s_데이터
                with open(path_주식체결, mode='at', encoding='cp949') as f:
                    f.write(f'{s_주식체결}\n')

    async def run_실시간시세(self, li_종목코드, li_데이터타입):
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
        task_exec_조회순위저장 = asyncio.create_task(self.exec_조회순위저장())
        task_exec_ui = asyncio.create_task(self.exec_ui())
        task_exec_콘솔 = asyncio.create_task(self.exec_콘솔())
        task_exec_저장 = asyncio.create_task(self.exec_저장())

        # task 활성화
        await task_수신대기
        await task_exec_실시간등록
        await task_exec_조회순위저장
        await task_exec_ui
        await task_exec_콘솔
        await task_exec_저장


# noinspection SpellCheckingInspection
def run():
    c = TraderBot()
    asyncio.run(c.run_실시간시세(li_종목코드=['097230'], li_데이터타입=['주식체결']))


if __name__ == '__main__':
    run()
