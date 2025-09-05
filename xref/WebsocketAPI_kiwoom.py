import os
import pandas as pd
import asyncio
import websockets
import json

from fcntl import FASYNC

import RestAPI_kiwoom


# noinspection SpellCheckingInspection,NonAsciiCharacters,PyPep8Naming,PyAttributeOutsideInit
class WebsocketAPIkiwoom:
    def __init__(self):
        # 기준정보 정의
        self.s_서버구분 = '실서버'   # 실서버, 모의서버
        self.s_거래소 = 'KRX'    # KRX:한국거래소, NXT:넥스트트레이드
        self.s_서버주소 = self.info_서버주소()

        # 폴더 정의
        self.folder_기준 = os.path.dirname(os.path.abspath(__file__))

        # 변수 정의
        self.s_오늘 = pd.Timestamp.now().strftime('%Y%m%d')
        self.websocket = None
        self.b_연결상태 = False
        self.b_동작중 = True

        # 토큰 발급
        rest = RestAPI_kiwoom.RestAPIkiwoom()
        self.s_접근토큰 = rest.auth_접근토큰갱신()

    async def connent_서버(self):
        """ 서버에 연결 요청 """
        try:
            # 웹소켓 연결
            self.websocket = await websockets.connect(self.s_서버주소)
            self.b_연결상태 = True

            # 로그인 요청
            dic_바디 = dict(trnm='LOGIN', token=self.s_접근토큰)
            await self.send_요청메세지(dic_바디=dic_바디)

        except Exception as e:
            print(f'서버접속 실패 - {e}')
            self.b_연결상태 = False

    async def disconnect_서버(self):
        """ 서버 접속 종료 """
        # 동작중 flag 초기화
        self.b_동작중 = False

        # 접속 종료
        if self.b_연결상태 and self.websocket:
            await self.websocket.close()
            self.b_연결상태 = False
            print('서버접속 종료')

    async def send_요청메세지(self, dic_바디):
        """ 서버로 요청 메세지 송부 (연결 없으면 자동으로 연결) """
        # 연결 없을 시 연결
        if not self.b_연결상태:
            await self.connent_서버()

        # 요청 메세지 전송
        dic_바디 = json.dumps(dic_바디) if not isinstance(dic_바디, str) else dic_바디
        await self.websocket.send(dic_바디)

    async def receive_수신메세지(self):
        """ 서버에서 오는 메세지 수신 """
        # 수신 대기 (동작중 일때만 대기)
        while self.b_동작중:
            try:
                # 수신 데이터 변환
                res = json.loads(await self.websocket.recv())
                s_서비스 = res.get('trnm')
                s_리턴코드 = res.get('return_code')
                s_리턴메세지 = res.get('return_msg')
                if s_리턴코드 != 0 and s_리턴코드 is not None:
                    print(f'수신 이상 - {s_리턴메세지}')

                # 결과 처리 - PING (수신값 그대로 재송신)
                if s_서비스 == 'PING':
                    await self.send_요청메세지(res)

                # 결과 처리 - LOGIN (로그인: 실패 시 메세지 출력)
                elif s_서비스 == 'LOGIN':
                    if s_리턴코드 != 0:
                        print(f'로그인 실패 - {res.get('return_msg')}')
                        await self.disconnect_서버()
                    else:
                        # 로그인 시 조건검색 목록조회 패킷 전송
                        await self.send_요청메세지(dic_바디=dict(trnm='CNSRLST'))

                # 결과 처리 - REG (등록: 실패 시 메세지 출력)
                elif s_서비스 == 'REG':
                    if s_리턴코드 != 0:
                        print(f'종목등록 실패 - {res.get('return_msg')}')
                        await self.disconnect_서버()

                # 결과 처리 - REAL (실시간시세 - 데이터 처리 함수 호출)
                elif s_서비스 == 'REAL':
                    await self.proc_실시간시세(res)

                # 결과 처리 - CNSR (조건검색 - 데이터 처리 함수 호출)
                elif s_서비스[:4] == 'CNSR':
                    await self.proc_조건검색(res)

                # 기타 - 오류 메세지 후 중단
                else:
                    print(f'미등록 서비스 - {s_서비스}')
                    await self.disconnect_서버()

            except websockets.ConnectionClosed:
                print('서버에 의한 종료')
                self.b_연결상태 = False
                self.b_동작중 = False
                await self.websocket.close()

    async def proc_실시간시세(self, res):
        """ REAL | 실시간시세 데이터 처리 """
        # 수신 데이터 변환
        s_서비스 = res.get('trnm')
        dic_데이터 = res.get('data')[0]

        # 추가 데이터 정의
        s_데이터타입 = dic_데이터['name']
        s_종목코드 = dic_데이터['item']
        dic_데이터_변동 = dic_데이터['values']

        # 데이터 출력 - 0B | 주식체결
        if s_데이터타입 == '주식체결':
            print(dic_데이터)

    async def proc_조건검색(self, res):
        """ CNSR | 조건검색 데이터 처리 """
        # 수신 데이터 변환
        s_서비스 = res.get('trnm')
        li_데이터 = res.get('data')

        # 기준정보 정의
        dic_서비스 = dict(목록조회='CNSRLST', 요청일반='CNSRREQ', 요청실시간='CNSRREQ', 실시간해제='CNSRCLR')

        # 서비스별 데이터 처리 - 목록조회
        if s_서비스 == 'CNSRLST':
            self.res조건검색_li_목록조회 = li_데이터

        # 서비스별 데이터 처리 - 요청일반
        elif s_서비스 == 'CNSRREQ' and 'cont_yn' in res:
            # 수신 데이터 업데이트
            self.res조건검색_li_요청일반 = self.res조건검색_li_요청일반 + li_데이터

            # 추가 조회 요청 (연속조회 존재 시)
            s_검색식번호 = res.get('seq').replace(' ', '')
            s_연속조회여부 = res.get('cont_yn')
            s_연속조회키 = res.get('next_key')
            if s_연속조회여부 == 'Y':
                dic_바디 = dict(trnm='CNSRREQ', seq=s_검색식번호, search_type='0', stex_tp='K',
                              cont_yn=s_연속조회여부, next_key=s_연속조회키)
                await self.send_요청메세지(dic_바디=dic_바디)

            # 연속조회 없을 시 종료
            if s_연속조회여부 == 'N':
                await self.websocket.close()

        # 서비스별 데이터 처리 - 요청실시간
        elif s_서비스 == 'CNSRREQ' and 'cont_yn' not in res:
            # 수신 데이터 업데이트
            self.res조건검색_li_요청실시간 = self.res조건검색_li_요청실시간 + li_데이터

            # 접속 종료
            await self.websocket.close()

        pass

    async def req_실시간등록(self, li_종목코드, li_데이터타입, s_기존유지=True):
        """ 실시간시세 조회를 위한 종목코드 및 데이터타입 등록 요청 (주문체결은 미등록시에도 자동 수신) """
        # 기준정보 정의
        dic_데이터타입 = dict(주문체결='00', 잔고='04', 주식기세='0A', 주식체결='0B', 주식우선호가='0C', 주식호가잔량='0D',
                         주식시간외호가='0E', 주식당일거래원='0F', ETFNAV='0G', 주식예상체결='0H', 업종지수='0J', 업종등락='0U',
                         주식종목정보='0g', ELW이론가='0m', 장시작시간='0s', ELW지표='0u', 종목프로그램매매='0w', VI발동해제='1h')

        # 변수 재정의
        li_데이터타입_코드 = [dic_데이터타입[타입] for 타입 in li_데이터타입]
        s_기존유지 = '1' if s_기존유지 else '0'

        # 등록 요청
        li_데이터 = [dict(item=li_종목코드, type=li_데이터타입_코드)]
        dic_바디 = dict(trnm='REG', grp_no='1', refresh=s_기존유지, data=li_데이터)
        await self.send_요청메세지(dic_바디=dic_바디)

        # 리턴 메세지 생성
        s_리턴메세지 = f'실시간등록 요청 완료 - {li_데이터}'

        return s_리턴메세지

    async def req_조건검색(self, s_데이터타입, s_검색식번호='0'):
        """ 조건검색 조회 요청 """
        # 기준정보 정의
        dic_데이터타입 = dict(목록조회='ka10171', 요청일반='ka10172', 요청실시간='ka10173', 실시간해제='ka10174')
        dic_서비스 = dict(목록조회='CNSRLST', 요청일반='CNSRREQ', 요청실시간='CNSRREQ', 실시간해제='CNSRCLR')

        # 변수 재정의
        dic_바디 = None

        # 바디 정의 - 목록조회
        if s_데이터타입 == '목록조회':
            self.res조건검색_li_목록조회 = list()
            dic_바디 = dict(trnm='CNSRLST')

        # 바디 정의 - 요청일반
        elif s_데이터타입 == '요청일반':
            self.res조건검색_li_요청일반 = list()
            dic_바디 = dict(trnm='CNSRREQ', seq=s_검색식번호, search_type='0', stex_tp='K', cont_yn='N', next_key='')

        # 바디 정의 - 요청실시간
        elif s_데이터타입 == '요청실시간':
            self.res조건검색_li_요청실시간 = [] if not hasattr(self, 'res조건검색_li_요청실시간') else self.res조건검색_li_요청실시간
            dic_바디 = dict(trnm='CNSRREQ', seq=s_검색식번호, search_type='1', stex_tp='K')

        # 바디 정의 - 실시간해제
        elif s_데이터타입 == '실시간해제':
            dic_바디 = dict(trnm='CNSRCLR', seq=s_검색식번호)

        # 기타 - 오류 메세지 후 중단
        else:
            print(f'미등록 데이터타입 - {s_데이터타입}')
            await self.disconnect_서버()

        # 서버 요청
        await self.send_요청메세지(dic_바디=dic_바디)

    def info_서버주소(self, s_서비스='공통'):
        """ 서비스명을 입력받아 해당하는 서버 주소 리턴 """
        # 기준정보 정의 - 호스트명
        dic_호스트 = dict(실서버='wss://api.kiwoom.com:10000',
                       모의서버='wss://mockapi.kiwoom.com:10000')

        # 기준정보 정의 - 서비스명
        dic_서비스 = dict(실시간시세='/api/dostk/websocket', 조건검색='/api/dostk/websocket', 공통='/api/dostk/websocket')

        # 서버주소 생성
        url_호스트 = dic_호스트[self.s_서버구분]
        url_서비스 = dic_서비스[s_서비스] if s_서비스 in dic_서비스 else None
        s_서버주소 = f'{url_호스트}{url_서비스}' if url_서비스 is not None else 'err_서비스미존재'

        return s_서버주소

    async def run_웹소켓(self):
        """ 웹소켓 서버 접속 및 수신 대기 """
        await self.connent_서버()
        await self.receive_수신메세지()


# noinspection PyPep8Naming,SpellCheckingInspection,NonAsciiCharacters
async def ws_get_조건검색(s_구분, s_검색식번호='5'):
    """ 조건검색에 등록해 놓은 대상종목 리스트 수신 후 리턴 """
    # 웹소켓 실행
    api = WebsocketAPIkiwoom()
    receive_task = asyncio.create_task(api.run_웹소켓())

    # 요청 등록 및 수신 대기
    await asyncio.sleep(1)
    await api.req_조건검색(s_데이터타입='요청일반', s_검색식번호=s_검색식번호)
    await api.req_조건검색(s_데이터타입='요청실시간', s_검색식번호=s_검색식번호)
    await receive_task

    # 수신 데이터 가져오기
    li_조건검색목록 = api.res조건검색_li_목록조회
    li_대상종목 = api.res조건검색_li_요청일반 if s_구분 == '일반' else api.res조건검색_li_요청실시간 if s_구분 == '실시간' else None
    # li_대상종목_실시간 = api.res조건검색_li_요청실시간 if s_구분 == '실시간' else None

    return li_조건검색목록, li_대상종목


# noinspection PyPep8Naming,SpellCheckingInspection,NonAsciiCharacters,PyShadowingNames
def get_조검검색(s_구분='실시간', s_검색식번호='5'):
    """ 조건검색에 등록된 대상종목 가져오기 => 나중에 trader로 옮길 것 """
    # 데이터 가져오기
    li_조건검색목록, li_대상종목 = asyncio.run(ws_get_조건검색(s_구분=s_구분, s_검색식번호=s_검색식번호))

    # 기준정보 정의
    dic_컬럼코드 = {'9001': '종목코드', '302': '종목명', '10': '현재가', '25': '전일대비기호', '11': '전일대비', '12': '등락율',
                '13': '누적거래량', '16': '시가', '17': '고가', '18': '저가'}

    # 대상종목 데이터 처리
    df_대상종목 = pd.DataFrame(li_대상종목)

    if s_구분 == '목록':
        df_대상종목 = pd.DataFrame(li_조건검색목록)
        df_대상종목.columns = ['검색식번호', '검색식명']

    elif s_구분 == '일반':
        li_컬럼명 = [dic_컬럼코드[코드] for 코드 in df_대상종목.columns]
        df_대상종목.columns = li_컬럼명

    elif s_구분 == '실시간':
        df_대상종목.columns = ['종목코드']
        df_대상종목['종목코드'] = df_대상종목['종목코드'].str[1:]
        df_대상종목 = df_대상종목.sort_values('종목코드').reset_index(drop=True)

    return df_대상종목

# noinspection PyPep8Naming,NonAsciiCharacters,SpellCheckingInspection
async def test_웹소켓():
    # task 생성
    api = WebsocketAPIkiwoom()
    task_수신대기 = asyncio.create_task(api.run_웹소켓())

    # 요청 등록
    await asyncio.sleep(1)
    # res = await api.req_실시간등록(li_종목코드=['468530'], li_데이터타입=['주식체결'])
    # await api.req_조건검색(s_데이터타입='목록조회')
    # await api.req_조건검색(s_데이터타입='요청일반', s_검색식번호='5')
    # await api.req_조건검색(s_데이터타입='요청실시간', s_검색식번호='5')

    # 수신 완료까지 대기
    await task_수신대기

    pass


#######################################################################################################################
if __name__ == '__main__':
    # df_대상종목 = get_조검검색('목록')
    asyncio.run(test_웹소켓())

    api = WebsocketAPIkiwoom()
    res = api.req_실시간등록(li_종목코드=['468530'], li_데이터타입=['주식체결'])
    pass
