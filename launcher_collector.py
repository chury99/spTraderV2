import os
import sys
import json
import time

import pandas as pd
import multiprocessing as mp
import asyncio

import ut.로그maker, ut.폴더manager, ut.도구manager as Tool
import xapi.RestAPI_kiwoom, xapi.WebsocketAPI_kiwoom
import collector.bot_정보수집, collector.bot_실시간


# noinspection NonAsciiCharacters,PyPep8Naming,SpellCheckingInspection,PyUnresolvedReferences
class LauncherCollector:
    def __init__(self):
        # config 읽어 오기
        self.folder_프로젝트 = os.path.dirname(os.path.abspath(__file__))
        dic_config = json.load(open(os.path.join(self.folder_프로젝트, 'config.json'), mode='rt', encoding='utf-8'))

        # 로그 설정
        log = ut.로그maker.LogMaker(s_파일명=os.path.basename(__file__).replace('.py', ''),
                                  path_로그=os.path.join(dic_config['folder_log'], f'{dic_config['로그이름_collector']}.log'))
        self.make_로그 = log.make_로그

        # 폴더 정의
        dic_폴더정보 = ut.폴더manager.define_폴더정보()

        # 기준정보 정의
        self.s_오늘 = pd.Timestamp.now().strftime('%Y%m%d')
        self.path_파이썬 = dic_config['path_파이썬']
        self.s_파일명 = os.path.basename(__file__).replace('.py', '')
        self.s_종료시각 = dic_config['종료시각_실시간']

        # 카카오 API 연결
        sys.path.append(dic_config['folder_kakao'])
        import API_kakao
        self.kakao = API_kakao.KakaoAPI()


        # 로그 기록
        self.make_로그(f'구동 시작')

    def launcher_정보수집(self):
        """ 정보수집 모듈 실행 """
        # 프로세스 정의 및 실행
        p_수집봇 = mp.Process(target=collector.bot_정보수집.run, name='bot_정보수집')
        p_수집봇.start()

        # 프로세스 종료
        p_수집봇.join()

        # 로그 기록
        if p_수집봇.exitcode <= 0:
            self.make_로그(f'{p_수집봇.name} 구동 완료')
        else:
            self.send_카톡_오류발생(s_프로세스명=p_수집봇.name)

    def launcher_실시간(self):
        """ 실시간 모듈 실행 - 시간 확인 후 종료 """
        # 프로세스 정의 및 실행
        p_수집봇 = mp.Process(target=collector.bot_실시간.run, name='bot_실시간')
        p_수집봇.start()

        # 시간 확인 후 종료
        while True:
            # 종료시간 이후라면 프로세스 종료
            if pd.Timestamp.now() > pd.Timestamp(self.s_종료시각):
            # if pd.Timestamp.now() > pd.Timestamp('15:46:00'):
                if p_수집봇.is_alive():
                    p_수집봇.terminate()
                    break
            # 종료시간 내라면 미구동 시 카톡 송부
            else:
                if not p_수집봇.is_alive():
                    self.send_카톡_오류발생(s_프로세스명=p_수집봇.name)
                    break

            # 확인 주기 설정
            time.sleep(1)

        # 프로세스 종료
        p_수집봇.join()

        # 로그 기록
        if p_수집봇.exitcode <= 0:
            self.make_로그(f'{p_수집봇.name} 구동 완료')
        else:
            self.send_카톡_오류발생(s_프로세스명=p_수집봇.name)

    def launcher_차트수집(self):
        """ 차트수집 모듈 실행 - 실시간 모듈 종료 후 바로 진행 """
        pass

    def launcher_캐시생성(self):
        """ 캐시생성 모듈 실행 """
        pass

    def send_카톡_오류발생(self, s_프로세스명):
        """ 실행 오류 발생 시 프로세스명 포함하여 카톡 메세지 송부 """
        # 메세지 정의
        s_메세지 = (f'!!! [{self.s_파일명}] !!!\n'
                 f'모듈 실행 중 오류 발생 - {s_프로세스명}')

        # 메세지 송부
        self.kakao.send_메세지(s_사용자='알림봇', s_수신인='여봉이', s_메세지=s_메세지)


def run():
    """ 실행 함수 """
    l = LauncherCollector()
    l.launcher_정보수집()
    l.launcher_실시간()
    l.launcher_차트수집()
    l.launcher_캐시생성()


if __name__ == '__main__':
    run()
