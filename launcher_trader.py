import os
import sys
import json
import time

import pandas as pd
import multiprocessing as mp

import ut.로그maker, ut.폴더manager
import trader.bot_트레이딩, trader.bot_화면관리, trader.bot_실시간수신, trader.bot_실시간저장


# noinspection NonAsciiCharacters,PyPep8Naming,SpellCheckingInspection,PyUnreachableCode
class LauncherTrader:
    def __init__(self):
        # config 읽어 오기
        self.folder_프로젝트 = os.path.dirname(os.path.abspath(__file__))
        self.s_파일명 = os.path.basename(__file__).replace('.py', '')
        dic_config = json.load(open(os.path.join(self.folder_프로젝트, 'config.json'), mode='rt', encoding='utf-8'))

        # 로그 설정
        log = ut.로그maker.LogMaker(s_파일명=self.s_파일명, s_로그명='로그이름_trader')
        sys.stderr = ut.로그maker.StderrHook(path_에러로그=log.path_에러)
        self.make_로그 = log.make_로그

        # 폴더 정의
        dic_폴더정보 = ut.폴더manager.define_폴더정보()

        # 기준정보 정의
        self.s_오늘 = pd.Timestamp.now().strftime('%Y%m%d')
        self.s_종료시각 = dic_config['종료시각_트레이더']

        # 카카오 API 연결
        sys.path.append(dic_config['folder_kakao'])
        # noinspection PyUnresolvedReferences
        import API_kakao
        self.kakao = API_kakao.KakaoAPI()

        # 로그 기록
        self.make_로그(f'구동 시작')

    def run_트레이더(self):
        """ 트레이딩을 위한 bot 실행 - 병렬 구동 """
        # queue 생성
        # queue_mp_실시간저장 = mp.Queue()
        queue_mp_실시간저장 = mp.Manager().Queue()

        # 프로세스 정의
        # p_트레이딩 = mp.Process(target=trader.bot_트레이딩.run, name='bot_트레이딩')
        # p_화면관리 = mp.Process(target=trader.bot_화면관리.run, name='bot_화면관리')
        p_실시간수신 = mp.Process(target=trader.bot_실시간수신.run, args=(queue_mp_실시간저장,), name='bot_실시간수신')
        p_실시간저장 = mp.Process(target=trader.bot_실시간저장.run, args=(queue_mp_실시간저장,), name='bot_실시간저장')
        li_프로세스 = [p_실시간수신, p_실시간저장]

        # 프로세스 실행
        for p_봇 in li_프로세스:
            p_봇.start()

        # 시간 확인 후 종료
        while True:
            # 동작중 플래그 설정
            b_동작중 = True

            # 종료시간 이후라면 프로세스 종료
            if pd.Timestamp.now() > pd.Timestamp(self.s_종료시각):
            # if pd.Timestamp.now() > pd.Timestamp('18:00:00'):
                for p_봇 in li_프로세스:
                    if p_봇.is_alive():
                        p_봇.terminate()
                b_동작중 = False

            # 종료시간 내 처리
            else:
                for p_봇 in li_프로세스:
                    if not p_봇.is_alive():
                        # 종료코드 1 이면 재실행 - 오류로 인한 종료
                        if p_봇.exitcode == 1:
                            self.kakao.send_메세지(s_사용자='알림봇', s_수신인='여봉이', s_메세지=f'{p_봇.name} 모듈 재시작')
                            p_봇.terminate()
                            p_봇.join()
                            p_봇.start()
                        # 이외 경우 오류 알림
                        else:
                            self.send_카톡_오류발생(s_프로세스명=p_봇.name, n_오류코드=p_봇.exitcode)
                            b_동작중 = False

            # 대기 종료 확인
            if not b_동작중:
                break

            # 확인 주기 설정
            time.sleep(1)

        # 프로세스 종료
        for p_봇 in li_프로세스:
            p_봇.join()

        # 로그 기록
        for p_봇 in li_프로세스:
            if p_봇.exitcode <= 0:
                self.make_로그(f'{p_봇.name} 구동 완료')
            else:
                self.send_카톡_오류발생(s_프로세스명=p_봇.name, n_오류코드=p_봇.exitcode)

    def send_카톡_오류발생(self, s_프로세스명, n_오류코드):
        """ 실행 오류 발생 시 프로세스명 포함하여 카톡 메세지 송부 """
        # 메세지 정의
        s_메세지 = (f'!!! [{self.s_파일명}] !!!\n'
                 f'오류 발생 - {s_프로세스명} | code {n_오류코드}')

        # 메세지 송부
        self.kakao.send_메세지(s_사용자='알림봇', s_수신인='여봉이', s_메세지=s_메세지)


def run():
    """ 실행 함수 """
    l = LauncherTrader()
    l.run_트레이더()


if __name__ == '__main__':
    run()