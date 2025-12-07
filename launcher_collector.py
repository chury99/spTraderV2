import os
import sys
import json
import time

import pandas as pd
import multiprocessing as mp

import ut.로그maker, ut.폴더manager, ut.파일manager
import collector.bot_정보수집, collector.bot_차트수집, collector.bot_캐시생성

# noinspection NonAsciiCharacters,PyPep8Naming,SpellCheckingInspection
class LauncherCollector:
    def __init__(self):
        # config 읽어 오기
        self.folder_프로젝트 = os.path.dirname(os.path.abspath(__file__))
        self.s_파일명 = os.path.basename(__file__).replace('.py', '')
        dic_config = json.load(open(os.path.join(self.folder_프로젝트, 'config.json'), mode='rt', encoding='utf-8'))

        # 로그 설정
        log = ut.로그maker.LogMaker(s_파일명=self.s_파일명, s_로그명='로그이름_collector')
        sys.stderr = ut.로그maker.StderrHook(path_에러로그=log.path_에러)
        self.make_로그 = log.make_로그

        # 폴더 정의
        dic_폴더정보 = ut.폴더manager.define_폴더정보()

        # 기준정보 정의
        self.s_오늘 = pd.Timestamp.now().strftime('%Y%m%d')
        self.s_종료시각 = dic_config['종료시각']

        # 카카오 API 연결
        sys.path.append(dic_config['folder_kakao'])
        # noinspection PyUnresolvedReferences
        import API_kakao
        self.kakao = API_kakao.KakaoAPI()

        # 로그 기록
        self.make_로그(f'구동 시작')

    def run_정보수집(self):
        """ 정보수집 모듈 실행 """
        # 프로세스 정의
        p_수집봇 = mp.Process(target=collector.bot_정보수집.run, name='bot_정보수집')

        # 프로세스 실행 및 종료 대기
        p_수집봇.start()
        p_수집봇.join()

        # 로그 기록
        if p_수집봇.exitcode <= 0:
            self.make_로그(f'{p_수집봇.name} 구동 완료')
        else:
            self.send_카톡_오류발생(s_프로세스명=p_수집봇.name, n_오류코드=p_수집봇.exitcode)

    def run_차트수집(self):
        """ 차트수집 모듈 실행 - 실시간 모듈 종료 후 바로 진행 """
        # 프로세스 정의
        dic_수집봇 = dict(s_타겟=collector.bot_차트수집.run, s_네임='bot_차트수집')

        # 실행 대기
        dt_시작시각 = pd.Timestamp(self.s_종료시각) + pd.Timedelta(minutes=1)
        while True:
            if pd.Timestamp.now() >= dt_시작시각:
                self.make_로그(f'\n{dic_수집봇['s_네임']} 구동 시작')
                break
            else:
                dt_현재 = pd.Timestamp.now()
                s_현재시각 = dt_현재.strftime('%H:%M:%S')
                s_시작시각 = dt_시작시각.strftime('%H:%M:%S')
                s_남은시간 = str(dt_시작시각 - dt_현재).split(' ')[-1].split('.')[0]
                print(f'\r[{s_현재시각}] 실행시각 {s_시작시각} - {s_남은시간} 후 실행 예정', end='', flush=True)
                time.sleep(1)

        # 프로세스 실행 - 비정상 종료 시 재실행
        dt_에러발생 = pd.Timestamp.now()
        while True:
            # 프로세스 구동
            p_수집봇 = mp.Process(target=dic_수집봇['s_타겟'], name=dic_수집봇['s_네임'])
            p_수집봇.start()
            p_수집봇.join()

            # 종상 종료 시 종료
            if p_수집봇.exitcode <= 0:
                break

            # 비정상 종료 처리
            else:
                time.sleep(1)
                if pd.Timestamp.now() - dt_에러발생 < pd.Timedelta(seconds=3):
                    break
                else:
                    self.kakao.send_메세지(s_사용자='알림봇', s_수신인='여봉이', s_메세지=f'{p_수집봇.name} 모듈 재시작')
                    dt_에러발생 = pd.Timestamp.now()

        # 로그 기록
        if p_수집봇.exitcode <= 0:
            self.make_로그(f'{p_수집봇.name} 구동 완료')
        else:
            self.send_카톡_오류발생(s_프로세스명=p_수집봇.name, n_오류코드=p_수집봇.exitcode)

    def run_캐시생성(self):
        """ 캐시생성 모듈 실행 """
        # 프로세스 정의
        p_수집봇 = mp.Process(target=collector.bot_캐시생성.run, name='bot_캐시생성')

        # 프로세스 실행 및 종료 대기
        p_수집봇.start()
        p_수집봇.join()

        # 로그 기록
        if p_수집봇.exitcode <= 0:
            self.make_로그(f'{p_수집봇.name} 구동 완료')
        else:
            self.send_카톡_오류발생(s_프로세스명=p_수집봇.name, n_오류코드=p_수집봇.exitcode)

    def ut_파일정리(self):
        """ 파일manager 모듈 실행 """
        # 프로세스 정의
        p_수집봇 = mp.Process(target=ut.파일manager.run, name='bot_파일정리')

        # 프로세스 실행 및 종료 대기
        p_수집봇.start()
        p_수집봇.join()

        # 로그 기록
        if p_수집봇.exitcode <= 0:
            self.make_로그(f'{p_수집봇.name} 구동 완료')
        else:
            self.send_카톡_오류발생(s_프로세스명=p_수집봇.name, n_오류코드=p_수집봇.exitcode)

    def send_카톡_오류발생(self, s_프로세스명, n_오류코드):
        """ 실행 오류 발생 시 프로세스명 포함하여 카톡 메세지 송부 """
        # 메세지 정의
        s_메세지 = (f'!!! [{self.s_파일명}] !!!\n'
                 f'오류 발생 - {s_프로세스명} | code {n_오류코드}')

        # 메세지 송부
        self.kakao.send_메세지(s_사용자='알림봇', s_수신인='여봉이', s_메세지=s_메세지)


def run():
    """ 실행 함수 """
    l = LauncherCollector()
    l.run_정보수집()
    l.run_차트수집()
    l.run_캐시생성()
    l.ut_파일정리()


if __name__ == '__main__':
    try:
        run()
    except KeyboardInterrupt:
        print('\n### [ KeyboardInterrupt detected ] ###')
