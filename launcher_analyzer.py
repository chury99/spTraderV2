import os
import sys
import json
import time

import pandas as pd
import multiprocessing as mp

import ut.로그maker, ut.폴더manager, ut.파일manager
import analyzer.bot_초봉분석

# noinspection NonAsciiCharacters,PyPep8Naming,SpellCheckingInspection
class LauncherAnalyzer:
    def __init__(self):
        # config 읽어 오기
        self.folder_프로젝트 = os.path.dirname(os.path.abspath(__file__))
        self.s_파일명 = os.path.basename(__file__).replace('.py', '')
        # dic_config = json.load(open(os.path.join(self.folder_프로젝트, 'config.json'), mode='rt', encoding='utf-8'))
        dic_config = ut.도구manager.config로딩()

        # 로그 설정
        log = ut.로그maker.LogMaker(s_파일명=self.s_파일명, s_로그명='로그이름_analyer')
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

    def run_백테스팅(self):
        """ 백테스팅 모듈 실행 """
        # 프로세스 정의
        p_분석봇 = mp.Process(target=analyzer.bot_초봉분석.run, name='bot_초봉분석')

        # 프로세스 실행 및 종료 대기
        p_분석봇.start()
        p_분석봇.join()

        # 로그 기록
        if p_분석봇.exitcode <= 0:
            self.make_로그(f'{p_분석봇.name} 구동 완료')
        else:
            self.send_카톡_오류발생(s_프로세스명=p_분석봇.name, n_오류코드=p_분석봇.exitcode)

    def send_카톡_오류발생(self, s_프로세스명, n_오류코드):
        """ 실행 오류 발생 시 프로세스명 포함하여 카톡 메세지 송부 """
        # 메세지 정의
        s_메세지 = (f'!!! [{self.s_파일명}] !!!\n'
                 f'오류 발생 - {s_프로세스명} | code {n_오류코드}')

        # 메세지 송부
        self.kakao.send_메세지(s_사용자='알림봇', s_수신인='여봉이', s_메세지=s_메세지)


def run():
    """ 실행 함수 """
    l = LauncherAnalyzer()
    l.run_백테스팅()


if __name__ == '__main__':
    try:
        run()
    except KeyboardInterrupt:
        print('\n### [ KeyboardInterrupt detected ] ###')
