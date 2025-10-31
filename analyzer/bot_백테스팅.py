import os
import sys
import json
import time
import re
import multiprocessing as mp

import pandas as pd
from tqdm import tqdm
import matplotlib.pyplot as plt
import paramiko

import ut.로그maker, ut.폴더manager, ut.도구manager as Tool, ut.차트maker
import xapi.RestAPI_kiwoom, xapi.WebsocketAPI_kiwoom


# noinspection NonAsciiCharacters,SpellCheckingInspection,PyPep8Naming
class AnalyzerBot:
    def __init__(self, b_디버그모드=False):
        # config 읽어 오기
        self.folder_베이스 = os.path.dirname(os.path.abspath(__file__))
        self.folder_프로젝트 = os.path.dirname(self.folder_베이스)
        self.s_파일명 = os.path.basename(__file__).replace('.py', '')
        dic_config = json.load(open(os.path.join(self.folder_프로젝트, 'config.json'), mode='rt', encoding='utf-8'))

        # 로그 설정
        log = ut.로그maker.LogMaker(s_파일명=self.s_파일명, s_로그명='로그이름_analyzer')
        sys.stderr = ut.로그maker.StderrHook(path_에러로그=log.path_에러)
        self.make_로그 = log.make_로그

        # 폴더 정의
        dic_폴더정보 = ut.폴더manager.define_폴더정보()
        self.folder_work = dic_폴더정보['folder_work']
        self.folder_차트캐시 = dic_폴더정보['데이터|차트캐시']
        self.folder_전체종목 = dic_폴더정보['데이터|전체종목']
        self.folder_백테스팅 = dic_폴더정보['분석|백테스팅']
        os.makedirs(self.folder_차트캐시, exist_ok=True)
        os.makedirs(self.folder_백테스팅, exist_ok=True)

        # 기준정보 정의
        self.s_오늘 = pd.Timestamp.now().strftime('%Y%m%d')
        self.b_디버그모드 = b_디버그모드
        self.n_멀티코어수 = mp.cpu_count() - 2
        self.dic_매개변수 = dict()

        # 서버정보 정의
        dic_서버정보 = json.load(open(os.path.join(self.folder_프로젝트, 'server_info.json'), mode='rt', encoding='utf-8'))
        self.dic_서버접속 = dic_서버정보['sftp']
        self.dic_서버폴더 = dic_서버정보['folder']

        # 차트maker 정의
        self.chart = ut.차트maker.ChartMaker()

        # 로그 기록
        self.make_로그(f'구동 시작')

    def sync_캐시파일(self):
        """ 서버와 캐시파일 동기화 """
        # 기준정보 정의
        folder_로컬 = self.folder_차트캐시
        folder_서버 = f'{self.dic_서버폴더['server_work']}{self.folder_차트캐시.replace(self.folder_work, '')}'

        # 대상폴더 확인
        li_대상폴더 = sorted(폴더 for 폴더 in os.listdir(folder_로컬) if os.path.isdir(os.path.join(folder_로컬, 폴더)))

        # 파일 동기화
        li_동기화파일명 = list()
        for s_대상폴더 in li_대상폴더:
            s_로컬폴더 = os.path.join(folder_로컬, s_대상폴더)
            s_서버폴더 = os.path.join(folder_서버, s_대상폴더)
            li_동기화파일명_개별 = Tool.sftp_동기화_파일명(folder_로컬=s_로컬폴더, folder_서버=s_서버폴더, s_모드='서버2로컬')
            li_동기화파일명 = li_동기화파일명 + li_동기화파일명_개별

        # 로그 기록
        self.make_로그(f'{len(li_동기화파일명):,.0f}개 파일 완료\n - {li_동기화파일명}')

    def make_매수매도(self):
        """ 초봉 데이터 기준 매수매도 정보 생성 """

        pass

    def make_결과정리(self):
        """ 매수매도 결과에 따라 수익 정리 """
        pass

    def make_수익요약(self):
        """ 수익 요약 및 리포트 발행 """
        pass


def run():
    """ 실행 함수 """
    a = AnalyzerBot(b_디버그모드=True)
    # a.sync_캐시파일()
    a.make_매수매도()
    a.make_결과정리()
    a.make_수익요약()

if __name__ == '__main__':
    run()
