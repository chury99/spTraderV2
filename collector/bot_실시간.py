import os
import json
import pandas as pd

import ut.로그maker, ut.폴더manager


# noinspection NonAsciiCharacters,SpellCheckingInspection,PyPep8Naming
class Collector:
    def __init__(self):
        # config 읽어 오기
        self.folder_베이스 = os.path.dirname(os.path.abspath(__file__))
        self.folder_프로젝트 = os.path.dirname(self.folder_베이스)
        dic_config = json.load(open(os.path.join(self.folder_프로젝트, 'config.json'), mode='rt', encoding='utf-8'))

        # 로그 설정
        log = ut.로그maker.LogMaker(os.path.join(dic_config['folder_log'], f'{dic_config['로그이름_collector']}.log'))
        self.make_로그 = log.make_로그

        # 폴더 정의
        dic_폴더정보 = ut.폴더manager.define_폴더정보()
        self.folder_실시간 = dic_폴더정보['데이터|실시간']
        os.makedirs(self.folder_실시간, exist_ok=True)

        # 로그 기록
        self.make_로그(f'구동 시작')



        pass


if __name__ == '__main__':
    c = Collector()
