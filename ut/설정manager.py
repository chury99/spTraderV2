import os
import sys
import json
import re
import shutil

import pandas as pd
import paramiko
from pandas.core.methods.selectn import SelectNSeries

import ut.로그maker, ut.폴더manager


# noinspection NonAsciiCharacters,PyPep8Naming,SpellCheckingInspection
class ConfigManager:
    def __init__(self):
        """ config.json 파일 확인 후 구동 중인 환경에 맞도록 변수 정의 """
        # config 읽어 오기
        self.folder_베이스 = os.path.dirname(os.path.abspath(__file__))
        self.folder_프로젝트 = os.path.dirname(self.folder_베이스)
        self.s_파일명 = os.path.basename(__file__).replace('.py', '')
        dic_config = json.load(open(os.path.join(self.folder_프로젝트, 'config.json'), mode='rt', encoding='utf-8'))

        # 구동 중인 os 확인
        dic_운영체제 = dict(darwin='mac', win32='win', linux='linux')
        self.s_운영체제 = dic_운영체제[sys.platform]

        # config 정의
        self.dic_config = dic_config
        self.dic_config.update(
            folder_work=dic_config['folder_work_mac'] if self.s_운영체제 == 'mac' else
                        dic_config['folder_work_win'] if self.s_운영체제 == 'win' else None,
            folder_log=dic_config['folder_log_mac'] if self.s_운영체제 == 'mac' else
                        dic_config['folder_log_win'] if self.s_운영체제 == 'win' else None,
            folder_kakao=dic_config['folder_kakao_mac'] if self.s_운영체제 == 'mac' else
                        dic_config['folder_kakao_win'] if self.s_운영체제 == 'win' else None,
            path_파이썬=dic_config['path_파이썬_mac'] if self.s_운영체제 == 'mac' else
                        dic_config['path_파이썬_win'] if self.s_운영체제 == 'win' else None
        )


if __name__ == '__main__':
    pass
