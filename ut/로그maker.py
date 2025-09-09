import os
import sys
import pandas as pd


# noinspection NonAsciiCharacters,PyPep8Naming
class LogMaker:
    def __init__(self, path_로그):
        self.path_로그 = path_로그

    def make_로그(self, s_내용, li_출력=None):
        """ 입력 받은 s_내용에 시간 및 구분자 붙여서 저장 """
        # 정보 설정
        s_시각 = pd.Timestamp.now().strftime('%H:%M:%S')
        s_파일 = os.path.basename(sys.argv[0]).replace('.py', '')
        s_모듈 = sys._getframe(1).f_code.co_name

        # 로그 생성
        s_로그 = f'[{s_시각}] {s_파일} | {s_모듈} | {s_내용}'

        # log 출력
        li_출력 = ['콘솔', '파일'] if li_출력 is None else li_출력
        if '콘솔' in li_출력:
            print(s_로그)
        if '파일' in li_출력:
            with open(self.path_로그, mode='at', encoding='cp949') as f:
                f.write(f'{s_로그}\n')


if __name__ == '__main__':
    pass
