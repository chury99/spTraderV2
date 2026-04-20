import os
import sys
import pandas as pd

import re
import json
import time
from tqdm import tqdm
from google import genai

import ut.폴더manager


# noinspection PyPep8Naming,NonAsciiCharacters,SpellCheckingInspection
def calc_상승확률(li_대상종목):
    """ 입력받은 종목 기준으로 상승확률 계산하여 리턴 """
    # 일봉 불러오기
    dic_폴더정보 = ut.폴더manager.define_폴더정보()
    folder_차트캐시 = dic_폴더정보['데이터|차트캐시']
    s_일자 = max(re.findall(r'\d{8}', 파일)[0]
                for 파일 in os.listdir(os.path.join(folder_차트캐시, '일봉1')) if '.pkl' in 파일)
    dic_일봉_전체 = pd.read_pickle(os.path.join(folder_차트캐시, '일봉1', f'dic_차트캐시_1일봉_{s_일자}.pkl'))
    dic_일봉 = {종목코드: df for 종목코드, df in dic_일봉_전체.items() if 종목코드 in li_대상종목}

    # 제미나이 기줁정보 정의
    client = genai.Client(api_key='AIzaSyA_skOteDsjZ63FdCdHumXPlz9GO96NZRg')
    s_모델 = 'gemini-3-flash-preview'

    # 상승확률 계산 - 5회 반복
    li_dic응답 = list()
    for i in range(5):
        # 제미나이 cli 적용
        # client = genai.Client(api_key='AIzaSyA_skOteDsjZ63FdCdHumXPlz9GO96NZRg')
        # s_모델 = 'gemini-3-flash-preview'
        s_질문 = ('너는 일 단위의 단기 매매를 전문으로 하는 주식 퀀트 분석 전문가야.\n'
                '투자의 기본 틀은 +10% 이상 시 익절, -3% 이하 시 손절하는 방식이야.\n'
                '아래 종목들은 오늘 장 마감 기준 내일 상승할 여력이 있는 후보 종목들인데,\n'
                '각 종목의 캔들 패턴, 거래량 변화, 추세 형성 패턴 등을 분석해서\n'
                '오늘 종가 대비 내일 고가가 +10% 이상 상승할 확률을 계산해줘. 단, 10% 상승하기 이전에 -3% 밑으로 내려가면 안돼.\n'
                '분석의 중요도는 추세 돌파 가능성, 캔들의 상승패턴 형성, 거래량 집중의 우선순위로 분석해줘.'
                '\n'
                '응답은 다른 말은 하지말고 반드시 아래 json 구조를 지켜주고, 단위는 표기하지 말아줘.\n'
                ' : {"종목코드" : {"종목명" : "종목명", "상승확률" : "확률숫자", "이유" : "이유"}}\n'
                '대상 종목은 아래와 같아.\n'
                f' : {li_대상종목}\n'
                '대상 종목의 일봉 정보는 아래와 같아. 약 한달 전부터 오늘까지의 일봉 데이터야. 마지막 일자 이후의 상승 확률을 구하면 돼.\n'
                f' : {dic_일봉}\n'
                '차트 데이터를 읽을 때 날짜 혼동하지 말고, 등락률 정확히 파악해서 결과에 혼선을 주지 않도록 해.')
        time.sleep(1)
        res = client.models.generate_content(model=s_모델, contents=s_질문)

        # 데이터 정리
        match = re.search(r'```json\n(.*?)\n```', res.text, re.DOTALL)
        s_응답내용 = match.group(1) if match else re.search(r'\{.*\}', res.text, re.DOTALL).group(0)
        dic_응답 = json.loads(s_응답내용)
        li_dic응답.append(dic_응답)

    # 최종 확률 산정 - 5회 중 best, worst 제외한 3개 값의 평균
    dic_상승확률 = li_dic응답[-1]
    for s_종목코드 in dic_상승확률.keys():
        li_확률 = [int(dic_응답.get(s_종목코드, dict()).get('상승확률', 0)) for dic_응답 in li_dic응답]
        li_확률_대상 = sorted(li_확률)[1: -1]
        dic_상승확률[s_종목코드]['상승확률'] = int(sum(li_확률_대상) / len(li_확률_대상)) if len(li_확률_대상) > 0 else 0

    return dic_상승확률, s_모델


if __name__ == '__main__':
    # 테스트 코드
    client = genai.Client(api_key='AIzaSyA_skOteDsjZ63FdCdHumXPlz9GO96NZRg')
    s_모델 = 'gemini-2.5-flash'
    s_모델 = 'gemini-3-flash-preview'
    s_질문 = ('안녕~ 넌 어느정도 성능을 가지고 있니?')
    time.sleep(1)
    res = client.models.generate_content(model=s_모델, contents=s_질문)

    pass