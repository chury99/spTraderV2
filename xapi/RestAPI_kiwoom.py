import os
import json
import pandas as pd
import requests
import time


# noinspection SpellCheckingInspection,NonAsciiCharacters,PyPep8Naming,PyShadowingNames,PyTypeChecker
class RestAPIkiwoom:
    def __init__(self):
        # config 읽어 오기
        self.folder_베이스 = os.path.dirname(os.path.abspath(__file__))
        self.folder_프로젝트 = os.path.dirname(self.folder_베이스)
        dic_config = json.load(open(os.path.join(self.folder_프로젝트, 'config.json'), mode='rt', encoding='utf-8'))

        # 기준정보 정의
        self.s_서버구분 = dic_config['서버구분']   # 실서버, 모의서버
        self.s_거래소 = dic_config['거래소구분']    # KRX:한국거래소, NXT:넥스트트레이드
        self.n_tr딜레이 = 0.2    # tr 요청간 딜레이 - 이용약관 제 11조 (API 호출 횟수 제한) 기준 초당 5건

        # 토큰 발급
        self.s_접근토큰 = self.auth_접근토큰갱신()

    # noinspection PyTypeChecker
    def auth_접근토큰갱신(self):
        """ 저장된 접속토큰 갱신 후 리턴 """
        # 파일 정보 정의
        path_접속키 = os.path.join(self.folder_베이스, 'kiwoomKey.json')
        path_접근토큰 = os.path.join(self.folder_베이스,'kiwoomToken.json')

        # 토큰 불러오기
        if os.path.exists(path_접근토큰):
            dic_접근토큰 = json.load(open(path_접근토큰, mode='rt', encoding='utf-8'))
        else:
            dic_접근토큰 = self.tr_접근토큰발급(path_접속키)
            json.dump(dic_접근토큰, open(path_접근토큰, mode='wt', encoding='utf-8'), indent=2, ensure_ascii=False)

        # 정상수신 확인
        if isinstance(dic_접근토큰, str):
            breakpoint()

        # 만료여부 확인
        dt_토큰만료 = pd.Timestamp(dic_접근토큰['expires_dt'])
        if pd.Timestamp.now() > dt_토큰만료 - pd.Timedelta(hours=12):
            self.tr_접근토큰폐기(path_접속키, path_접근토큰)
            dic_접근토큰 = self.tr_접근토큰발급(path_접속키)
            json.dump(dic_접근토큰, open(path_접근토큰, mode='wt', encoding='utf-8'), indent=2, ensure_ascii=False)

        # 토큰값 정의
        s_접근토큰 = dic_접근토큰['token']

        return s_접근토큰

    def tr_접근토큰발급(self, path_접속키):
        """ au10001 | 저장된 appKey, secretKey 읽어와서 토큰 발급 후 리턴 """
        # 입력정보 불러오기 - 접속키
        dic_접속키 = json.load(open(path_접속키, mode='rt', encoding='utf-8'))

        # 데이터 요청
        s_서버주소 = self.info_서버주소(s_서비스='접근토큰발급')
        dic_헤더 = {'Content-Type': 'application/json;charset=UTF-8'}
        dic_요청 = dict(grant_type='client_credentials', appkey=dic_접속키['appkey'], secretkey=dic_접속키['secretkey'])
        res = requests.post(url=s_서버주소, headers=dic_헤더, json=dic_요청)

        # 응답 확인
        dic_데이터 = res.json()
        if dic_데이터['return_code'] != 0:
            return f'err-{dic_데이터["return_code"]}-{dic_데이터["return_msg"]}'

        return dic_데이터

    def tr_접근토큰폐기(self, path_접속키, path_접근토큰):
        """ au10002 | 서버에서 발행한 토큰 폐기 요청 """
        # 입력정보 불러오기 - 접속키, 접근토큰
        dic_접속키 = json.load(open(path_접속키, mode='rt', encoding='utf-8'))
        dic_접근토큰 = json.load(open(path_접근토큰, mode='rt', encoding='utf-8'))

        # 데이터 요청
        s_서버주소 = self.info_서버주소(s_서비스='접근토큰폐기')
        dic_헤더 = {'Content-Type': 'application/json;charset=UTF-8'}
        dic_요청 = dict(appkey=dic_접속키['appkey'], secretkey=dic_접속키['secretkey'], token=dic_접근토큰['token'])
        res = requests.post(url=s_서버주소, headers=dic_헤더, json=dic_요청)

        # 응답 확인
        dic_데이터 = res.json()
        if dic_데이터['return_code'] != 0:
            return 'err_서버응답이상'

        return dic_데이터

    def tr_주식주문(self, s_구분, s_종목코드, n_주문수량, n_주문단가, s_매매구분='IOC보통', s_조건단가=None):
        """ 주문 | 주식 주문 관련 매수, 매도, 정정, 취소 요청 """
        # 기준정보 정의
        dic_tr아이디 = dict(매수='kt10000', 매도='kt10001', 정정='kt10002', 취소='kt10003')
        dic_매매구분 = dict(보통='0', 시장가='3', 조건부지정가='5', 최유리지정가='6', 최우선지정가='7',
                        장마감후시간외='81', 장시작전시간외='61', 시간외단일가='62',
                        IOC보통='10', IOC시장가='13', IOC최유리='16', FOK보통='20', FOK시장가='23', FOK최유리='26',
                        스톱지정가='28', 중간가='29', IOC중간가='30', FOK중간가='31')

        # tr 요청
        s_서버주소 = self.info_서버주소(s_서비스='주문')
        s_tr아이디 = dic_tr아이디[s_구분]
        dic_바디 = dict(dmst_stex_tp=self.s_거래소, stk_cd=s_종목코드, ord_qty=str(n_주문수량), ord_uv=str(n_주문단가),
                      trde_tp=dic_매매구분[s_매매구분])
        dic_데이터 = self.get_tr데이터(s_서버주소=s_서버주소, s_tr아이디=s_tr아이디, dic_바디=dic_바디)

        # 데이터 정리
        if dic_데이터 != 'err_서버응답이상':
            s_결과코드 = dic_데이터['return_code']
            s_결과메세지 = dic_데이터['return_msg']
            s_주문번호 = dic_데이터['ord_no']
            s_주문거래소 = dic_데이터['dmst_stex_tp']
            return dic_데이터
        else:
            print(f'주문이상 - {dic_데이터}')
            return dic_데이터

    def tr_업종별주가요청(self, s_시장):
        """ 업종 | ka20002 | 시장 내 전체 종목에 대한 정보 조회 후 리턴 """
        # 기준정보 정의
        dic_시장구분 = dict(코스피='0', 코스닥='1', 코스피200='2')
        dic_업종코드 = dict(코스피='001', 코스닥='101',
                        대형주='002', 중형주='003', 소형주='004', KOSPI200='201', KOSTAR='302', KRX100='701')
        dic_거래소구분 = dict(KRX='1', NXT='2', 통합='3')

        # tr 요청
        s_서버주소 = self.info_서버주소(s_서비스='업종')
        s_tr아이디 = 'ka20002'
        s_리스트키 = 'inds_stkpc'
        dic_바디 = dict(mrkt_tp=dic_시장구분[s_시장], inds_cd=dic_업종코드[s_시장], stex_tp=dic_거래소구분[self.s_거래소])
        dic_데이터 = self.get_tr데이터(s_서버주소=s_서버주소, s_tr아이디=s_tr아이디, dic_바디=dic_바디, s_리스트키=s_리스트키)

        # 데이터 정리
        df_데이터 = pd.DataFrame(dic_데이터[s_리스트키])
        df_종목별주가 = pd.DataFrame()
        if len(df_데이터) > 0:
            df_종목별주가['종목코드'] = df_데이터['stk_cd'].astype(str)
            df_종목별주가['종목명'] = df_데이터['stk_nm'].astype(str)
            df_종목별주가['현재가'] = df_데이터['cur_prc'].astype(int).abs()
            df_종목별주가['전일대비기호'] = df_데이터['pred_pre_sig'].astype(int)
            df_종목별주가['전일대비'] = df_데이터['pred_pre'].astype(int)
            df_종목별주가['등락률'] = df_데이터['flu_rt'].astype(float)
            df_종목별주가['현재거래량'] = df_데이터['now_trde_qty'].astype(int)
            df_종목별주가['매도호가'] = df_데이터['sel_bid'].astype(int).abs()
            df_종목별주가['매수호가'] = df_데이터['buy_bid'].astype(int).abs()
            df_종목별주가['시가'] = df_데이터['open_pric'].astype(int).abs()
            df_종목별주가['고가'] = df_데이터['high_pric'].astype(int).abs()
            df_종목별주가['저가'] = df_데이터['low_pric'].astype(int).abs()

        return df_종목별주가

    def tr_체결잔고요청(self):
        """ 계좌 | kt00005 | 계좌 예수금 및 보유 주식 조회 후 리턴 """
        # tr 요청
        s_서버주소 = self.info_서버주소(s_서비스='계좌')
        s_tr아이디 = 'kt00005'
        s_리스트키 = 'stk_cntr_remn'
        dic_바디 = dict(dmst_stex_tp=self.s_거래소)
        dic_데이터 = self.get_tr데이터(s_서버주소=s_서버주소, s_tr아이디=s_tr아이디, dic_바디=dic_바디, s_리스트키=s_리스트키)

        # 데이터 정리
        dic_계좌잔고 = dict(n_d2예수금=int(dic_데이터['entr_d2']))
        df_데이터 = pd.DataFrame(dic_데이터[s_리스트키])
        df_종목별잔고 = pd.DataFrame()
        if len(df_데이터) > 0:
            df_종목별잔고['종목코드'] = df_데이터['stk_cd'].astype(str)
            df_종목별잔고['종목명'] = df_데이터['stk_nm'].astype(str)
            df_종목별잔고['현재잔고'] = df_데이터['cur_qty'].astype(int)
            df_종목별잔고['현재가'] = df_데이터['cur_prc'].astype(int)
            df_종목별잔고['매입단가'] = df_데이터['buy_uv'].astype(int)
            df_종목별잔고['매입금액'] = df_데이터['pur_amt'].astype(int)
            df_종목별잔고['평가금액'] = df_데이터['evlt_amt'].astype(int)
            df_종목별잔고['평가손익'] = df_데이터['evltv_prft'].astype(int)
            df_종목별잔고['손익률'] = df_데이터['pl_rt'].astype(float)

        return dic_계좌잔고, df_종목별잔고

    def tr_주식일봉차트조회요청(self, s_종목코드, s_기준일from=None, s_기준일to=None):
        """ 차트 | ka10081 | 종목별 일봉 정보 조회 후 리턴 """
        # tr 요청
        s_서버주소 = self.info_서버주소(s_서비스='차트')
        s_tr아이디 = 'ka10081'
        s_수정주가 = '0'    # 0:미적용, 1:적용
        s_기준일to = pd.Timestamp.now().strftime('%Y%m%d') if s_기준일to is None else s_기준일to
        s_기준일from = s_기준일to if s_기준일from is None else s_기준일from
        s_리스트키 = 'stk_dt_pole_chart_qry'
        dic_바디 = dict(stk_cd=s_종목코드, base_dt=s_기준일to, upd_stkpc_tp=s_수정주가)
        dic_데이터 = self.get_tr데이터(s_서버주소=s_서버주소, s_tr아이디=s_tr아이디, dic_바디=dic_바디, s_리스트키=s_리스트키,
                                 s_기준일from=s_기준일from)

        # 데이터 정리
        df_데이터 = pd.DataFrame(dic_데이터[s_리스트키])
        df_일봉 = pd.DataFrame()
        if len(df_데이터) > 0:
            df_일봉['일자'] = df_데이터['dt'].astype(str)
            df_일봉['종목코드'] = s_종목코드
            df_일봉['종목명'] = self.dic_종목코드2종목명[s_종목코드]
            df_일봉['시가'] = df_데이터['open_pric'].astype(int)
            df_일봉['고가'] = df_데이터['high_pric'].astype(int)
            df_일봉['저가'] = df_데이터['low_pric'].astype(int)
            df_일봉['종가'] = df_데이터['cur_prc'].astype(int)
            df_일봉['거래량'] = df_데이터['trde_qty'].astype(int)
            df_일봉['거래대금(백만)'] = df_데이터['trde_prica'].astype(int)

            # from 일자 정리
            df_일봉 = df_일봉[df_일봉['일자'] >= s_기준일from]

        return df_일봉

    def tr_주식분봉차트조회요청(self, s_종목코드, s_틱범위, s_기준일from=None, s_기준일to=None):
        """ 차트 | ka10080 | 종목별 분봉 정보 조회 후 리턴 """
        # tr 요청
        s_서버주소 = self.info_서버주소(s_서비스='차트')
        s_tr아이디 = 'ka10080'
        s_수정주가 = '0'    # 0:미적용, 1:적용
        s_기준일to = pd.Timestamp.now().strftime('%Y%m%d') if s_기준일to is None else s_기준일to
        s_기준일from = s_기준일to if s_기준일from is None else s_기준일from
        s_리스트키 = 'stk_min_pole_chart_qry'
        dic_바디 = dict(stk_cd=s_종목코드, tic_scope=s_틱범위, upd_stkpc_tp=s_수정주가)
        dic_데이터 = self.get_tr데이터(s_서버주소=s_서버주소, s_tr아이디=s_tr아이디, dic_바디=dic_바디, s_리스트키=s_리스트키,
                                 s_기준일from=s_기준일from)

        # 데이터 정리
        df_데이터 = pd.DataFrame(dic_데이터[s_리스트키])
        df_분봉 = pd.DataFrame()
        if len(df_데이터) > 0:
            df_분봉['일자'] = df_데이터['cntr_tm'].str[:8]
            df_분봉['종목코드'] = s_종목코드
            df_분봉['종목명'] = self.dic_종목코드2종목명[s_종목코드]
            df_분봉['시간'] = df_데이터['cntr_tm'].str[8:10] + ':' + df_데이터['cntr_tm'].str[10:12] + ':' + df_데이터['cntr_tm'].str[12:]
            df_분봉['시가'] = df_데이터['open_pric'].astype(int).abs()
            df_분봉['고가'] = df_데이터['high_pric'].astype(int).abs()
            df_분봉['저가'] = df_데이터['low_pric'].astype(int).abs()
            df_분봉['종가'] = df_데이터['cur_prc'].astype(int).abs()
            df_분봉['거래량'] = df_데이터['trde_qty'].astype(int)

            # from 일자 정리
            df_분봉 = df_분봉[df_분봉['일자'] >= s_기준일from]

        return df_분봉

    def tr_실시간종목조회순위(self, s_기간='5'):
        """ 종목정보 | ka00198 | 실시간 종목 조회 순위 정보 조회 후 리턴 \n
            s_기간: [1]1분 [2]10분 [3]1시간 [4] 당일누적 [5] 30초 (MTS default 30초) """
        # tr 요청
        s_서버주소 = self.info_서버주소(s_서비스='종목정보')
        s_tr아이디 = 'ka00198'
        s_리스트키 = 'item_inq_rank'
        dic_바디 = dict(qry_tp=s_기간)
        dic_데이터 = self.get_tr데이터(s_서버주소=s_서버주소, s_tr아이디=s_tr아이디, dic_바디=dic_바디, s_리스트키=s_리스트키)

        # 데이터 정리
        df_데이터 = pd.DataFrame(dic_데이터[s_리스트키])
        df_실시간조회순위 = pd.DataFrame()
        if len(df_데이터) > 0:
            df_실시간조회순위['일자'] = df_데이터['dt'].astype(str)
            df_실시간조회순위['시간'] = df_데이터['tm'].str[:2] + ':' + df_데이터['tm'].str[2:4] + ':' + df_데이터['tm'].str[4:]
            df_실시간조회순위['종목코드'] = df_데이터['stk_cd'].astype(str)
            df_실시간조회순위['종목명'] = df_데이터['stk_nm'].astype(str)
            df_실시간조회순위['빅데이터순위'] = df_데이터['bigd_rank'].astype(int)
            df_실시간조회순위['순위등락'] = df_데이터['rank_chg'].astype(int)
            df_실시간조회순위['순위등락부호'] = df_데이터['rank_chg_sign'].astype(str)
            df_실시간조회순위['과거현재가'] = df_데이터['past_curr_prc'].astype(int).abs()
            df_실시간조회순위['기준가대비부호'] = df_데이터['base_comp_sign'].astype(int)
            df_실시간조회순위['기준가대비등락률'] = df_데이터['base_comp_chgr'].astype(float)
            df_실시간조회순위['직전기준대비부호'] = df_데이터['prev_base_sign'].astype(int)
            try:
                df_실시간조회순위['직전기준대비등락률'] = df_데이터['prev_base_chgr'].astype(float)
            except:
                pass

        return df_실시간조회순위

    def get_tr데이터(self, s_서버주소, s_tr아이디, dic_바디, s_리스트키=None, s_기준일from=None):
        """ tr 조회 요청 후 응답 데이터 리턴 """
        # 변수 생성
        s_연속조회여부 = 'Y'
        s_연속조회키 = None
        dic_데이터_누적 = dict()

        # 데이터 조회
        while s_연속조회여부 == 'Y':
            # 데이터 요청
            dic_헤더 = self.info_헤더(s_tr아이디=s_tr아이디) if s_연속조회여부 is None else\
                self.info_헤더(s_tr아이디=s_tr아이디, s_연속조회여부=s_연속조회여부, s_연속조회키=s_연속조회키)
            res = requests.post(url=s_서버주소, headers=dic_헤더, json=dic_바디)

            # 응답 확인
            dic_데이터 = res.json()
            if res.status_code != 200 or dic_데이터['return_code'] != 0:
                return dic_데이터['return_msg']

            # 데이터 정리
            s_tr아이디, s_연속조회여부, s_연속조회키 = res.headers['api-id'], res.headers['cont-yn'], res.headers['next-key']

            # 데이터 업데이트
            for key, value in dic_데이터.items():
                if key == s_리스트키:
                    dic_데이터_누적[key] = dic_데이터_누적[key] + value if key in dic_데이터_누적 else value
                else:
                    dic_데이터_누적[key] = value

            # 일자 기준 조회완료 확인
            if s_기준일from is not None:
                # 일자항목 찾기
                s_일자키 = None
                for key, value in dic_데이터[s_리스트키][0].items():
                    if value[:2] in ['19', '20'] and len(value) >= 8:
                        s_일자키 = key

                # 조회 완료 확인
                s_조회일last = dic_데이터_누적[s_리스트키][-1][s_일자키]
                if s_조회일last < s_기준일from:
                    break

            # tr 딜레이
            if s_연속조회여부 == 'Y':
                time.sleep(self.n_tr딜레이)

        return dic_데이터_누적

    def info_헤더(self, s_tr아이디, s_연속조회여부=None, s_연속조회키=None):
        """ 요청하는 tr의 헤더 작성 후 리턴 """
        # 헤더 정보 생성
        dic_헤더 = {'Content-Type': 'application/json;charset=UTF-8',
                  'authorization': f'Bearer {self.s_접근토큰}',
                  'api-id': s_tr아이디}

        # 연속 조회시 추가
        if s_연속조회여부 is not None:
            dic_헤더.update({'cont-yn': s_연속조회여부,
                           'next-key': s_연속조회키})

        return dic_헤더

    def info_서버주소(self, s_서비스):
        """ 서비스명을 입력받아 해당하는 서버 주소 리턴 """
        # 기준정보 정의 - 호스트명
        dic_호스트 = dict(실서버='https://api.kiwoom.com',
                       모의서버='https://mockapi.kiwoom.com')

        # 기준정보 정의 - 서비스명
        dic_서비스 = dict(접근토큰발급='/oauth2/token', 접근토큰폐기='/oauth2/revoke',
                       계좌='/api/dostk/acnt', 공매도='/api/dostk/shsa', 기관외국인='/api/dostk/frgnistt',
                       대차거래='/api/dostk/slb', 순위정보='/api/dostk/rkinfo', 시세='/api/dostk/mrkcond',
                       신용주문='/api/dostk/crdordr', 업종='/api/dostk/sect', 종목정보='/api/dostk/stkinfo',
                       주문='/api/dostk/ordr', 차트='/api/dostk/chart', 테마='	/api/dostk/thme',
                       ELW='/api/dostk/elw', ETF='/api/dostk/etf')
        dic_웹소켓 = dict(실시간시세='/api/dostk/websocket', 조건검색='/api/dostk/websocket')

        # 서버주소 생성
        url_호스트 = dic_호스트[self.s_서버구분]
        url_서비스 = dic_서비스[s_서비스] if s_서비스 in dic_서비스 else None
        s_서버주소 = f'{url_호스트}{url_서비스}' if url_서비스 is not None else 'err_서비스미존재'

        return s_서버주소

    # def download_전체종목(self):
    #     """ 전체 종목코드 및 종목명 다운운받아 저장 후 리턴 """
    #     # 폴더생성
    #     os.makedirs(self.folder_전체종목, exist_ok=True)
    #
    #     # 다운로드
    #     li_df_전체종목 = list()
    #     for s_시장 in ['코스피', '코스닥']:
    #         df_종목별주가 = self.tr_업종별주가요청(s_시장=s_시장)
    #         li_df_전체종목.append(df_종목별주가)
    #     df_전체종목 = pd.concat(li_df_전체종목, axis=0)
    #
    #     # 종목코드 변환용 dic 생성
    #     dic_종목코드2종목명 = df_전체종목.set_index('종목코드')['종목명'].to_dict()
    #
    #     # 파일 저장
    #     df_전체종목.to_pickle(os.path.join(self.folder_전체종목, f'df_전체종목_{self.s_오늘}.pkl'))
    #     df_전체종목.to_csv(os.path.join(self.folder_전체종목, f'df_전체종목_{self.s_오늘}.csv'), index=False, encoding='cp949')
    #     pd.to_pickle(dic_종목코드2종목명, os.path.join(self.folder_전체종목, f'dic_종목코드2종목명_{self.s_오늘}.pkl'))
    #
    #     return dic_종목코드2종목명


if __name__ == '__main__':
    # noinspection PyPep8Naming,NonAsciiCharacters,SpellCheckingInspection
    def test():
        api = RestAPIkiwoom()
        # dic_계좌잔고, df_종목별잔고 = api.tr_체결잔고요청()
        # df_일봉 = api.tr_주식일봉차트조회요청(s_종목코드='000020', s_기준일from='20230101', s_기준일to='20250831')
        # df_분봉 = api.tr_주식분봉차트조회요청(s_종목코드='000020', s_틱범위='1', s_기준일from='20250825', s_기준일to='20250831')
        # df_종목별주가 = api.tr_업종별주가요청(s_시장='코스피')
        # dic_종목코드2종목명 = api.download_전체종목()
        # df_실시간조회순위 = api.tr_실시간종목조회순위()
        # res = api.tr_주식주문(s_구분='매수', s_종목코드='319400', n_주문수량=1, n_주문단가=6590, s_매매구분='보통')
        pass
    test()
