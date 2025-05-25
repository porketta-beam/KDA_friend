import streamlit as st
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from mplfinance.original_flavor import candlestick_ohlc
import matplotlib.dates as mdates
import matplotlib.font_manager as fm

# 한글 폰트 설정
plt.rcParams['font.family'] = 'Malgun Gothic'  # 윈도우 기본 한글 폰트
plt.rcParams['axes.unicode_minus'] = False  # 마이너스 기호 깨짐 방지

def render_main_content():
    
    nav1, nav2, nav3, nav4 = st.columns([2,2,1,1])
    with nav1:
        focused_corp = st.button("SK하이닉스 \n 000660")
    with nav2:
        favor_user = st.button("당신의 투자성향은?")
    with nav4:
        back_botton = st.button("뒤로가기")
        
    indicator1, indicator2, indicator3, indicator4, indicator5, indicator6  = st.columns(6)
    
    indicator1.button("RSI")
    indicator2.button("MACD")
    indicator3.button("BOLL")
    indicator4.button("KDJ")
    indicator5.button("CCI")
    indicator6.button("ADX")
    
    
    ##################################################################
    ################## 차트용 더미 데이터 / 삭제할 것 ##################
    ##################################################################
    
    # OHLCV 더미 데이터 생성
    dates = pd.date_range(start='2023-01-01', end='2023-12-31', freq='D')
    np.random.seed(42)
    
    # 시작가 100,000원 기준으로 랜덤 변동
    price = 100000 + np.random.randn(len(dates)).cumsum() * 1000
    
    # OHLCV 데이터 생성
    open_price = price
    high_price = price + np.random.rand(len(dates)) * 2000
    low_price = price - np.random.rand(len(dates)) * 2000
    close_price = price + np.random.randn(len(dates)) * 500
    volume = np.random.randint(100000, 1000000, size=len(dates))
    
    # 캔들차트용 데이터프레임 생성
    df = pd.DataFrame({
        'Open': open_price,
        'High': high_price,
        'Low': low_price,
        'Close': close_price,
        'Volume': volume
    }, index=dates)
    
    # Matplotlib 캔들차트 생성
    import matplotlib.pyplot as plt
    from mplfinance.original_flavor import candlestick_ohlc
    import matplotlib.dates as mdates
    
    # Figure 생성
    fig, ax = plt.subplots(figsize=(12, 3))
    
    # 데이터 준비
    ohlc = df[['Open', 'High', 'Low', 'Close']].copy()
    ohlc.index = mdates.date2num(ohlc.index.to_pydatetime())
    ohlc = ohlc.reset_index()
    
    # 캔들차트 그리기
    candlestick_ohlc(ax, ohlc.values[-30:], width=0.6, colorup='red', colordown='blue')
    
    # x축 날짜 포맷 설정
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    plt.xticks(rotation=45)
    
    # 그리드 추가
    ax.grid(True)
    ax.set_title('SK하이닉스 주가 차트 (최근 1개월)')
    
    # 캔들차트 figure 출력
    st.pyplot(fig)
    
    # RSI 계산
    delta = np.diff(price)
    gain = (delta > 0) * delta
    loss = (delta < 0) * -delta
    gain = np.pad(gain, (1,0), 'constant')
    loss = np.pad(loss, (1,0), 'constant')
    
    avg_gain = pd.Series(gain).rolling(window=14).mean()
    avg_loss = pd.Series(loss).rolling(window=14).mean()
    
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    
    # RSI 데이터프레임 생성
    rsi_data = pd.DataFrame({
        'RSI': rsi
    }, index=dates).dropna()
    
    # RSI 차트 생성
    fig2, ax2 = plt.subplots(figsize=(12, 2))  # 높이를 4에서 2로 변경
    ax2.plot(dates[-30:], rsi[-30:], color='purple')  # 최근 1개월만 표시
    ax2.axhline(y=70, color='r', linestyle='--', alpha=0.5)
    ax2.axhline(y=30, color='g', linestyle='--', alpha=0.5)
    ax2.set_title('RSI 지표 (최근 1개월)')
    ax2.grid(True)
    ax2.set_ylim(0, 100)
    
    # x축 날짜 범위 설정
    ax2.set_xlim(dates[-30], dates[-1])  # 최근 1개월
    
    # x축 날짜 포맷 설정
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    plt.xticks([])  # x축 tick 이름 삭제
    
    # RSI 차트 출력
    st.pyplot(fig2)
    
    ##################################################################
    ##################################################################
    ##################################################################
    
    # st.badge("Success", icon=":material/check:", color="green") :orange-badge[⚠️ Needs review]
    
    with st.expander(":orange-badge[⚠️ RSI에 대해서 궁금하신가요?]"):
        st.write('''
            RSI는 주가의 움직임을 측정하는 지표입니다.
            70% 이상이면 과매수, 30% 이하면 과매도 상태로 판단합니다.
            주가가 상승하면 매수 신호, 하락하면 매도 신호로 해석합니다.
        ''')
        st.image("https://post-image.valley.town/t_EaJysREE09ln8dlBzbc.png")

    ##################################################################
    #########################본문 더미 데이터 삭제 필요################    ##################################################################
    
    rep_abs = st.container(border=True)
    rep_logic = st.container(border=True)
    
    with rep_abs:
        st.markdown("""
                    # SOOP에겐 식은 SOUP 먹기
                    
                    동사의 2026E EPS 14,419원에 Target PER 12.90x를 적용한 182,500원을 목표 주가
                    로 제시한다. 10여년 간 이어져 온 트위치와의 경쟁구도는 트위치가 수익성 악화로
                    한국 시장에서 철수하며 동사의 승리로 막을 내렸다. 그 역할을 계승하겠다며 호기롭
                    게 도전장을 내민 치지직은 처음의 포부와는 동사와 본격적인 경쟁을 벌이기도 전에
                    힘들어하는 모습을 보이고 있다. 치지직이 트위치의 전철을 밟으며 사라지는 미래를
                    그려보며, 전례 없던 독점적인 지위를 누릴 동사를 알아보자.
                    
                    # Point 1. 전파 끊기는 소리, 치지직
                    
                    트위치가 사라진 현재, 한국 인터넷 방송 시장에서 동사의 유일한 경쟁자는 치지직이
                    다. 그러나 올해 첫 발을 내딛은 플랫폼인 치지직은 몇 걸음 떼지도 못한 채 뒷걸음
                    질 치고 있다. 다년간 탄탄하게 쌓아 올린 동사의 성벽과는 달리, 급하게 쌓아 올린
                    모래성인 치지직은 결국 밑단부터 흩어지며 트위치를 따라 사라질 것이다. 이미 그
                    전조증상이 시작었으며 치지직의 성장 가능성이 막혀 있다는 것과, 이는 구조적인 문
                    제이기에 극복이 불가능 하다는 것을 P와 Q 각각에서 뜯어보자.
                    
                    # Point 2. 드디어 최강이 된 SOOP
                    
                    일반적인 생각과 달리 국내 인터넷방송 산업의 주도권은 이미 완벽히 동사 쪽으로
                    기울었다. 치지직은 가지고 있던 고유의 해자를 모두 동사에 빼앗기며 게임, 버추얼,
                    커머스 모두에서 실패했다. 치지직 입장에서는 지금의 경쟁 패배가 더욱 고착화되고
                    역전할 수 없는 요인들이 산재해 있어 경쟁 패배가 더욱 뼈아프다. 결국 사업 존속의
                    목적을 찾지 못한 치지직은 크게 축소될 것이다. 역사상 처음으로 독점의 지위를 누
                    리게 될 동사는 크게 날아오를 준비를 마쳤다.
                    """)
    with rep_logic:
        st.markdown("""                   
                    ## 2.1. 한국의 인터넷 방송 생태계

                    인터넷 방송 플랫폼의 P와 Q를 간단히 하면 P는 ARPU(유저당 평균 매출), Q는 MAU(월간 활성
                    유저 수)를 의미한다. 즉, 이용자 수와 그 지출이 플랫폼의 수익성과 직결되는 것이다. 이 때 이
                    용자층 확보에 가장 핵심이 되는 것은 스트리머인데, 스트리머가 제공하는 효용이 이용자들이
                    플랫폼에 접속하고(Q) 구독이나 일회성 결제(P)를 하는 이유이기 때문이다. 트위치가 철수를 결
                    정하자 다른 플랫폼 업체들이 앞다투어 스트리머를 유치하고자 했던 이유가 여기에 있다.
                    
                    플랫폼과 스트리머 모두에게 BM의 수익성은 굉장히 중요하다. 플랫폼의 주 수입원은 크게 이용
                    자 지출액으로 구성된 플랫폼 매출과, 광고주로부터 오는 광고 수입으로 나뉜다. 유사한 플랫폼
                    사업모델이라도 매출 구성 비율에 따라 수익성에 큰 차이를 내는데, 이는 스트리머와 플랫폼이
                    사전에 정해진 비율에 따라 수입을 분배하기 때문에 곧 스트리머의 수익과도 직결된다. 따라서
                    수익성이 좋은 BM은 스트리머에게 어필할 수 있는 무기가 된다. [도표 2-1.]
                    
                    이용자 특성은 이러한 BM 설계 시 수익성을 위해 중요하게 고려해야 한다. 이에 대한 대표적인
                    예시로 트위치를 들 수 있는데, 트위치는 토크 및 게임에 특화된 문화를 조성하며 7000여명의
                    스트리머를 유치하는 데 성공했음에도 불구하고, 동사와 수익성 측면에서 상당한 차이를 보였다.
                    이에 반해 동사는 일회성 결제를 유도하는 BM을 설계하여 탑라인의 성장을 이뤄냈다. 한국의
                    플랫폼 이용자들은 일회성 결제에 대한 과금전환율이 높다. 이는 웹툰 산업의 사례에서 명확하
                    게 드러나는데, 한국의 경우 일회성 결제의 PUR(과금전환율)이 타국에 비해 압도적이다. 그러나
                    반대로 구독 경제의 경우, 23년 기준 미국의 OTT 플랫폼의 구독율은 86%로, 한국 40%의 2배
                    가 넘는 모습을 보여 미국의 경우 일회성 결제율은 낮지만, 유료 구독률은 높다는 환경적 차이
                    를 확인할 수 있다. [도표 2-2.]
                    
                    ## 2.2. 역사가 증명한 식은 SOOP 먹기
                    인터넷 방송 산업은 아프리카TV와 트위치가 양분하는 구도가 10여년 간 지속되어 왔다. 이러한
                    구도는 트위치가 24년 수익성 악화로 인한 철수를 선언하며 아프리카TV의 승리로 끝났으나, 치
                    지직이라는 새로운 경쟁자가 트위치를 계승하며 새로운 경쟁 패러다임을 열었다.
                    아프리카TV는 게임 방송을 메인으로 인터넷 방송 산업에 뛰어들어 방송국에서 잘 다루지 않는
                    마이너 게임들의 라이브 방송을 바탕으로 성장했다. 07년 별풍선이라는 비즈니스 모델을 고안하
                    여 자신이 응원하는 스트리머에게 직접 기부할 수 있는 혁신적인 시스템을 도입하였다. 수익성
                    좋은 비즈니스 모델을 통해 다수의 스트리머를 영업하며 크기를 키운 동사는 콘텐츠와 트래픽에
                    서 우위를 선점한 이후 10년대 중반까지 다음 tv팟, KOO TV 등의 경쟁자들을 압도하며 독과점
                    의 지위를 유지해왔다. [도표 2-3.]
                    
                    동사의 오랜 경쟁자인 트위치가 본격적으로 한국에 들어온 것은 15년 9월이다. 한국 서버가 추
                    가될 것이라는 소식은 2월에 발표하였으나, 9월부터 공격적으로 스트리머들과 전속 계약을 맺으
                    며 시장에 침투했다. 트위치는 스트리머 수입의 불안정성을 해결할 수 있도록 월급 개념의 확실
                    한 페이를 약속하는 등 파격적인 대우를 통해 다수의 스트리머들을 자신의 플랫폼으로 끌어들였
                    다. 특히 높은 화질에 목말랐던 게임 스트리머들이 화질을 보장해주는 트위치라는 대안으로 이
                    적하며, 본격적으로 트위치와 동사의 대결구도가 시작되었다.
                    
                    10년 간 이어져오던 경쟁구도는 트위치가 수익성 악화로 24년 2월 한국에서 철수하며 막을 내
                    렸다. 이는 전술했듯이 트위치 BM의 구조적 문제 때문이다. 미국에 뿌리를 둔 트위치는 구독이
                    나 광고 외 자체적인 수입원이 없었다. 뒤늦게 서드파티를 이용하여 기부경제 서비스를 제공하
                    였으나, 제3자에게 지불하는 수수료 때문에 트위치에게 돌아가는 이익은 6~15%로 매우 낮았다.
                    이에 트위치 역시 비트라는 자체 시스템을 도입했으나, 영상 후원이 안 되는 등 기존 서비스 대
                    신 비트를 사용할 유인이 없어 결국 기부경제로 수익률을 올리는 데 실패했다.
                    
                    트위치가 떠난 지금, 인터넷 방송 시장은 동사와 치지직이 양분하는 새로운 경쟁 구도를 맞이했
                    다. 신규 플레이어인 치지직은 오픈 베타 테스트 일정을 앞당기는 것 외에도 트위치의 영상 후
                    원 기능, UI 등 유사한 환경을 조성하며 트위치 스트리머들을 흡수하기 위해 총력을 기울였다.
                    비록 몇몇 대형 스트리머들을 동사에게 빼앗기긴 했으나, 이러한 노력 끝에 치지직은 시장의 플
                    레이어로서 성공적으로 자리 잡았다. 그러나 최근 치지직에서의 파트너 BJ가 이탈하는 등 불안
                    정한 모습과, 모든 지표에서 동사가 앞선다는 점에서 트위치와의 경쟁과는 사뭇 다른 양상을 띨
                    것으로 예상된다. [도표 2-4.]
                    """)
