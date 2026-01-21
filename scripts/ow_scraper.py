import requests
import pandas as pd
import time
import html
import json
import os
import random  # 랜덤 시간 생성을 위해 추가
from bs4 import BeautifulSoup
from itertools import product
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def get_session():
    """
    연결이 끊기거나 서버가 바쁠 때 자동으로 재시도하는 세션을 만듭니다.
    """
    session = requests.Session()
    retry = Retry(
        total=3,              # 최대 3번까지 재시도
        backoff_factor=2,     # 재시도 간격 (2초, 4초, 8초... 늘어남)
        status_forcelist=[429, 500, 502, 503, 504], # 이 에러들은 재시도 함
        allowed_methods=["HEAD", "GET", "OPTIONS"]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    
    # 봇 차단 방지 헤더 설정
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    })
    return session

def main():
    date_str = datetime.now().strftime("%Y-%m-%d")
    season_dir = "Season19"
    season_num = "".join(ch for ch in season_dir if ch.isdigit())
    season_code = f"S{season_num}"
    date_short = datetime.strptime(date_str, "%Y-%m-%d").strftime("%y%m%d")
    
    save_root = os.path.join(season_dir, date_str)
    os.makedirs(save_root, exist_ok=True)

    print(f"=== Saving data under: {save_root} ===")

    gamemodes = [0, 1]
    regions = ["Americas", "Europe", "Asia"]
    maps = [
        "all-maps", 
        "throne-of-anubis", "hanaoka", "antarctic-peninsula", "nepal", "lijiang-tower", 
        "busan", "samoa", "oasis", "ilios", "route-66", "watchpoint-gibraltar", 
        "dorado", "rialto", "shambali-monastery", "circuit-royal", "junkertown", 
        "havana", "new-junk-city", "suravasa", "aatlis", "numbani", "midtown", 
        "blizzard-world", "eichenwalde", "kings-row", "paraiso", "hollywood", 
        "new-queen-street", "runasapi", "esperanca", "colosseo"
    ]
    tiers = ["All", "Bronze", "Silver", "Gold", "Platinum", "Diamond", "Master", "Grandmaster"]

    # ★ 세션 생성 (여기서 한 번만 만듦)
    session = get_session()

    for region in regions:
        print(f"\n===== 🌎 {region} 수집 시작 =====")
        records = []

        for gamemode, map_name, tier in product(gamemodes, maps, tiers):
            if gamemode == 0 and tier != "All": continue
            elif gamemode == 1 and map_name in ["throne-of-anubis", "hanaoka"]: continue

            url = (
                "https://overwatch.blizzard.com/ko-kr/rates/"
                f"?input=pc&map={map_name}&region={region}"
                f"&role=All&rq={gamemode}&tier={tier}"
            )
            
            # 진행 상황 로깅 간소화 (너무 많이 찍히면 정신없음)
            # print(f"Processing: {region} | {map_name} | {tier} ...") 

            try:
                # ★ session.get 사용 (재시도 로직 포함됨)
                res = session.get(url, timeout=20)
                res.raise_for_status() # 404 등 에러 체크

                soup = BeautifulSoup(res.text, "html.parser")
                tag = soup.find("blz-data-table")
                
                if not tag or not tag.get("allrows"):
                    # 데이터가 없는 경우 (정상적인 상황일 수도 있음)
                    # print(f"   -> 데이터 없음 (Skip)")
                    continue

                raw_json = html.unescape(tag["allrows"])
                data = json.loads(raw_json)

                for hero in data:
                    cells = hero.get("cells", {})
                    hero_meta = hero.get("hero", {})
                    records.append({
                        "date": date_str,
                        "game_mode": "competitive" if gamemode == 1 else "quickplay",
                        "region": region,
                        "map": map_name,
                        "tier": tier,
                        "hero_name": cells.get("name", ""),
                        "role": hero_meta.get("role", ""),
                        "pick_rate(%)": cells.get("pickrate", ""),
                        "win_rate(%)": cells.get("winrate", "")
                    })

                # ★ 핵심: 0.3초 고정이 아니라, 1.0 ~ 2.0초 사이 랜덤 대기
                # 서버가 "숨 쉴 틈"을 줍니다.
                sleep_time = random.uniform(1.0, 2.0)
                time.sleep(sleep_time)

            except Exception as e:
                print(f"❌ ERROR: {region}-{map_name}-{tier} | {e}")
                # 에러 났을 때는 조금 더 길게 쉬어줌 (5초)
                time.sleep(5)
                continue

        if records:
            df_region = pd.DataFrame(records)
            filename = f"{season_code}_{region}_{date_short}.csv"
            filepath = os.path.join(save_root, filename)
            df_region.to_csv(filepath, index=False, encoding="utf-8-sig")
            print(f"✅ {region} 저장 완료 ({len(records)}행): {filepath}")
        else:
            print(f"⚠️ {region} 데이터 없음")

    print("🎉 수집 종료!")

if __name__ == "__main__":
    main()
