#%%
import requests
import pandas as pd
import time
import html
import json
import os
from bs4 import BeautifulSoup
from itertools import product
from datetime import datetime

# 시즌 폴더 (고정)
base_dir = "Season19"

# 오늘 날짜 문자열
today_str = datetime.now().strftime("%Y-%m-%d")

# 오늘 날짜 기준 저장 폴더: Season19/2025-12-05
save_dir = os.path.join(base_dir, today_str)
os.makedirs(save_dir, exist_ok=True)

# 수집 대상
gamemodes = [0, 1]  # 0: 빠른 대전, 1: 경쟁전
regions = ["Americas", "Europe", "Asia"]
maps = [
    "all-maps", 
    "throne-of-anubis", "hanaoka",
    "antarctic-peninsula", "nepal", "lijiang-tower", "busan", "samoa", "oasis", "ilios",
    "route-66", "watchpoint-gibraltar", "dorado", "rialto", "shambali-monastery", "circuit-royal", "junkertown", "havana",
    "new-junk-city", "suravasa", "aatlis",
    "numbani", "midtown", "blizzard-world", "eichenwalde", "kings-row", "paraiso", "hollywood",
    "new-queen-street", "runasapi", "esperanca", "colosseo"
]
tiers = ["All", "Bronze", "Silver", "Gold", "Platinum", "Diamond", "Master", "Grandmaster"]

for region in regions:
    print(f"\n===== 🌎 {region} 수집 시작 =====")
    records = []  # 지역별로 초기화

    for gamemode, map_name, tier in product(gamemodes, maps, tiers):

        if gamemode == 0 and tier != "All":
            continue
        elif gamemode == 1 and map_name in ["throne-of-anubis", "hanaoka"]:
            continue

        url = (
            "https://overwatch.blizzard.com/ko-kr/rates/"
            f"?input=pc&map={map_name}&region={region}"
            f"&role=All&rq={gamemode}&tier={tier}"
        )
        print(f"🌍 수집 중: region={region}, map={map_name}, tier={tier} - {url}")

        try:
            res = requests.get(url)
            res.raise_for_status()
            soup = BeautifulSoup(res.text, "html.parser")

            tag = soup.find("blz-data-table")
            if not tag:
                print(f"⚠️ 데이터 없음: region={region}, map={map_name}, tier={tier}")
                continue

            raw_json = html.unescape(tag["allrows"])
            data = json.loads(raw_json)

            for hero in data:
                cells = hero.get("cells", {})
                hero_meta = hero.get("hero", {})
                records.append({
                    "game_mode": "competitive" if gamemode == 1 else "quickplay",
                    "region": region,
                    "map": map_name,
                    "tier": tier,
                    "hero_name": cells.get("name", ""),
                    "role": hero_meta.get("role", ""),
                    "pick_rate(%)": cells.get("pickrate", ""),
                    "win_rate(%)": cells.get("winrate", "")
                })

            time.sleep(1)

        except Exception as e:
            print(f"❌ 실패: region={region}, map={map_name}, tier={tier} | {e}")
            continue

    # 지역별 DataFrame & CSV 저장
    df_region = pd.DataFrame(records)

    filename = f"overwatch_all_stats_{region.lower()}.csv"
    filepath = os.path.join(save_dir, filename)

    df_region.to_csv(filepath, index=False, encoding="utf-8-sig")
    print(f"✅ {region} 데이터 CSV 저장 완료: {filepath}")

print("🎉 모든 지역 데이터 수집 및 저장 완료!")
