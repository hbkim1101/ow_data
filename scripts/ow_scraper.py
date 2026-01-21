# script/ow_scraper.py
import requests
import pandas as pd
import time
import html
import json
import os
from bs4 import BeautifulSoup
from itertools import product
from datetime import datetime

def main():
    # ===== 0. 날짜 기반 상위 폴더 설정 =====
    # 오늘 날짜로 자동 설정 (YYYY-MM-DD)
    date_str = datetime.now().strftime("%Y-%m-%d")

    # 최상위 시즌 폴더
    season_dir = "Season19"

    # Season19 → S19 같은 코드로 변환
    season_num = "".join(ch for ch in season_dir if ch.isdigit())  # "19"
    season_code = f"S{season_num}"  # "S19"

    # 2025-12-05 → 251205 형식으로 변환
    date_short = datetime.strptime(date_str, "%Y-%m-%d").strftime("%y%m%d")

    # Season19/2025-12-05 이런 식으로 날짜별 폴더 생성
    save_root = os.path.join(season_dir, date_str)
    os.makedirs(save_root, exist_ok=True)

    print(f"=== Saving data under: {save_root} ===")
    print(f"=== File name pattern: {season_code}_<Region>_{date_short}.csv ===")

    # ===== 1. 수집 대상 설정 =====
    gamemodes = [0, 1]  # 0: 빠른 대전, 1: 경쟁전
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

    # ===== 2. 지역별 수집 루프 =====
    for region in regions:
        print(f"\n===== 🌎 {region} 수집 시작 =====")
        records = []  # 지역별로 초기화

        for gamemode, map_name, tier in product(gamemodes, maps, tiers):

            # 빠른 대전은 tier=All만 존재
            if gamemode == 0 and tier != "All":
                continue
            # 경쟁전인데 폐지된 맵은 스킵
            elif gamemode == 1 and map_name in ["throne-of-anubis", "hanaoka"]:
                continue

            url = (
                "https://overwatch.blizzard.com/ko-kr/rates/"
                f"?input=pc&map={map_name}&region={region}"
                f"&role=All&rq={gamemode}&tier={tier}"
            )
            print(f"🌍 수집 중: region={region}, map={map_name}, tier={tier} - {url}")

            try:
                res = requests.get(url, timeout=15)
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

                # 너무 빠르게 때리지 않도록
                time.sleep(0.1)

            except Exception as e:
                print(f"❌ 실패: region={region}, map={map_name}, tier={tier} | {e}")
                continue

        # ===== 3. 지역별 DataFrame & CSV 저장 =====
        if records:
            df_region = pd.DataFrame(records)

            # 👉 여기서 파일명 형식을 S19_Asia_251205 이런 식으로 맞춤
            filename = f"{season_code}_{region}_{date_short}.csv"
            filepath = os.path.join(save_root, filename)

            df_region.to_csv(filepath, index=False, encoding="utf-8-sig")
        else:
            print(f"⚠️ {region} 지역에 수집된 데이터가 없습니다.")

    print("🎉 모든 지역 데이터 수집 및 저장 완료!")

if __name__ == "__main__":
    main()
