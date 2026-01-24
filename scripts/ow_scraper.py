import requests
import pandas as pd
import time
import html
import json
import os
from bs4 import BeautifulSoup
from itertools import product
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# ===== 설정값 =====
MAX_WORKERS = 5  # 5~8 권장
TIMEOUT_SEC = 30 

def scrape_single_url(args):
    region, gamemode, map_name, tier, date_str = args
    records = []
    
    # URL 생성
    base_url = "https://overwatch.blizzard.com/ko-kr/rates/"
    params = f"?input=pc&map={map_name}&region={region}&role=All&rq={gamemode}&tier={tier}"
    target_url = base_url + params

    max_retries = 3
    for attempt in range(max_retries):
        try:
            # allow_redirects=True로 둡니다 (URL 확인보다는 내용 확인이 중요하므로)
            res = requests.get(target_url, timeout=TIMEOUT_SEC)
            res.raise_for_status()

            # HTML 파싱
            soup = BeautifulSoup(res.text, "html.parser")

            # ================================================================
            # 🛡️ [진짜 핵심] HTML 내부의 <option> 태그 검증
            # 서버가 200 OK를 주더라도, 실제로 선택된 옵션이 다른지 확인합니다.
            # ================================================================
            
            # (1) 맵 검증
            if map_name != "all-maps":
                # 내가 요청한 맵 이름(value)을 가진 option 태그를 찾고, 
                # 그 태그에 'selected' 속성이 있는지 확인
                # 예: <option value="hanaoka" selected> 가 있어야 통과
                selected_map_option = soup.find("option", {"value": map_name, "selected": True})
                
                if not selected_map_option:
                    # print(f"⏩ [SKIP] Map Mismatch: 요청({map_name}) != 결과(HTML내 선택안됨)")
                    return []

            # (2) 티어 검증
            if tier != "All":
                # 티어 역시 HTML 내에서 선택되어 있는지 확인
                selected_tier_option = soup.find("option", {"value": tier, "selected": True})
                
                if not selected_tier_option:
                    # print(f"⏩ [SKIP] Tier Mismatch: 요청({tier}) != 결과(HTML내 선택안됨)")
                    return []
            
            # ================================================================

            # 데이터 추출
            tag = soup.find("blz-data-table")
            if not tag:
                return []

            raw_json = html.unescape(tag["allrows"])
            data = json.loads(raw_json)

            if not data: # 데이터가 비어있으면 종료
                return []

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
            
            time.sleep(0.1) 
            return records

        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(1)
            else:
                # print(f"❌ [FAIL] {map_name}/{tier}: {e}")
                return [] 

    return []

def main():
    # ===== 0. 기본 설정 =====
    date_str = datetime.now().strftime("%Y-%m-%d")
    season_dir = "Season20"
    season_num = "".join(ch for ch in season_dir if ch.isdigit())
    season_code = f"S{season_num}"
    date_short = datetime.strptime(date_str, "%Y-%m-%d").strftime("%y%m%d")

    save_root = os.path.join(season_dir, date_str)
    os.makedirs(save_root, exist_ok=True)

    print(f"=== Saving data under: {save_root} ===")
    print(f"=== Workers: {MAX_WORKERS} threads ===")

    # ===== 1. 수집 대상 설정 =====
    gamemodes = [0, 1]
    regions = ["Asia"]
    maps = [
        "all-maps", "throne-of-anubis", "hanaoka", "antarctic-peninsula", "nepal", "lijiang-tower", 
        "busan", "samoa", "oasis", "ilios", "route-66", "watchpoint-gibraltar", "dorado", 
        "rialto", "shambali-monastery", "circuit-royal", "junkertown", "havana", "new-junk-city", 
        "suravasa", "aatlis", "numbani", "midtown", "blizzard-world", "eichenwalde", 
        "kings-row", "paraiso", "hollywood", "new-queen-street", "runasapi", "esperanca", "colosseo"
    ]
    tiers = ["All", "Bronze", "Silver", "Gold", "Platinum", "Diamond", "Master", "Grandmaster"]

    total_rows = 0

    # ===== 2. 지역별 수집 =====
    for region in regions:
        print(f"\n===== 🌎 {region} 수집 시작 (Parallel) =====")
        
        tasks = []
        for gamemode, map_name, tier in product(gamemodes, maps, tiers):
            if gamemode == 0 and tier != "All": continue
            elif gamemode == 1 and map_name in ["throne-of-anubis", "hanaoka"]: continue
            
            tasks.append((region, gamemode, map_name, tier, date_str))

        region_records = []
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_url = {executor.submit(scrape_single_url, t): t for t in tasks}
            
            for i, future in enumerate(as_completed(future_to_url)):
                try:
                    data = future.result()
                    if data:
                        region_records.extend(data)
                except Exception as exc:
                    print(f"Error: {exc}")
                
                if (i + 1) % 50 == 0:
                    print(f"   ... {i + 1}/{len(tasks)} 완료")

        # ===== 3. 저장 =====
        if region_records:
            df_region = pd.DataFrame(region_records)
            total_rows += len(df_region)

            filename = f"{season_code}_{region}_{date_short}.csv"
            filepath = os.path.join(save_root, filename)
            df_region.to_csv(filepath, index=False, encoding="utf-8-sig")
            print(f"💾 {region} 저장 완료: {len(df_region)} rows")
        else:
            print(f"⚠️ {region} 데이터 없음")

    print(f"\n🎉 전체 완료! 총 데이터 행 수: {total_rows}")

    if "GITHUB_ENV" in os.environ:
        with open(os.environ["GITHUB_ENV"], "a") as f:
            f.write(f"TOTAL_ROWS={total_rows}\n")

if __name__ == "__main__":
    main()
