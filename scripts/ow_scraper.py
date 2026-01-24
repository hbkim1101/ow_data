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
MAX_WORKERS = 5  # 동시에 보낼 요청 수 (너무 높으면 429 에러/차단 위험. 5~8 추천)
TIMEOUT_SEC = 30 # 타임아웃 시간

def scrape_single_url(args):
    """
    하나의 URL을 처리하는 작업 단위 함수
    """
    region, gamemode, map_name, tier, date_str = args
    
    records = []
    
    # URL 생성
    url = (
        "https://overwatch.blizzard.com/ko-kr/rates/"
        f"?input=pc&map={map_name}&region={region}"
        f"&role=All&rq={gamemode}&tier={tier}"
    )

    # 재시도 로직
    max_retries = 3
    for attempt in range(max_retries):
        try:
            res = requests.get(url, timeout=TIMEOUT_SEC)

            # 리다이렉트 감지 (데이터 없음)
            if res.history:
                # print(f"⏩ [SKIP] {map_name}/{tier} (Redirected)")
                return [] # 빈 리스트 반환

            res.raise_for_status()
            soup = BeautifulSoup(res.text, "html.parser")

            tag = soup.find("blz-data-table")
            if not tag:
                # print(f"⚠️ [NO DATA] {map_name}/{tier}")
                return []

            raw_json = html.unescape(tag["allrows"])
            data = json.loads(raw_json)

            for hero in data:
                cells = hero.get("cells", {})
                hero_meta = hero.get("hero", {})
                records.append({
                    "date": date_str,
                    "game_mode": "competitive" if gamemode == 2 else "quickplay",
                    "region": region,
                    "map": map_name,
                    "tier": tier,
                    "hero_name": cells.get("name", ""),
                    "role": hero_meta.get("role", ""),
                    "pick_rate(%)": cells.get("pickrate", ""),
                    "win_rate(%)": cells.get("winrate", "")
                })
            
            # 성공 시 약간의 딜레이 후 리턴
            time.sleep(0.1) 
            return records

        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(1) # 재시도 전 대기
            else:
                print(f"❌ [FAIL] {map_name}/{tier}: {e}")
                return [] # 실패 시 빈 리스트

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

    # ===== 2. 지역별 수집 (지역 단위는 순차 처리하되, 내부는 병렬 처리) =====
    for region in regions:
        print(f"\n===== 🌎 {region} 수집 시작 (Parallel) =====")
        
        # 작업 목록(Task List) 생성
        tasks = []
        for gamemode, map_name, tier in product(gamemodes, maps, tiers):
            if gamemode == 0 and tier != "All": continue
            elif gamemode == 1 and map_name in ["throne-of-anubis", "hanaoka"]: continue
            
            # 작업 인자 묶기
            tasks.append((region, gamemode, map_name, tier, date_str))

        region_records = []
        
        # ThreadPoolExecutor로 병렬 실행
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # tasks 리스트를 병렬로 던짐
            future_to_url = {executor.submit(scrape_single_url, t): t for t in tasks}
            
            # 완료되는 순서대로 결과 수집
            for i, future in enumerate(as_completed(future_to_url)):
                try:
                    data = future.result()
                    if data:
                        region_records.extend(data)
                except Exception as exc:
                    print(f"Generated an exception: {exc}")
                
                # 진행 상황 간략 표시 (50개 단위로 점 찍기)
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

    # GitHub Actions 환경 변수 내보내기
    if "GITHUB_ENV" in os.environ:
        with open(os.environ["GITHUB_ENV"], "a") as f:
            f.write(f"TOTAL_ROWS={total_rows}\n")

if __name__ == "__main__":
    main()
