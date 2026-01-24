import requests
import pandas as pd
import time
import html
import json
import os
from bs4 import BeautifulSoup
from itertools import product
from datetime import datetime
from urllib.parse import unquote
from concurrent.futures import ThreadPoolExecutor, as_completed

# ===== 설정값 =====
MAX_WORKERS = 5  # 동시 요청 수 (5~8 권장)
TIMEOUT_SEC = 30 # 타임아웃

def scrape_single_url(args):
    """
    [작업 단위] URL 요청 및 정밀 검증(Validation) 수행
    """
    region, gamemode, map_name, tier, date_str = args
    
    records = []
    
    # 1. 요청 URL 조립
    base_url = "https://overwatch.blizzard.com/ko-kr/rates/"
    # rq: 0(빠른대전), 2(경쟁전)
    params = f"?input=pc&map={map_name}&region={region}&role=All&rq={gamemode}&tier={tier}"
    target_url = base_url + params

    max_retries = 3
    for attempt in range(max_retries):
        try:
            # 2. 요청 전송 (allow_redirects=True로 설정하여 최종 도착지 확인)
            res = requests.get(target_url, timeout=TIMEOUT_SEC, allow_redirects=True)
            res.raise_for_status()

            # ===== 🛡️ [핵심] URL 대조 검증 (Validation) =====
            # 브라우저/서버 간 인코딩 차이 해결을 위해 디코딩
            final_url_decoded = unquote(res.url)
            
            # (1) 게임 모드(rq) 검증
            # 내가 요청한 모드(rq=2)가 사라지고 rq=0 등으로 바뀌었는지 확인
            if f"rq={gamemode}" not in final_url_decoded:
                # print(f"⏩ [SKIP] GameMode Mismatch: {map_name}/{tier}")
                return []

            # (2) 맵 이름 검증
            if map_name not in final_url_decoded:
                # print(f"⏩ [SKIP] Map Mismatch: {map_name} -> Removed in URL")
                return []

            # (3) 티어 검증
            if tier not in final_url_decoded:
                 # print(f"⏩ [SKIP] Tier Mismatch: {tier} -> Removed in URL")
                 return []
            
            # ===================================================

            # 3. 데이터 파싱
            soup = BeautifulSoup(res.text, "html.parser")
            tag = soup.find("blz-data-table")
            
            # 데이터 테이블이 아예 없는 경우
            if not tag:
                return []

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
            
            # 성공 시 약간의 딜레이 (서버 부하 방지)
            time.sleep(0.1) 
            return records

        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(1) # 재시도 전 대기
            else:
                # 실패 로그 (필요 시 주석 해제)
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
        
        # 작업 목록(Task List) 생성
        tasks = []
        for gamemode, map_name, tier in product(gamemodes, maps, tiers):
            # 1차 필터링 (불필요한 조합 제외)
            if gamemode == 0 and tier != "All": continue
            elif gamemode == 1 and map_name in ["throne-of-anubis", "hanaoka"]: continue
            
            tasks.append((region, gamemode, map_name, tier, date_str))

        region_records = []
        
        # ThreadPoolExecutor로 병렬 실행
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_url = {executor.submit(scrape_single_url, t): t for t in tasks}
            
            for i, future in enumerate(as_completed(future_to_url)):
                try:
                    data = future.result()
                    if data:
                        region_records.extend(data)
                except Exception as exc:
                    print(f"Error in worker: {exc}")
                
                # 진행 상황 로깅 (50개 단위)
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
