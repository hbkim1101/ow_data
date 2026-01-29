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
MAX_WORKERS = 5  # 동시 요청 수
TIMEOUT_SEC = 30 

def scrape_single_url(args):
    region, input_gamemode, map_name, tier, date_str = args
    
    # [수정됨] 폴백 로직 설정
    # 요청 들어온 모드가 2(경쟁전)이면 [2, 1] 순서로 시도
    # 그 외(빠른대전 0 등)는 원래 값만 시도
    modes_to_try = [2, 1] if input_gamemode == 2 else [input_gamemode]

    # 설정된 모드 후보들을 순차적으로 시도
    for current_gamemode in modes_to_try:
        records = []
        
        # URL 생성
        base_url = "https://overwatch.blizzard.com/ko-kr/rates/"
        # rq 파라미터에 현재 시도 중인 current_gamemode 사용
        params = f"?input=pc&map={map_name}&region={region}&role=All&rq={current_gamemode}&tier={tier}"
        target_url = base_url + params

        max_retries = 3
        for attempt in range(max_retries):
            try:
                res = requests.get(target_url, timeout=TIMEOUT_SEC)
                res.raise_for_status()

                soup = BeautifulSoup(res.text, "html.parser")

                # ================================================================
                # 🛡️ HTML 태그(Select Option) 3중 검증
                # ================================================================

                # [1] 게임 모드 검증 (현재 시도 중인 모드와 일치하는지 확인)
                selected_gamemode = soup.find("option", {"value": str(current_gamemode), "selected": True})
                if not selected_gamemode:
                    # 검증 실패 시, 다음 시도(retries)가 아니라 다음 모드(modes_to_try)로 넘어가야 하므로
                    # 여기서는 빈 리스트 반환하고 loop break 유도
                    break 

                # [2] 맵 검증
                if map_name != "all-maps":
                    selected_map = soup.find("option", {"value": map_name, "selected": True})
                    if not selected_map:
                        break

                # [3] 티어 검증
                if tier != "All":
                    selected_tier = soup.find("option", {"value": tier, "selected": True})
                    if not selected_tier:
                        break
                
                # ================================================================

                # 데이터 추출
                tag = soup.find("blz-data-table")
                if not tag:
                    break

                raw_json = html.unescape(tag["allrows"])
                data = json.loads(raw_json)

                if not data: 
                    break

                for hero in data:
                    cells = hero.get("cells", {})
                    hero_meta = hero.get("hero", {})
                    records.append({
                        "date": date_str,
                        # [수정됨] 1, 2 모두 "competitive"로 기록, 0은 "quickplay"
                        "game_mode": "competitive" if current_gamemode in [1, 2] else "quickplay",
                        "region": region,
                        "map": map_name,
                        "tier": tier,
                        "hero": cells.get("name", ""),
                        "role": hero_meta.get("role", ""),
                        "pick_rate": cells.get("pickrate", ""),
                        "win_rate": cells.get("winrate", "")
                    })
                
                time.sleep(0.1)
                
                # 데이터를 성공적으로 찾았으면 즉시 반환 (더 이상 다른 모드/재시도 불필요)
                return records

            except Exception as e:
                if attempt < max_retries - 1:
                    time.sleep(1)
                else:
                    pass # 마지막 시도 실패 시 다음 로직으로 이동

        # 만약 records가 채워졌다면 루프 종료 및 반환 (위의 return records가 처리함)
        # 여기까지 왔다는 건, 현재 current_gamemode로는 실패했다는 뜻
        # 다음 modes_to_try로 넘어감 (예: 2 실패 -> 1 시도)

    return []

def main():
    # ===== 0. 기본 설정 =====
    date_str = datetime.now().strftime("%Y-%m-%d")
    season_dir = "Season20" # 필요시 수정
    season_num = "".join(ch for ch in season_dir if ch.isdigit())
    season_code = f"S{season_num}"
    date_short = datetime.strptime(date_str, "%Y-%m-%d").strftime("%y%m%d")

    save_root = os.path.join(season_dir, date_str)
    os.makedirs(save_root, exist_ok=True)

    print(f"=== Saving data under: {save_root} ===")
    print(f"=== Workers: {MAX_WORKERS} threads ===")

    # ===== 1. 수집 대상 설정 (순서 정의) =====
    # 이 리스트 순서대로 최종 파일이 정렬됩니다.
    gamemodes = [0, 2] # 0:quickplay, 2:competitive (실패시 1로 자동 폴백)
    regions = ["Americas", "Europe", "Asia"]
    
    maps = [
        "all-maps", "throne-of-anubis", "hanaoka", "antarctic-peninsula", "nepal", "lijiang-tower", 
        "busan", "samoa", "oasis", "ilios", "route-66", "watchpoint-gibraltar", "dorado", 
        "rialto", "shambali-monastery", "circuit-royal", "junkertown", "havana", "new-junk-city", 
        "suravasa", "aatlis", "numbani", "midtown", "blizzard-world", "eichenwalde", 
        "kings-row", "paraiso", "hollywood", "new-queen-street", "runasapi", "esperanca", "colosseo"
    ]
    
    tiers = ["All", "Bronze", "Silver", "Gold", "Platinum", "Diamond", "Master", "Grandmaster"]

    total_rows = 0

    # 정렬을 위한 텍스트 변환 맵핑
    # [수정됨] 2가 들어와도 결과값은 competitive이므로 2->competitive 매핑 유지
    mode_map_str = {0: "quickplay", 2: "competitive"} 
    ordered_modes = [mode_map_str[g] for g in gamemodes] 

    # ===== 2. 지역별 수집 =====
    for region in regions:
        print(f"\n===== 🌎 {region} 수집 시작 (Parallel) =====")
        
        tasks = []
        for gamemode, map_name, tier in product(gamemodes, maps, tiers):
            # [조건부 건너뛰기]
            # 빠대(0)인데 티어가 전체가 아니면 스킵
            if gamemode == 0 and tier != "All": continue
            
            # 경쟁전(1,2)에서 특정 맵 스킵 (사용자 원본 로직 유지)
            # 여기서는 2로 진입하므로 2일 때 검사
            if gamemode == 2 and map_name in ["throne-of-anubis", "hanaoka"]: continue
            
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
                    print(f"    ... {i + 1}/{len(tasks)} 완료")

        # ===== 3. 저장 및 정렬 (Sorting) =====
        if region_records:
            df_region = pd.DataFrame(region_records)
            
            # ---------------------------------------------------------
            # 🧹 [정렬 로직]
            # ---------------------------------------------------------
            
            df_region['game_mode'] = pd.Categorical(
                df_region['game_mode'], categories=ordered_modes, ordered=True
            )
            df_region['map'] = pd.Categorical(
                df_region['map'], categories=maps, ordered=True
            )
            df_region['tier'] = pd.Categorical(
                df_region['tier'], categories=tiers, ordered=True
            )

            df_region = df_region.sort_values(by=['game_mode', 'map', 'tier'])
            
            # ---------------------------------------------------------

            total_rows += len(df_region)

            filename = f"{season_code}_{region}_{date_short}.csv"
            filepath = os.path.join(save_root, filename)
            df_region.to_csv(filepath, index=False, encoding="utf-8-sig")
            print(f"💾 {region} 저장 완료 (정렬됨): {len(df_region)} rows")
        else:
            print(f"⚠️ {region} 데이터 없음")

    print(f"\n🎉 전체 완료! 총 데이터 행 수: {total_rows}")

    if "GITHUB_ENV" in os.environ:
        with open(os.environ["GITHUB_ENV"], "a") as f:
            f.write(f"TOTAL_ROWS={total_rows}\n")

if __name__ == "__main__":
    main()
