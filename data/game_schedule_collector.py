#!/usr/bin/env python3
"""
경기 일정 데이터 수집기
3월부터 9월까지 네이버 스포츠 API에서 경기 데이터를 수집하여 DB에 저장
"""

import os
import sys
import requests
import json
from datetime import datetime, timedelta
from typing import List, Dict, Any
from dotenv import load_dotenv

# 환경변수 로드
load_dotenv()

# 현재 디렉토리를 Python 경로에 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.supabase_client import SupabaseManager

class GameScheduleCollector:
    def __init__(self):
        """경기 일정 수집기 초기화"""
        self.supabase = None
        self.api_base_url = "https://api-gw.sports.naver.com/schedule/games"
        
        # Supabase 연결
        try:
            self.supabase = SupabaseManager()
            print("✅ Supabase 연결 성공")
        except Exception as e:
            print(f"❌ Supabase 연결 실패: {e}")
            raise e
    
    def fetch_games_for_date(self, date: str) -> List[Dict[str, Any]]:
        """특정 날짜의 경기 데이터 조회"""
        try:
            params = {
                "fields": "basic,schedule,baseball",
                "upperCategoryId": "kbaseball",
                "categoryId": "kbo",
                "fromDate": date,
                "toDate": date,
                "roundCodes": "",
                "size": 500
            }
            
            print(f"🏟️ {date} 경기 일정 API 호출 중...")
            
            response = requests.get(self.api_base_url, params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                if data.get("success") and data.get("result", {}).get("games"):
                    games = data["result"]["games"]
                    print(f"✅ {date} 경기 {len(games)}개 조회 성공")
                    return games
                else:
                    print(f"⚠️ {date} 경기 데이터 없음")
                    return []
            else:
                print(f"❌ {date} API 호출 실패: {response.status_code}")
                return []
                
        except Exception as e:
            print(f"❌ {date} 경기 일정 조회 오류: {e}")
            return []
    
    def save_game_to_db(self, game_data: Dict[str, Any]) -> bool:
        """경기 데이터를 DB에 저장"""
        try:
            # API 데이터를 DB 스키마에 맞게 변환
            db_data = {
                "game_id": game_data.get("gameId"),
                "super_category_id": game_data.get("superCategoryId"),
                "category_id": game_data.get("categoryId"),
                "category_name": game_data.get("categoryName"),
                "game_date": game_data.get("gameDate"),
                "game_date_time": game_data.get("gameDateTime"),
                "time_tbd": game_data.get("timeTbd", False),
                "stadium": game_data.get("stadium"),
                "title": game_data.get("title"),
                "home_team_code": game_data.get("homeTeamCode"),
                "home_team_name": game_data.get("homeTeamName"),
                "home_team_score": game_data.get("homeTeamScore", 0),
                "away_team_code": game_data.get("awayTeamCode"),
                "away_team_name": game_data.get("awayTeamName"),
                "away_team_score": game_data.get("awayTeamScore", 0),
                "winner": game_data.get("winner"),
                "status_code": game_data.get("statusCode"),
                "status_num": game_data.get("statusNum", 0),
                "status_info": game_data.get("statusInfo"),
                "cancel": game_data.get("cancel", False),
                "suspended": game_data.get("suspended", False),
                "has_video": game_data.get("hasVideo", False),
                "round_code": game_data.get("roundCode"),
                "reversed_home_away": game_data.get("reversedHomeAway", False),
                "home_team_emblem_url": game_data.get("homeTeamEmblemUrl"),
                "away_team_emblem_url": game_data.get("awayTeamEmblemUrl"),
                "game_on_air": game_data.get("gameOnAir", False),
                "widget_enable": game_data.get("widgetEnable", False),
                "special_match_info": game_data.get("specialMatchInfo"),
                "series_outcome": game_data.get("seriesOutcome"),
                "home_starter_name": game_data.get("homeStarterName"),
                "away_starter_name": game_data.get("awayStarterName"),
                "win_pitcher_name": game_data.get("winPitcherName"),
                "lose_pitcher_name": game_data.get("losePitcherName"),
                "home_current_pitcher_name": game_data.get("homeCurrentPitcherName"),
                "away_current_pitcher_name": game_data.get("awayCurrentPitcherName"),
                "series_game_no": game_data.get("seriesGameNo", 0),
                "broad_channel": game_data.get("broadChannel"),
                "round_name": game_data.get("roundName"),
                "round_game_no": game_data.get("roundGameNo", 0)
            }
            
            # 기존 데이터 확인 (game_id로 중복 체크)
            existing = self.supabase.supabase.table("game_schedule").select("*").eq("game_id", db_data["game_id"]).execute()
            
            if existing.data:
                # 기존 데이터 업데이트
                result = self.supabase.supabase.table("game_schedule").update(db_data).eq("game_id", db_data["game_id"]).execute()
                print(f"✅ {db_data['game_id']} 경기 데이터 업데이트 완료")
            else:
                # 새 데이터 삽입
                result = self.supabase.supabase.table("game_schedule").insert(db_data).execute()
                print(f"✅ {db_data['game_id']} 경기 데이터 저장 완료")
            
            return True
            
        except Exception as e:
            print(f"❌ {game_data.get('gameId', 'Unknown')} 경기 데이터 저장 오류: {e}")
            return False
    
    def collect_games_for_month(self, year: int, month: int) -> int:
        """특정 월의 모든 경기 데이터 수집"""
        print(f"\n📅 {year}년 {month}월 경기 데이터 수집 시작")
        print("=" * 50)
        
        success_count = 0
        fail_count = 0
        
        # 해당 월의 첫째 날과 마지막 날 계산
        start_date = datetime(year, month, 1)
        if month == 12:
            end_date = datetime(year + 1, 1, 1) - timedelta(days=1)
        else:
            end_date = datetime(year, month + 1, 1) - timedelta(days=1)
        
        current_date = start_date
        
        while current_date <= end_date:
            date_str = current_date.strftime("%Y-%m-%d")
            
            # 해당 날짜의 경기 데이터 조회
            games = self.fetch_games_for_date(date_str)
            
            # 각 경기 데이터를 DB에 저장
            for game in games:
                if self.save_game_to_db(game):
                    success_count += 1
                else:
                    fail_count += 1
            
            # 다음 날로 이동
            current_date += timedelta(days=1)
            
            # API 호출 간격 조절 (서버 부하 방지)
            import time
            time.sleep(0.5)
        
        print(f"\n✅ {year}년 {month}월 수집 완료!")
        print(f"   성공: {success_count}개")
        print(f"   실패: {fail_count}개")
        
        return success_count
    
    def collect_games_march_to_september(self, year: int = 2025) -> int:
        """3월부터 9월까지의 모든 경기 데이터 수집"""
        print("🚀 2025년 3월~9월 경기 데이터 수집 시작")
        print("=" * 60)
        print(f"⏰ 시작 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        total_success = 0
        
        # 3월부터 9월까지 수집
        for month in range(3, 10):
            success_count = self.collect_games_for_month(year, month)
            total_success += success_count
        
        print("\n" + "=" * 60)
        print(f"🎉 전체 수집 작업 완료!")
        print(f"✅ 총 성공: {total_success}개")
        print(f"⏰ 완료 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        return total_success

def main():
    """메인 함수"""
    try:
        collector = GameScheduleCollector()
        
        # 2025년 3월~9월 경기 데이터 수집
        total_games = collector.collect_games_march_to_september(2025)
        
        print(f"\n🎯 수집 완료: 총 {total_games}개 경기 데이터 저장됨")
        
    except Exception as e:
        print(f"❌ 수집 작업 실패: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
