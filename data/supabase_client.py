#!/usr/bin/env python3
"""
Supabase 클라이언트 관리
"""

import os
from typing import Dict, Any, Optional, List
from datetime import datetime
from supabase import create_client, Client
from dotenv import load_dotenv

# 환경변수 로드
load_dotenv()

class SupabaseManager:
    def __init__(self):
        """Supabase 클라이언트 초기화"""
        try:
            supabase_url = os.getenv("SUPABASE_URL")
            # 서비스 키 우선 사용 (RLS 우회)
            supabase_key = os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_ANON_KEY")
            
            if not supabase_url or not supabase_key:
                raise ValueError("SUPABASE_URL과 SUPABASE_SERVICE_KEY 또는 SUPABASE_ANON_KEY 환경변수가 필요합니다.")
            
            # Supabase 클라이언트 생성 (기본 옵션만 사용)
            self.supabase: Client = create_client(supabase_url, supabase_key)
            print("✅ Supabase 클라이언트 초기화 완료")
            
        except Exception as e:
            print(f"❌ Supabase 클라이언트 초기화 실패: {e}")
            raise e
    
    def create_tables(self) -> bool:
        """필요한 테이블들이 존재하는지 확인하고 없으면 생성"""
        try:
            # player_info 테이블 확인
            try:
                self.supabase.table("player_info").select("id").limit(1).execute()
                print("✅ player_info 테이블 존재 확인")
            except:
                print("❌ player_info 테이블이 존재하지 않습니다.")
                return False
            
            # game_schedule 테이블 확인
            try:
                self.supabase.table("game_schedule").select("id").limit(1).execute()
                print("✅ game_schedule 테이블 존재 확인")
            except:
                print("❌ game_schedule 테이블이 존재하지 않습니다.")
                return False
            
            return True
            
        except Exception as e:
            print(f"❌ 테이블 생성 오류: {e}")
            return False
    
    def save_player_data(self, player_data: Dict[str, Any]) -> bool:
        """선수 데이터 저장"""
        try:
            player_name = player_data.get("playerName")
            if not player_name:
                print("❌ 선수 이름이 없습니다.")
                return False
            
            # 기존 데이터 확인
            existing = self.supabase.table("player_info").select("*").eq("playerName", player_name).execute()
            
            data_to_save = {
                "playerName": player_name,
                "record": player_data.get("record", {}),
                "chart": player_data.get("chart", {}),
                "vsTeam": player_data.get("vsTeam", {}),
                "basicRecord": player_data.get("basicRecord", {})
            }
            
            if existing.data:
                # 기존 데이터 업데이트
                result = self.supabase.table("player_info").update(data_to_save).eq("playerName", player_name).execute()
                print(f"✅ {player_name} 선수 데이터 업데이트 완료")
            else:
                # 새 데이터 삽입
                result = self.supabase.table("player_info").insert(data_to_save).execute()
                print(f"✅ {player_name} 선수 데이터 저장 완료")
            
            return True
            
        except Exception as e:
            print(f"❌ 선수 데이터 저장 오류: {e}")
            return False
    
    def get_player_data(self, player_name: str) -> Optional[Dict[str, Any]]:
        """선수 데이터 조회"""
        try:
            result = self.supabase.table("player_info").select("*").eq("playerName", player_name).execute()
            
            if result.data:
                player_data = result.data[0]
                return {
                    "playerName": player_data["playerName"],
                    "record": player_data["record"],
                    "chart": player_data["chart"],
                    "vsTeam": player_data["vsTeam"],
                    "basicRecord": player_data["basicRecord"]
                }
            else:
                print(f"❌ {player_name} 선수 데이터를 찾을 수 없습니다.")
                return None
                
        except Exception as e:
            print(f"❌ 선수 데이터 조회 오류: {e}")
            return None
    
    def search_players(self, search_term: str) -> List[Dict[str, Any]]:
        """선수 검색"""
        try:
            result = self.supabase.table("player_info").select("*").ilike("playerName", f"%{search_term}%").execute()
            return result.data or []
            
        except Exception as e:
            print(f"❌ 선수 검색 오류: {e}")
            return []
    
    def get_all_players(self) -> List[Dict[str, Any]]:
        """모든 선수 데이터 조회 (pcode 테이블에서)"""
        try:
            result = self.supabase.table("pcode").select("*").execute()
            return result.data or []
            
        except Exception as e:
            print(f"❌ 모든 선수 데이터 조회 오류: {e}")
            return []
    
    def get_player_mapping(self) -> Dict[str, str]:
        """선수 매핑 정보 조회 (playerName -> pcode)"""
        try:
            result = self.supabase.table("pcode").select("playerName, pcode").execute()
            
            if result.data:
                mapping = {}
                for player in result.data:
                    player_name = player.get("playerName")
                    pcode = player.get("pcode")
                    if player_name and pcode:
                        mapping[player_name] = pcode
                return mapping
            else:
                print("❌ 선수 매핑 데이터를 찾을 수 없습니다.")
                return {}
                
        except Exception as e:
            print(f"❌ 선수 매핑 조회 오류: {e}")
            return {}
    
    def get_pcode_by_name(self, player_name: str) -> Optional[str]:
        """선수 이름으로 pcode 조회"""
        try:
            result = self.supabase.table("pcode").select("pcode").eq("playerName", player_name).execute()
            
            if result.data:
                return result.data[0].get("pcode")
            else:
                print(f"❌ {player_name} 선수의 pcode를 찾을 수 없습니다.")
                return None
                
        except Exception as e:
            print(f"❌ pcode 조회 오류: {e}")
            return None
    
    def get_game_schedule(self, date: str = None) -> List[Dict[str, Any]]:
        """경기 일정 조회"""
        try:
            if date:
                result = self.supabase.table("game_schedule").select("*").eq("date", date).execute()
            else:
                result = self.supabase.table("game_schedule").select("*").order("date").execute()
            
            return result.data or []
            
        except Exception as e:
            print(f"❌ 경기 일정 조회 오류: {e}")
            return []
    def get_future_games(self) -> List[Dict[str, Any]]:
        """오늘 날짜 기준으로 미래 경기들만 조회"""
        try:
            from datetime import datetime
            
            # 오늘 날짜를 MM.DD 형식으로 변환
            today = datetime.now()
            today_str = today.strftime('%m.%d')
            
            # 모든 경기 일정 가져오기
            all_games = self.get_game_schedule()
            
            # 오늘 이후의 경기들만 필터링
            future_games = []
            for game in all_games:
                game_date = game.get('date', '')
                
                # 날짜 형식 정규화 (요일 제거)
                if '(' in game_date:
                    date_part = game_date.split('(')[0]
                else:
                    date_part = game_date
                
                # 오늘 이후의 경기인지 확인
                if self._is_future_date(date_part, today_str):
                    future_games.append(game)
            
            print(f"📅 오늘({today_str}) 기준 미래 경기: {len(future_games)}개")
            return future_games
            
        except Exception as e:
            print(f"❌ 미래 경기 조회 오류: {e}")
            return []
    
    def _is_future_date(self, game_date: str, today_date: str) -> bool:
        """게임 날짜가 오늘 이후인지 확인"""
        try:
            # MM.DD 형식 비교
            game_month, game_day = map(int, game_date.split('.'))
            today_month, today_day = map(int, today_date.split('.'))
            
            # 월이 다르면 월 비교
            if game_month != today_month:
                return game_month > today_month
            
            # 같은 월이면 일 비교
            return game_day >= today_day
            
        except Exception as e:
            print(f"❌ 날짜 비교 오류: {e}")
            return False