#!/usr/bin/env python3
"""
새로운 정규화된 테이블 구조를 사용하는 Supabase 클라이언트 관리
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
        """필요한 테이블들이 존재하는지 확인"""
        try:
            # players 테이블 확인
            try:
                self.supabase.table("players").select("id").limit(1).execute()
                print("✅ players 테이블 존재 확인")
            except:
                print("❌ players 테이블이 존재하지 않습니다.")
                return False
            
            # player_season_stats 테이블 확인
            try:
                self.supabase.table("player_season_stats").select("id").limit(1).execute()
                print("✅ player_season_stats 테이블 존재 확인")
            except:
                print("❌ player_season_stats 테이블이 존재하지 않습니다.")
                return False
            
            # player_game_stats 테이블 확인
            try:
                self.supabase.table("player_game_stats").select("id").limit(1).execute()
                print("✅ player_game_stats 테이블 존재 확인")
            except:
                print("❌ player_game_stats 테이블이 존재하지 않습니다.")
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
            print(f"❌ 테이블 확인 오류: {e}")
            return False
    
    def get_player_basic_info(self, player_name: str) -> Optional[Dict[str, Any]]:
        """선수 기본 정보 조회"""
        try:
            result = self.supabase.table("players").select("*").eq("player_name", player_name).execute()
            
            if result.data:
                return result.data[0]
            else:
                print(f"❌ {player_name} 선수 기본 정보를 찾을 수 없습니다.")
                return None
                
        except Exception as e:
            print(f"❌ 선수 기본 정보 조회 오류: {e}")
            return None
    
    def get_player_season_stats(self, player_name: str = None, player_id: int = None, gyear: str = "2025") -> List[Dict[str, Any]]:
        """선수 시즌별 통계 조회"""
        try:
            query = self.supabase.table("player_season_stats").select("*")
            
            if player_name:
                query = query.eq("player_name", player_name)
            elif player_id:
                query = query.eq("player_id", player_id)
            
            if gyear:
                query = query.eq("gyear", gyear)
            
            result = query.execute()
            return result.data or []
                
        except Exception as e:
            print(f"❌ 선수 시즌별 통계 조회 오류: {e}")
            return []
    
    def get_player_game_stats(self, player_name: str = None, player_id: int = None, limit: int = 10) -> List[Dict[str, Any]]:
        """선수 경기별 통계 조회"""
        try:
            query = self.supabase.table("player_game_stats").select("*")
            
            if player_name:
                query = query.eq("player_name", player_name)
            elif player_id:
                query = query.eq("player_id", player_id)
            
            query = query.order("created_at", desc=True).limit(limit)
            result = query.execute()
            return result.data or []
                
        except Exception as e:
            print(f"❌ 선수 경기별 통계 조회 오류: {e}")
            return []
    
    def get_player_complete_data(self, player_name: str) -> Optional[Dict[str, Any]]:
        """선수의 모든 데이터를 통합해서 조회 (기존 player_info와 유사한 형태)"""
        try:
            # 1. 기본 정보 조회
            basic_info = self.get_player_basic_info(player_name)
            if not basic_info:
                return None
            
            # 2. 시즌별 통계 조회
            season_stats = self.get_player_season_stats(player_name=player_name)
            
            # 3. 경기별 통계 조회 (최근 10경기)
            game_stats = self.get_player_game_stats(player_name=player_name, limit=10)
            
            # 4. 기존 player_info 형태로 데이터 구성
            player_data = {
                "player_name": basic_info["player_name"],
                "pcode": basic_info["pcode"],
                "team": basic_info["team"],
                "position": basic_info["position"],
                "record": {
                    "season": season_stats
                },
                "game": game_stats,
                "basicRecord": {
                    "position": basic_info["position"],
                    "team": basic_info["team"]
                }
            }
            
            return player_data
                
        except Exception as e:
            print(f"❌ 선수 통합 데이터 조회 오류: {e}")
            return None
    
    def search_players(self, search_term: str) -> List[Dict[str, Any]]:
        """선수 검색"""
        try:
            result = self.supabase.table("players").select("*").ilike("player_name", f"%{search_term}%").execute()
            return result.data or []
            
        except Exception as e:
            print(f"❌ 선수 검색 오류: {e}")
            return []
    
    def get_all_players(self) -> List[Dict[str, Any]]:
        """모든 선수 기본 정보 조회"""
        try:
            result = self.supabase.table("players").select("*").execute()
            return result.data or []
            
        except Exception as e:
            print(f"❌ 모든 선수 데이터 조회 오류: {e}")
            return []
    
    def get_players_by_team(self, team_code: str) -> List[Dict[str, Any]]:
        """특정 팀의 선수들 조회"""
        try:
            result = self.supabase.table("players").select("*").eq("team", team_code).execute()
            return result.data or []
            
        except Exception as e:
            print(f"❌ 팀별 선수 조회 오류: {e}")
            return []
    
    def get_players_by_position(self, position: str) -> List[Dict[str, Any]]:
        """특정 포지션의 선수들 조회"""
        try:
            result = self.supabase.table("players").select("*").eq("position", position).execute()
            return result.data or []
            
        except Exception as e:
            print(f"❌ 포지션별 선수 조회 오류: {e}")
            return []
    
    def get_top_players_by_stat(self, stat_field: str, position: str = None, team: str = None, limit: int = 10) -> List[Dict[str, Any]]:
        """특정 통계 기준 상위 선수들 조회"""
        try:
            query = self.supabase.table("player_season_stats").select(f"*, players!inner(player_name, team, position)")
            
            if position:
                query = query.eq("players.position", position)
            if team:
                query = query.eq("team", team)
            
            # 2025년 데이터만
            query = query.eq("gyear", "2025")
            
            # 통계 필드로 정렬 (내림차순)
            query = query.order(stat_field, desc=True).limit(limit)
            
            result = query.execute()
            return result.data or []
                
        except Exception as e:
            print(f"❌ 상위 선수 조회 오류: {e}")
            return []
    
    def get_player_mapping(self) -> Dict[str, str]:
        """선수 매핑 정보 조회 (player_name -> pcode)"""
        try:
            result = self.supabase.table("players").select("player_name, pcode").execute()
            
            if result.data:
                mapping = {}
                for player in result.data:
                    player_name = player.get("player_name")
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
            result = self.supabase.table("players").select("pcode").eq("player_name", player_name).execute()
            
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
    
    # 기존 player_info 호환성을 위한 메서드들
    def get_player_data(self, player_name: str) -> Optional[Dict[str, Any]]:
        """기존 호환성을 위한 메서드 (get_player_complete_data와 동일)"""
        return self.get_player_complete_data(player_name)
    
    def save_player_data(self, player_data: Dict[str, Any]) -> bool:
        """선수 데이터 저장 (새로운 구조에서는 사용하지 않음)"""
        print("⚠️ 새로운 테이블 구조에서는 save_player_data를 사용하지 않습니다.")
        print("   대신 create_tables_and_migrate.py를 사용하여 데이터를 마이그레이션하세요.")
        return False
