#!/usr/bin/env python3
"""
새로운 정규화된 테이블 구조를 사용하는 선수 데이터 수집 스케줄러
매일 밤 11시 59분에 players 테이블의 모든 선수 데이터를 수집하여 
player_season_stats와 player_game_stats 테이블에 저장
"""

import os
import sys
import json
import re
import requests
from datetime import datetime
from typing import Dict, Any, List
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from dotenv import load_dotenv

# 환경변수 로드
load_dotenv()

# 현재 디렉토리를 Python 경로에 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.supabase_client import SupabaseManager

class PlayerDataScheduler:
    def __init__(self):
        """스케줄러 초기화"""
        self.supabase = None
        self.player_record_base_url = "https://m.sports.naver.com/player/index"
        
        # Supabase 연결
        try:
            self.supabase = SupabaseManager()
            print("✅ Supabase 연결 성공")
        except Exception as e:
            print(f"❌ Supabase 연결 실패: {e}")
            raise e
    
    def get_all_players_from_players_table(self) -> List[Dict[str, Any]]:
        """players 테이블에서 모든 선수 조회"""
        try:
            print("🔍 players 테이블에서 모든 선수 조회 중...")
            result = self.supabase.supabase.table("players").select("*").execute()
            
            if result.data:
                print(f"✅ {len(result.data)}명의 선수 조회 완료")
                return result.data
            else:
                print("❌ players 테이블에 선수 데이터가 없습니다.")
                return []
                
        except Exception as e:
            print(f"❌ players 테이블 조회 오류: {e}")
            return []
    
    def fetch_player_data_from_api(self, player_name: str, pcode: str) -> Dict[str, Any]:
        """네이버 API에서 선수 데이터 수집"""
        try:
            print(f"🏃 {player_name} 선수 데이터 API 요청 중...")
            
            params = {
                'from': 'nx',
                'playerId': pcode,
                'category': 'kbo',
                'tab': 'record'
            }
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            
            response = requests.get(self.player_record_base_url, params=params, headers=headers, timeout=30)
            print(f"📊 {player_name} API 응답 코드: {response.status_code}")
            
            if response.status_code == 200:
                html_content = response.text
                player_data = self.extract_player_data_from_html(html_content, player_name)
                
                if player_data:
                    print(f"✅ {player_name} 선수 데이터 추출 성공")
                    return player_data
                else:
                    print(f"❌ {player_name} 선수 데이터 추출 실패")
                    return None
            else:
                print(f"❌ {player_name} API 호출 실패: {response.status_code}")
                return None
                
        except Exception as e:
            print(f"❌ {player_name} API 요청 오류: {e}")
            return None
    
    def extract_player_data_from_html(self, html_content: str, player_name: str) -> Dict[str, Any]:
        """HTML에서 선수 데이터 추출"""
        try:
            player_data = {
                "player_name": player_name,
                "season_stats": [],
                "game_stats": []
            }
            
            # 기본 기록 추출 (basicRecord)
            basic_record_pattern = r'basicRecord":\s*({[^}]+})'
            basic_match = re.search(basic_record_pattern, html_content)
            if basic_match:
                try:
                    basic_record_str = basic_match.group(1) + "}"
                    basic_record = json.loads(basic_record_str)
                    player_data["basic_record"] = basic_record
                except:
                    pass
            
            # 시즌 기록 추출 (record.season)
            season_pattern = r'"season":\s*(\[[^\]]+\])'
            season_match = re.search(season_pattern, html_content)
            if season_match:
                try:
                    season_str = season_match.group(1)
                    season_data = json.loads(season_str)
                    player_data["season_stats"] = season_data
                except:
                    pass
            
            # 경기별 기록 추출 (record.game)
            game_pattern = r'"game":\s*(\[[^\]]+\])'
            game_match = re.search(game_pattern, html_content)
            if game_match:
                try:
                    game_str = game_match.group(1)
                    game_data = json.loads(game_str)
                    player_data["game_stats"] = game_data
                except:
                    pass
            
            print(f"📊 {player_name} 추출된 데이터:")
            print(f"   - season_stats: {len(player_data['season_stats'])}개")
            print(f"   - game_stats: {len(player_data['game_stats'])}개")
            print(f"   - basic_record: {'있음' if player_data.get('basic_record') else '없음'}")
            
            return player_data
            
        except Exception as e:
            print(f"❌ {player_name} HTML 파싱 오류: {e}")
            return None
    
    def save_player_season_stats(self, player_id: int, player_name: str, season_stats: List[Dict[str, Any]]) -> bool:
        """선수 시즌별 통계를 player_season_stats 테이블에 저장"""
        try:
            if not season_stats:
                print(f"⚠️ {player_name} 시즌별 통계가 없습니다.")
                return True
            
            # 기존 시즌별 통계 삭제
            try:
                self.supabase.supabase.table("player_season_stats").delete().eq("player_id", player_id).execute()
                print(f"🗑️ {player_name} 기존 시즌별 통계 삭제 완료")
            except Exception as e:
                print(f"⚠️ {player_name} 기존 시즌별 통계 삭제 중 오류: {e}")
            
            # 새로운 시즌별 통계 삽입
            stats_to_insert = []
            for stat in season_stats:
                # "통산" 데이터는 제외
                if stat.get('gyear') == '통산':
                    continue
                
                stat_data = {
                    'player_id': player_id,
                    'player_name': player_name,
                    'gyear': stat.get('gyear', ''),
                    'team': stat.get('team', ''),
                    'gamenum': self._safe_convert_int(stat.get('gamenum')),
                    'war': self._safe_convert_float(stat.get('war')),
                    
                    # 타자 통계
                    'hra': self._safe_convert_float(stat.get('hra')),
                    'ab': self._safe_convert_int(stat.get('ab')),
                    'run': self._safe_convert_int(stat.get('run')),
                    'hit': self._safe_convert_int(stat.get('hit')),
                    'h2': self._safe_convert_int(stat.get('h2')),
                    'h3': self._safe_convert_int(stat.get('h3')),
                    'hr': self._safe_convert_int(stat.get('hr')),
                    'tb': self._safe_convert_int(stat.get('tb')),
                    'rbi': self._safe_convert_int(stat.get('rbi')),
                    'sb': self._safe_convert_int(stat.get('sb')),
                    'cs': self._safe_convert_int(stat.get('cs')),
                    'sh': self._safe_convert_int(stat.get('sh')),
                    'sf': self._safe_convert_int(stat.get('sf')),
                    'bb': self._safe_convert_int(stat.get('bb')),
                    'hp': self._safe_convert_int(stat.get('hp')),
                    'kk': self._safe_convert_int(stat.get('kk')),
                    'gd': self._safe_convert_int(stat.get('gd')),
                    'err': self._safe_convert_int(stat.get('err')),
                    'obp': self._safe_convert_float(stat.get('obp')),
                    'slg': self._safe_convert_float(stat.get('slg')),
                    'ops': self._safe_convert_float(stat.get('ops')),
                    'isop': self._safe_convert_float(stat.get('isop')),
                    'babip': self._safe_convert_float(stat.get('babip')),
                    'wrcPlus': self._safe_convert_float(stat.get('wrcPlus')),
                    'woba': self._safe_convert_float(stat.get('woba')),
                    'wpa': self._safe_convert_float(stat.get('wpa')),
                    'paFlag': self._safe_convert_int(stat.get('paFlag')),
                    
                    # 투수 통계
                    'era': self._safe_convert_float(stat.get('era')),
                    'w': self._safe_convert_int(stat.get('w')),
                    'l': self._safe_convert_int(stat.get('l')),
                    'sv': self._safe_convert_int(stat.get('sv')),
                    'hold': self._safe_convert_int(stat.get('hold')),
                    'cg': self._safe_convert_int(stat.get('cg')),
                    'sho': self._safe_convert_int(stat.get('sho')),
                    'bf': self._safe_convert_int(stat.get('bf')),
                    'inn': stat.get('inn'),  # 이닝은 문자열 그대로
                    'inn2': self._safe_convert_int(stat.get('inn2')),
                    'r': self._safe_convert_int(stat.get('r')),
                    'er': self._safe_convert_int(stat.get('er')),
                    'whip': self._safe_convert_float(stat.get('whip')),
                    'k9': self._safe_convert_float(stat.get('k9')),
                    'bb9': self._safe_convert_float(stat.get('bb9')),
                    'kbb': self._safe_convert_float(stat.get('kbb')),
                    'qs': self._safe_convert_int(stat.get('qs')),
                    'wra': self._safe_convert_float(stat.get('wra')),
                }
                
                # None 값 제거
                stat_data = {k: v for k, v in stat_data.items() if v is not None}
                stats_to_insert.append(stat_data)
            
            if stats_to_insert:
                result = self.supabase.supabase.table("player_season_stats").insert(stats_to_insert).execute()
                print(f"✅ {player_name} 시즌별 통계 {len(stats_to_insert)}개 저장 완료")
                return True
            else:
                print(f"⚠️ {player_name} 저장할 시즌별 통계가 없습니다.")
                return True
                
        except Exception as e:
            print(f"❌ {player_name} 시즌별 통계 저장 오류: {e}")
            return False
    
    def save_player_game_stats(self, player_id: int, player_name: str, game_stats: List[Dict[str, Any]]) -> bool:
        """선수 경기별 통계를 player_game_stats 테이블에 저장"""
        try:
            if not game_stats:
                print(f"⚠️ {player_name} 경기별 통계가 없습니다.")
                return True
            
            # 기존 경기별 통계 삭제
            try:
                self.supabase.supabase.table("player_game_stats").delete().eq("player_id", player_id).execute()
                print(f"🗑️ {player_name} 기존 경기별 통계 삭제 완료")
            except Exception as e:
                print(f"⚠️ {player_name} 기존 경기별 통계 삭제 중 오류: {e}")
            
            # 새로운 경기별 통계 삽입 (최근 10경기만)
            stats_to_insert = []
            for stat in game_stats[:10]:  # 최근 10경기만
                stat_data = {
                    'player_id': player_id,
                    'player_name': player_name,
                    'gameId': stat.get('gameId'),
                    'gday': stat.get('gday'),
                    'opponent': stat.get('opponent'),
                    
                    # 타자 통계
                    'ab': self._safe_convert_int(stat.get('ab')),
                    'run': self._safe_convert_int(stat.get('run')),
                    'hit': self._safe_convert_int(stat.get('hit')),
                    'h2': self._safe_convert_int(stat.get('h2')),
                    'h3': self._safe_convert_int(stat.get('h3')),
                    'hr': self._safe_convert_int(stat.get('hr')),
                    'rbi': self._safe_convert_int(stat.get('rbi')),
                    'sb': self._safe_convert_int(stat.get('sb')),
                    'cs': self._safe_convert_int(stat.get('cs')),
                    'bb': self._safe_convert_int(stat.get('bb')),
                    'kk': self._safe_convert_int(stat.get('kk')),
                    'hra': self._safe_convert_float(stat.get('hra')),
                    'sf': self._safe_convert_int(stat.get('sf')),
                    'sh': self._safe_convert_int(stat.get('sh')),
                    'gd': self._safe_convert_int(stat.get('gd')),
                    'dheader': stat.get('dheader'),
                    
                    # 투수 통계
                    'inn': stat.get('inn'),  # 이닝은 문자열 그대로
                    'er': self._safe_convert_int(stat.get('er')),
                    'whip': self._safe_convert_float(stat.get('whip')),
                    'hp': self._safe_convert_int(stat.get('hp')),
                }
                
                # None 값 제거
                stat_data = {k: v for k, v in stat_data.items() if v is not None}
                stats_to_insert.append(stat_data)
            
            if stats_to_insert:
                result = self.supabase.supabase.table("player_game_stats").insert(stats_to_insert).execute()
                print(f"✅ {player_name} 경기별 통계 {len(stats_to_insert)}개 저장 완료")
                return True
            else:
                print(f"⚠️ {player_name} 저장할 경기별 통계가 없습니다.")
                return True
                
        except Exception as e:
            print(f"❌ {player_name} 경기별 통계 저장 오류: {e}")
            return False
    
    def _safe_convert_int(self, value, default=None):
        """안전한 int 변환"""
        if value is None or value == '':
            return default
        try:
            if isinstance(value, str) and not value.replace('.', '').replace('-', '').isdigit():
                return default
            return int(float(value))
        except (ValueError, TypeError):
            return default
    
    def _safe_convert_float(self, value, default=None):
        """안전한 float 변환"""
        if value is None or value == '':
            return default
        try:
            # 분수 형식이면 None 반환 (inn 필드가 아닌 경우)
            if isinstance(value, str) and (' ' in value and '/' in value):
                return default
            return float(value)
        except (ValueError, TypeError):
            return default
    
    def collect_all_players_data(self):
        """모든 선수 데이터 수집 및 저장"""
        print("🚀 선수 데이터 수집 작업 시작")
        print("=" * 60)
        print(f"⏰ 시작 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        try:
            # 1. players 테이블에서 모든 선수 조회
            players = self.get_all_players_from_players_table()
            
            if not players:
                print("❌ 수집할 선수가 없습니다.")
                return
            
            # 2. 각 선수별로 데이터 수집 및 저장
            success_count = 0
            fail_count = 0
            
            for i, player in enumerate(players, 1):
                player_name = player.get("player_name")
                pcode = player.get("pcode")
                player_id = player.get("id")
                
                if not player_name or not pcode or not player_id:
                    print(f"❌ {i}/{len(players)}: 선수 정보가 불완전합니다. 건너뜁니다.")
                    fail_count += 1
                    continue
                
                print(f"\n📊 {i}/{len(players)}: {player_name} 처리 중...")
                
                # API에서 데이터 수집
                player_data = self.fetch_player_data_from_api(player_name, pcode)
                
                if player_data:
                    # 시즌별 통계 저장
                    season_success = self.save_player_season_stats(
                        player_id, player_name, player_data.get('season_stats', [])
                    )
                    
                    # 경기별 통계 저장
                    game_success = self.save_player_game_stats(
                        player_id, player_name, player_data.get('game_stats', [])
                    )
                    
                    if season_success and game_success:
                        success_count += 1
                        print(f"✅ {player_name} 완료")
                    else:
                        fail_count += 1
                        print(f"❌ {player_name} 저장 실패")
                else:
                    fail_count += 1
                    print(f"❌ {player_name} 데이터 수집 실패")
                
                # API 호출 간격 조절 (서버 부하 방지)
                import time
                time.sleep(1)
            
            print("\n" + "=" * 60)
            print(f"🎉 선수 데이터 수집 작업 완료!")
            print(f"✅ 성공: {success_count}명")
            print(f"❌ 실패: {fail_count}명")
            print(f"⏰ 완료 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
        except Exception as e:
            print(f"❌ 선수 데이터 수집 작업 오류: {e}")
            import traceback
            traceback.print_exc()
    
    def start_scheduler(self):
        """스케줄러 시작"""
        print("🕐 선수 데이터 수집 스케줄러 시작")
        print("⏰ 실행 시간: 매일 밤 11시 59분")
        
        scheduler = BlockingScheduler()
        
        # 매일 밤 11시 59분에 실행
        scheduler.add_job(
            self.collect_all_players_data,
            trigger=CronTrigger(hour=23, minute=59),
            id='player_data_collection',
            name='선수 데이터 수집',
            replace_existing=True
        )
        
        try:
            scheduler.start()
        except KeyboardInterrupt:
            print("\n⏹️ 스케줄러가 중단되었습니다.")
            scheduler.shutdown()

def main():
    """메인 함수"""
    try:
        scheduler = PlayerDataScheduler()
        
        # 명령행 인수 확인
        if len(sys.argv) > 1 and sys.argv[1] == "--now":
            # 즉시 실행
            print("🚀 즉시 실행 모드")
            scheduler.collect_all_players_data()
        else:
            # 스케줄러 시작
            scheduler.start_scheduler()
            
    except Exception as e:
        print(f"❌ 스케줄러 실행 오류: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
