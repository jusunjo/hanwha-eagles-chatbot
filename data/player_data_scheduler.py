#!/usr/bin/env python3
"""
선수 데이터 수집 스케줄러
매일 밤 11시 59분에 pcode 테이블의 모든 선수 데이터를 수집하여 player_info 테이블에 저장
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
    
    def get_all_players_from_player_info(self) -> List[Dict[str, Any]]:
        """player_info 테이블에서 모든 선수 조회"""
        try:
            print("🔍 player_info 테이블에서 모든 선수 조회 중...")
            result = self.supabase.supabase.table("player_info").select("*").execute()
            
            if result.data:
                print(f"✅ {len(result.data)}명의 선수 조회 완료")
                return result.data
            else:
                print("❌ player_info 테이블에 선수 데이터가 없습니다.")
                return []
                
        except Exception as e:
            print(f"❌ player_info 테이블 조회 오류: {e}")
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
                "playerName": player_name,
                "record": {},
                "chart": {},
                "vsTeam": {},
                "basicRecord": {}
            }
            
            # 기본 기록 추출 (basicRecord)
            basic_record_pattern = r'basicRecord":\s*({[^}]+})'
            basic_match = re.search(basic_record_pattern, html_content)
            if basic_match:
                try:
                    basic_record_str = basic_match.group(1) + "}"
                    basic_record = json.loads(basic_record_str)
                    player_data["basicRecord"] = basic_record
                except:
                    pass
            
            # 시즌 기록 추출 (record.season)
            season_pattern = r'"season":\s*(\[[^\]]+\])'
            season_match = re.search(season_pattern, html_content)
            if season_match:
                try:
                    season_str = season_match.group(1)
                    season_data = json.loads(season_str)
                    player_data["record"]["season"] = season_data
                except:
                    pass
            
            # 경기별 기록 추출 (record.game)
            game_pattern = r'"game":\s*(\[[^\]]+\])'
            game_match = re.search(game_pattern, html_content)
            if game_match:
                try:
                    game_str = game_match.group(1)
                    game_data = json.loads(game_str)
                    player_data["record"]["game"] = game_data
                except:
                    pass
            
            # 차트 데이터 추출 (chart)
            chart_pattern = r'"chart":\s*({[^}]+})'
            chart_match = re.search(chart_pattern, html_content)
            if chart_match:
                try:
                    chart_str = chart_match.group(1) + "}"
                    chart_data = json.loads(chart_str)
                    player_data["chart"] = chart_data
                except:
                    pass
            
            # VS 팀 데이터 추출 (vsTeam)
            vsteam_pattern = r'"vsteam":\s*(\[[^\]]+\])'
            vsteam_match = re.search(vsteam_pattern, html_content)
            if vsteam_match:
                try:
                    vsteam_str = vsteam_match.group(1)
                    vsteam_data = json.loads(vsteam_str)
                    player_data["vsTeam"] = vsteam_data
                except:
                    pass
            
            print(f"📊 {player_name} 추출된 데이터:")
            print(f"   - basicRecord: {'있음' if player_data['basicRecord'] else '없음'}")
            print(f"   - season: {'있음' if player_data['record'].get('season') else '없음'}")
            print(f"   - game: {'있음' if player_data['record'].get('game') else '없음'}")
            print(f"   - chart: {'있음' if player_data['chart'] else '없음'}")
            print(f"   - vsTeam: {'있음' if player_data['vsTeam'] else '없음'}")
            
            return player_data
            
        except Exception as e:
            print(f"❌ {player_name} HTML 파싱 오류: {e}")
            return None
    
    def save_player_data_to_db(self, player_data: Dict[str, Any]) -> bool:
        """선수 데이터를 player_info 테이블에 저장"""
        try:
            player_name = player_data.get("playerName")
            if not player_name:
                print("❌ 선수 이름이 없습니다.")
                return False
            
            # 기존 데이터 확인
            existing = self.supabase.supabase.table("player_info").select("*").eq("playerName", player_name).execute()
            
            data_to_save = {
                "playerName": player_name,
                "record": player_data.get("record", {}),
                "chart": player_data.get("chart", {}),
                "vsTeam": player_data.get("vsTeam", {}),
                "basicRecord": player_data.get("basicRecord", {})
            }
            
            if existing.data:
                # 기존 데이터 업데이트
                result = self.supabase.supabase.table("player_info").update(data_to_save).eq("playerName", player_name).execute()
                print(f"✅ {player_name} 선수 데이터 업데이트 완료")
            else:
                # 새 데이터 삽입
                result = self.supabase.supabase.table("player_info").insert(data_to_save).execute()
                print(f"✅ {player_name} 선수 데이터 저장 완료")
            
            return True
            
        except Exception as e:
            print(f"❌ {player_name} 선수 데이터 저장 오류: {e}")
            return False
    
    def collect_all_players_data(self):
        """모든 선수 데이터 수집 및 저장"""
        print("🚀 선수 데이터 수집 작업 시작")
        print("=" * 60)
        print(f"⏰ 시작 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        try:
            # 1. player_info 테이블에서 모든 선수 조회
            players = self.get_all_players_from_player_info()
            
            if not players:
                print("❌ 수집할 선수가 없습니다.")
                return
            
            # 2. 각 선수별로 데이터 수집 및 저장
            success_count = 0
            fail_count = 0
            
            for i, player in enumerate(players, 1):
                player_name = player.get("playerName")
                pcode = player.get("pcode")
                
                if not player_name or not pcode:
                    print(f"❌ {i}/{len(players)}: 선수 정보가 불완전합니다. 건너뜁니다.")
                    fail_count += 1
                    continue
                
                print(f"\n📊 {i}/{len(players)}: {player_name} 처리 중...")
                
                # API에서 데이터 수집
                player_data = self.fetch_player_data_from_api(player_name, pcode)
                
                if player_data:
                    # DB에 저장
                    if self.save_player_data_to_db(player_data):
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
