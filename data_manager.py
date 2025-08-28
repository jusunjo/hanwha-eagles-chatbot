import requests
import json
import os
from datetime import datetime
from typing import Dict, Any, Optional

class HanwhaEaglesDataManager:
    def __init__(self):
        self.schedule_file = 'game_schedule.json'
        self.player_record_base_url = "https://m.sports.naver.com/player/index"
        self.player_mapping = self._load_player_mapping()
        
        # 오늘 날짜의 경기 ID를 동적으로 생성
        self.api_url = self._generate_today_game_api_url()
        
        print(f"📅 오늘 날짜: {datetime.now().strftime('%Y-%m-%d')}")
        print(f" API URL: {self.api_url}")
        print(" API 호출 시작...")
        self.data = self.fetch_api_data()
        
    def _generate_today_game_api_url(self) -> str:
        """오늘 날짜의 경기를 찾아서 API URL 생성"""
        try:
            # 오늘 날짜 정보
            today = datetime.now()
            today_str = today.strftime('%m.%d')
            today_year = today.strftime('%Y')
            
            print(f"🔍 오늘 날짜 ({today_str}) 경기 검색 중...")
            
            # 경기 일정 파일 로드
            if not os.path.exists(self.schedule_file):
                print(f"❌ {self.schedule_file} 파일을 찾을 수 없습니다.")
                return self._get_default_api_url()
            
            with open(self.schedule_file, 'r', encoding='utf-8') as f:
                schedule_data = json.load(f)
            
            # 오늘 날짜에 맞는 경기 찾기
            today_game = None
            for game in schedule_data.get('schedule', []):
                game_date = game.get('date', '')
                # "08.24(일)" 형식에서 날짜 부분만 추출
                if '(' in game_date:
                    date_part = game_date.split('(')[0]
                    if date_part == today_str:
                        today_game = game
                        break
            
            if today_game:
                print(f"✅ 오늘 경기 발견: {today_game['homeTeam']} vs {today_game['awayTeam']}")
                
                # 경기 ID 생성: YYYYMMDD + 홈팀코드 + 원정팀코드 + 2025
                home_code = today_game['homeTeamCode']
                away_code = today_game['awayTeamCode']
                game_date = today.strftime('%Y%m%d')
                
                # 두 가지 조합 시도
                combinations = [
                    (f"{game_date}{home_code}{away_code}02025", "홈팀코드+원정팀코드"),
                    (f"{game_date}{away_code}{home_code}02025", "원정팀코드+홈팀코드")
                ]
                
                for game_id, description in combinations:
                    api_url = f"https://api-gw.sports.naver.com/schedule/games/{game_id}/preview"
                    print(f"🔄 {description} 조합 시도: {game_id}")
                    
                    # API 호출 테스트
                    if self._test_api_url(api_url):
                        print(f"✅ {description} 조합 성공!")
                        print(f"🏟️ 경기장: {today_game['stadium']}")
                        print(f"🕐 경기시간: {today_game['time']}")
                        print(f"🎯 사용할 경기 ID: {game_id}")
                        return api_url
                    else:
                        print(f"❌ {description} 조합 실패")
                
                # 두 조합 모두 실패
                print("❌ 모든 경기 ID 조합이 실패했습니다.")
                return self._get_today_game_unavailable_url()
            else:
                print(f"❌ 오늘({today_str}) 경기가 없습니다.")
                return self._get_default_api_url()
                
        except Exception as e:
            print(f"❌ 경기 ID 생성 중 오류: {e}")
            return self._get_default_api_url()
    
    def _test_api_url(self, api_url: str) -> bool:
        """API URL이 유효한지 테스트"""
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            
            response = requests.get(api_url, headers=headers, timeout=5)
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    # 성공적인 응답인지 확인
                    if data.get('success') and 'result' in data:
                        return True
                except:
                    pass
            
            return False
            
        except Exception as e:
            print(f"   - API 테스트 중 오류: {e}")
            return False
    
    def _get_today_game_unavailable_url(self) -> str:
        """오늘 경기를 아직 불러올 수 없을 때 사용할 URL"""
        print("🔄 오늘 경기를 아직 불러올 수 없습니다. 기본 URL 사용")
        return "https://api-gw.sports.naver.com/schedule/games/111/preview"
        
    def _get_default_api_url(self) -> str:
        """기본 API URL 반환 (경기가 없을 때)"""
        print("🔄 기본 API URL 사용")
        return "https://api-gw.sports.naver.com/schedule/games/111/preview"
        
    def _load_player_mapping(self) -> Dict[str, str]:
        """player_mapping.json 파일 로드"""
        try:
            mapping_path = "player_mapping.json"
            if os.path.exists(mapping_path):
                with open(mapping_path, 'r', encoding='utf-8') as f:
                    mapping = json.load(f)
                    print(f"✅ 선수 매핑 데이터 로드 완료: {len(mapping)}명")
                    return mapping
            else:
                print("❌ player_mapping.json 파일을 찾을 수 없습니다.")
                return {}
        except Exception as e:
            print(f"❌ 선수 매핑 파일 로드 오류: {e}")
            return {}
    
    def get_player_id_by_name(self, player_name: str) -> Optional[str]:
        """선수 이름으로 player_id 조회"""
        return self.player_mapping.get(player_name)
    
    def get_player_data_by_name(self, player_name: str) -> Dict[str, Any]:
        """선수 이름으로 선수 데이터 조회"""
        player_id = self.get_player_id_by_name(player_name)
        if player_id:
            print(f"🏃 {player_name} 선수 데이터 요청 (ID: {player_id})")
            return self.fetch_player_record(player_id, player_name)
        else:
            print(f"❌ {player_name} 선수를 찾을 수 없습니다.")
            return {
                "playerName": player_name,
                "error": "등록되지 않은 선수입니다.",
                "record": {},
                "chart": {},
                "vsTeam": {},
                "basicRecord": {}
            }
    
    def fetch_api_data(self) -> Dict[str, Any]:
        """API에서 실시간 데이터 가져오기"""
        try:
            print("🚀 API 요청 전송 중...")
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            
            print(f"📋 요청 헤더: {headers}")
            response = requests.get(self.api_url, headers=headers, timeout=10)
            
            print(f"📊 HTTP 상태 코드: {response.status_code}")
            print(f"📏 응답 크기: {len(response.content)} bytes")
            
            response.raise_for_status()
            
            print("✅ API 응답 성공! JSON 파싱 중...")
            api_data = response.json()
            
            print(f"📈 응답 데이터 구조:")
            print(f"   - code: {api_data.get('code')}")
            print(f"   - success: {api_data.get('success')}")
            print(f"   - result 키 존재: {'result' in api_data}")
            if 'result' in api_data:
                print(f"   - previewData 키 존재: {'previewData' in api_data['result']}")
            
            # API 응답 구조 검증
            if self.validate_api_response(api_data):
                print("✅ API 응답 구조 검증 통과!")
                return api_data
            else:
                print("❌ API 응답 구조 검증 실패. 기본 데이터를 사용합니다.")
                return self.get_fallback_data()
                
        except requests.exceptions.RequestException as e:
            print(f"❌ API 호출 오류: {e}")
            return self.get_fallback_data()
        except json.JSONDecodeError as e:
            print(f"❌ JSON 파싱 오류: {e}")
            return self.get_fallback_data()
        except Exception as e:
            print(f"❌ 예상치 못한 오류: {e}")
            return self.get_fallback_data()
    
    def fetch_player_record(self, player_id: str, player_name: str = None) -> Dict[str, Any]:
        """선수의 record 데이터 가져오기"""
        try:
            print(f"🏃 선수 record 데이터 요청: {player_name or player_id}")
            
            params = {
                'from': 'nx',
                'playerId': player_id,  # 하드코딩된 값 대신 파라미터 사용
                'category': 'kbo',
                'tab': 'record'
            }
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            
            response = requests.get(self.player_record_base_url, params=params, headers=headers, timeout=10)
            print(f"📊 선수 record HTTP 상태 코드: {response.status_code}")
            
            if response.status_code == 200:
                html_content = response.text
                
                # HTML에서 record 데이터 추출
                record_data = self.extract_record_from_html(html_content, player_name or player_id)
                
                if record_data:
                    print(f"✅ {player_name or player_id} 선수 record 데이터 추출 성공!")
                    return record_data
                else:
                    print(f"❌ {player_name or player_id} 선수 record 데이터 추출 실패")
                    return self.get_player_fallback_data(player_name or player_id)
            else:
                print(f"❌ 선수 record API 호출 실패: {response.status_code}")
                return self.get_player_fallback_data(player_name or player_id)
                
        except Exception as e:
            print(f"❌ 선수 record 데이터 가져오기 오류: {e}")
            return self.get_player_fallback_data(player_name or player_id)
    
    def extract_record_from_html(self, html_content: str, player_name: str) -> Dict[str, Any]:
        """HTML에서 record 데이터 추출"""
        try:
            import re
            
            record_data = {
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
                    # JSON 파싱 시도
                    basic_record = json.loads(basic_record_str)
                    record_data["basicRecord"] = basic_record
                except:
                    pass
            
            # 시즌 기록 추출 (record.season)
            season_pattern = r'"season":\s*(\[[^\]]+\])'
            season_match = re.search(season_pattern, html_content)
            if season_match:
                try:
                    season_str = season_match.group(1)
                    season_data = json.loads(season_str)
                    record_data["record"]["season"] = season_data
                except:
                    pass
            
            # 경기별 기록 추출 (record.game)
            game_pattern = r'"game":\s*(\[[^\]]+\])'
            game_match = re.search(game_pattern, html_content)
            if game_match:
                try:
                    game_str = game_match.group(1)
                    game_data = json.loads(game_str)
                    record_data["record"]["game"] = game_data
                except:
                    pass
            
            # 차트 데이터 추출 (chart)
            chart_pattern = r'"chart":\s*({[^}]+})'
            chart_match = re.search(chart_pattern, html_content)
            if chart_match:
                try:
                    chart_str = chart_match.group(1) + "}"
                    chart_data = json.loads(chart_str)
                    record_data["chart"] = chart_data
                except:
                    pass
            
            # VS 팀 데이터 추출 (vsTeam)
            vsteam_pattern = r'"vsteam":\s*(\[[^\]]+\])'
            vsteam_match = re.search(vsteam_pattern, html_content)
            if vsteam_match:
                try:
                    vsteam_str = vsteam_match.group(1)
                    vsteam_data = json.loads(vsteam_str)
                    record_data["vsTeam"] = vsteam_data
                except:
                    pass
            
            print(f"📊 추출된 데이터:")
            print(f"   - basicRecord: {'있음' if record_data['basicRecord'] else '없음'}")
            print(f"   - season: {'있음' if record_data['record'].get('season') else '없음'}")
            print(f"   - game: {'있음' if record_data['record'].get('game') else '없음'}")
            print(f"   - chart: {'있음' if record_data['chart'] else '없음'}")
            print(f"   - vsTeam: {'있음' if record_data['vsTeam'] else '없음'}")
            
            return record_data
            
        except Exception as e:
            print(f"❌ HTML 파싱 오류: {e}")
            return None
    
    def get_player_fallback_data(self, player_name: str) -> Dict[str, Any]:
        """선수 record API 실패 시 기본 데이터"""
        return {
            "playerName": player_name,
            "record": {},
            "chart": {},
            "vsTeam": {},
            "basicRecord": {},
            "error": "데이터를 가져올 수 없습니다."
        }
    
    def validate_api_response(self, data: Dict[str, Any]) -> bool:
        """API 응답 구조 검증"""
        try:
            print("🔍 API 응답 구조 검증 중...")
            # 필수 키들이 존재하는지 확인
            required_keys = ['code', 'success', 'result']
            if not all(key in data for key in required_keys):
                print(f"❌ 필수 키 누락: {[key for key in required_keys if key not in data]}")
                return False
            
            if not data.get('success'):
                print("❌ success가 False입니다.")
                return False
                
            result = data.get('result', {})
            if 'previewData' not in result:
                print("❌ previewData 키가 없습니다.")
                return False
                
            print("✅ 모든 필수 키가 존재합니다.")
            return True
            
        except Exception as e:
            print(f"❌ 검증 중 오류 발생: {e}")
            return False
    
    def get_fallback_data(self) -> Dict[str, Any]:
        """API 호출 실패 시 사용할 기본 데이터"""
        print("🔄 기본 데이터를 사용합니다.")
        return {
            "code": 500,
            "success": False,
            "result": {
                "previewData": {
                    "gameInfo": {
                        "hFullName": "한화 이글스",
                        "aFullName": "SSG 랜더스",
                        "stadium": "대전",
                        "gdate": 20250824,
                        "gtime": "18:00"
                    },
                    "awayStandings": {
                        "name": "한화",
                        "rank": 2,
                        "w": 66,
                        "l": 48,
                        "d": 3
                    }
                }
            }
        }
    
    def refresh_data(self) -> Dict[str, Any]:
        """API에서 최신 데이터를 가져와서 업데이트"""
        print("🔄 데이터 새로고침 시작...")
        new_data = self.fetch_api_data()
        if new_data.get('success'):
            self.data = new_data
            print("✅ 데이터가 성공적으로 업데이트되었습니다.")
        else:
            print("❌ 데이터 업데이트에 실패했습니다.")
        
        return self.data
    
    def get_current_data(self) -> Dict[str, Any]:
        """현재 데이터 반환 (AI에게 전달용)"""
        return self.data
    
    def get_today_game_from_schedule(self) -> Dict[str, Any]:
        """game_schedule.json에서 오늘 경기 정보 조회 (API 실패 시 대체용)"""
        try:
            if not os.path.exists(self.schedule_file):
                return None
            
            with open(self.schedule_file, 'r', encoding='utf-8') as f:
                schedule_data = json.load(f)
            
            # 오늘 날짜 정보
            today = datetime.now()
            today_str = today.strftime('%m.%d')
            
            # 오늘 날짜에 맞는 경기 찾기
            for game in schedule_data.get('schedule', []):
                game_date = game.get('date', '')
                if '(' in game_date:
                    date_part = game_date.split('(')[0]
                    if date_part == today_str:
                        return {
                            "success": True,
                            "source": "schedule_file",
                            "game": game
                        }
            
            return None
            
        except Exception as e:
            print(f"❌ 스케줄 파일에서 오늘 경기 조회 오류: {e}")
            return None
    
    def get_game_by_date(self, date_str: str) -> Dict[str, Any]:
        """특정 날짜의 경기 정보 조회 (예: "8월 15일", "08.15")"""
        try:
            if not os.path.exists(self.schedule_file):
                return None
            
            with open(self.schedule_file, 'r', encoding='utf-8') as f:
                schedule_data = json.load(f)
            
            # 날짜 형식 정규화
            normalized_date = self._normalize_date_format(date_str)
            if not normalized_date:
                return None
            
            # 해당 날짜의 경기들 찾기
            games = []
            for game in schedule_data.get('schedule', []):
                game_date = game.get('date', '')
                if '(' in game_date:
                    date_part = game_date.split('(')[0]
                    if date_part == normalized_date:
                        games.append(game)
            
            if games:
                return {
                    "success": True,
                    "source": "schedule_file",
                    "date": date_str,
                    "games": games,
                    "count": len(games)
                }
            else:
                return {
                    "success": False,
                    "message": f"{date_str}에 경기가 없습니다."
                }
            
        except Exception as e:
            print(f"❌ 특정 날짜 경기 조회 오류: {e}")
            return None
    
    def _normalize_date_format(self, date_str: str) -> str:
        """다양한 날짜 형식을 "MM.DD" 형식으로 정규화"""
        try:
            # "8월 15일" -> "08.15"
            if '월' in date_str and '일' in date_str:
                month_match = re.search(r'(\d+)월', date_str)
                day_match = re.search(r'(\d+)일', date_str)
                
                if month_match and day_match:
                    month = int(month_match.group(1))
                    day = int(day_match.group(1))
                    return f"{month:02d}.{day:02d}"
            
            # "08.15" -> "08.15" (이미 정규화된 형식)
            elif re.match(r'\d{2}\.\d{2}', date_str):
                return date_str
            
            # "8.15" -> "08.15"
            elif re.match(r'\d+\.\d+', date_str):
                parts = date_str.split('.')
                if len(parts) == 2:
                    month = int(parts[0])
                    day = int(parts[1])
                    return f"{month:02d}.{day:02d}"
            
            return None
            
        except Exception as e:
            print(f"❌ 날짜 형식 정규화 오류: {e}")
            return None
    
    def get_player_data(self, player_id: str, player_name: str = None) -> Dict[str, Any]:
        """선수 데이터 반환 (AI에게 전달용)"""
        return self.fetch_player_record(player_id, player_name)