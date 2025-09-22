#!/usr/bin/env python3
"""
RAG 기반 Text-to-SQL 시스템
동적 스키마 정보를 제공하여 하드코딩된 프롬프트를 제거
"""

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from langchain_openai import ChatOpenAI
from langchain.prompts import ChatPromptTemplate
from data.supabase_client import SupabaseManager
from rag.schema_manager import SchemaManager
from data.game_record_service import game_record_service
import re
import json

class RAGTextToSQL:
    def __init__(self):
        """RAG 기반 Text-to-SQL 초기화"""
        try:
            self.llm = ChatOpenAI(
                model="gpt-4o-mini",
                temperature=0.1,
                api_key=os.getenv("OPENAI_API_KEY")
            )
            self.supabase = SupabaseManager()
            self.schema_manager = SchemaManager()
            
            print("✅ RAG 기반 Text-to-SQL 초기화 완료")
            
        except Exception as e:
            print(f"❌ RAG 기반 Text-to-SQL 초기화 실패: {e}")
            raise e
    
    def generate_sql(self, question: str) -> str:
        """자연어 질문을 SQL로 변환 (RAG 기반)"""
        try:
            # 동적 프롬프트 생성
            prompt = self.schema_manager.generate_dynamic_prompt(question)
            
            # SQL 생성
            response = self.llm.invoke(prompt)
            sql = self._extract_sql(response.content)
            
            print(f"🔍 생성된 SQL: {sql}")
            return sql
            
        except Exception as e:
            print(f"❌ SQL 생성 오류: {e}")
            return ""
    
    def _extract_sql(self, response: str) -> str:
        """응답에서 SQL 추출"""
        # SQL 정리
        sql = re.sub(r'```sql\s*', '', response)
        sql = re.sub(r'```\s*', '', sql)
        sql = sql.strip()
        
        # 설명 텍스트가 포함된 경우 SQL만 추출
        sql_match = re.search(r'SELECT.*?;', sql, re.DOTALL | re.IGNORECASE)
        if sql_match:
            sql = sql_match.group(0).strip()
        
        return sql
    
    def execute_sql(self, sql: str, question: str = "") -> list:
        """SQL 실행"""
        try:
            # 간단한 SELECT 쿼리만 지원
            if not sql.upper().startswith('SELECT'):
                return []
            
            # DB_ERROR 체크
            if "DB_ERROR:" in sql.upper():
                return [sql]
            
            # game_schedule 테이블 조회
            if "game_schedule" in sql.lower():
                return self._get_game_schedule_data(sql, question)
            
            # game_result 테이블 조회
            if "game_result" in sql.lower():
                return self._get_game_result_data(sql)
            
            # 선수 관련 테이블 조회
            if any(table in sql.lower() for table in ['player_season_stats', 'player_game_stats']):
                return self._query_player_data(sql, question)
                
        except Exception as e:
            print(f"❌ 데이터 조회 오류: {e}")
            return ["DB_ERROR: 데이터베이스 조회 중 오류가 발생했습니다."]
    
    def _query_player_data(self, sql: str, question: str = "") -> list:
        """선수 데이터 조회"""
        try:
            # SQL에서 선수명 추출
            player_names = self._extract_player_names_from_sql(sql)
            print(f"🔍 SQL에서 추출된 선수명: {player_names}")
            
            if player_names:
                # 특정 선수명이 있는 경우
                all_data = []
                for player_name in player_names:
                    print(f"🔍 선수 '{player_name}' 데이터 조회 중...")
                    player_data = self.supabase.get_player_complete_data(player_name)
                    if player_data:
                        print(f"✅ 선수 '{player_name}' 데이터 조회 성공")
                        all_data.append(player_data)
                    else:
                        print(f"❌ 선수 '{player_name}' 데이터를 찾을 수 없습니다.")
                return all_data
            else:
                # 선수명이 없는 경우 (팀별 통계 조회 등) - 직접 SQL 실행
                print("🔍 선수명이 없으므로 직접 SQL 실행")
                return self._execute_direct_sql(sql, question)
        except Exception as e:
            print(f"❌ 선수 데이터 조회 오류: {e}")
            return []
    
    def _execute_direct_sql(self, sql: str, question: str = "") -> list:
        """SQL을 직접 실행하여 데이터 조회"""
        try:
            # SQL 파싱하여 WHERE 조건 추출
            where_conditions = self._extract_where_conditions(sql)
            print(f"🔍 추출된 WHERE 조건: {where_conditions}")
            
            # 투수/타자 구분을 위한 필터링
            player_type = self._determine_player_type(sql)
            print(f"🔍 선수 유형: {player_type}")
            
            # player_season_stats 테이블 조회
            query = self.supabase.supabase.table("player_season_stats").select("*")
            
            # WHERE 조건 적용
            for col, val in where_conditions.items():
                query = query.eq(col, val)
            
            # 투수/타자 필터링 적용
            if player_type == "batter":
                # 타자: hra가 NULL이 아닌 선수들
                query = query.not_.is_("hra", "null")
            elif player_type == "pitcher":
                # 투수: era가 NULL이 아닌 선수들
                query = query.not_.is_("era", "null")
            
            # hra 컬럼을 사용하는 모든 질문에 대해 NULL 값 제외
            if "hra" in sql.lower() or "타율" in question:
                print("🔍 hra NULL 값 제외 필터링 적용")
                query = query.not_.is_("hra", "null")
            
            # 규정타석 필터링 적용 (타율 관련 질문인 경우)
            if ("hra" in sql.lower() or "타율" in question) and player_type in ["batter", "both"]:
                # 각 팀별로 규정타석 계산
                team_games = self._get_team_games_count()
                print(f"🔍 팀별 경기 수: {team_games}")
                
                # 타율 질문인 경우 타자만 필터링
                if player_type == "both":
                    query = query.not_.is_("hra", "null")
                
                # 팀별 규정타석 필터링 적용
                if where_conditions.get('team'):
                    # 특정 팀 질문인 경우
                    team = where_conditions['team']
                    if team in team_games:
                        required_pa = int(team_games[team] * 3.1)
                        print(f"🔍 {team} 팀 규정타석 필터링 적용: {required_pa}타석 이상")
                        query = query.gte("ab", required_pa)
                    else:
                        print(f"⚠️ {team} 팀의 경기 수를 찾을 수 없음")
                else:
                    # 모든 팀 질문인 경우 - 평균 경기 수 사용
                    avg_games = sum(team_games.values()) / len(team_games)
                    required_pa = int(avg_games * 3.1)
                    print(f"🔍 전체 팀 평균 규정타석 필터링 적용: {required_pa}타석 이상")
                    query = query.gte("ab", required_pa)
            
            # ORDER BY와 LIMIT 처리 - 일반적인 방식으로 처리
            order_by_match = re.search(r'ORDER BY\s+(\w+)\s+(DESC|ASC)', sql, re.IGNORECASE)
            limit_match = re.search(r'LIMIT\s+(\d+)', sql, re.IGNORECASE)
            
            if order_by_match and limit_match:
                # ORDER BY + LIMIT 조합인 경우: 모든 데이터를 가져와서 정렬 후 제한
                column = order_by_match.group(1).lower()
                direction = order_by_match.group(2).upper()
                limit_count = int(limit_match.group(1))
                
                result = query.execute()
                data = result.data or []
                
                # Python에서 정렬 (NULL 값은 0으로 처리)
                reverse = (direction == 'DESC')
                data = sorted(data, key=lambda x: x.get(column, 0) or 0, reverse=reverse)
                data = data[:limit_count]
            elif order_by_match:
                # ORDER BY만 있는 경우: Supabase ORDER BY 사용
                column = order_by_match.group(1).lower()
                direction = order_by_match.group(2).upper()
                query = query.order(column, desc=(direction == 'DESC'))
                result = query.execute()
                data = result.data or []
            elif limit_match:
                # LIMIT만 있는 경우
                limit_count = int(limit_match.group(1))
                query = query.limit(limit_count)
                result = query.execute()
                data = result.data or []
            else:
                # ORDER BY와 LIMIT이 없는 경우
                result = query.execute()
                data = result.data or []
            
            print(f"✅ 직접 SQL 실행 결과: {len(data)}개")
            if data:
                print(f"🔍 첫 번째 결과: {data[0].get('player_name', 'Unknown')} - 홈런: {data[0].get('hr', 0)}")
            return data
            
        except Exception as e:
            print(f"❌ 직접 SQL 실행 오류: {e}")
            return []
    
    def _extract_where_conditions(self, sql: str) -> dict:
        """SQL에서 WHERE 조건 추출"""
        import re
        conditions = {}
        
        # WHERE 절 찾기
        where_match = re.search(r'WHERE\s+(.+?)(?:\s+ORDER|\s+LIMIT|$)', sql, re.IGNORECASE | re.DOTALL)
        if where_match:
            where_clause = where_match.group(1)
            
            # 각 조건 파싱 (column = 'value' 형태)
            pattern = r"(\w+)\s*=\s*['\"]([^'\"]+)['\"]"
            matches = re.findall(pattern, where_clause)
            
            for col, val in matches:
                conditions[col] = val
        
        return conditions
    
    def _determine_player_type(self, sql: str) -> str:
        """SQL에서 투수/타자 유형 판단"""
        import re
        
        # 투수 관련 키워드 (명확한 투수 전용 키워드만)
        pitcher_keywords = [
            'era', 'w', 'l', 'sv', 'hold', 'cg', 'sho', 'bf', 'inn', 'er', 
            'whip', 'k9', 'bb9', 'kbb', 'qs', 'wra', '투수', '선발', '구원', '마무리'
        ]
        
        # 타자 관련 키워드 (명확한 타자 전용 키워드)
        batter_keywords = [
            'hra', 'hr', 'h2', 'h3', 'rbi', 'ab', 'obp', 'slg', 'ops', 'isop', 
            'babip', 'wrcplus', 'woba', 'wpa', '타자', '타율', '홈런', '타점', 
            '득점', '안타', '타수', '출루율', '장타율'
        ]
        
        # 공통 키워드 (투수와 타자 모두 사용)
        common_keywords = ['run', 'hit', 'bb', 'hp', 'kk']
        
        sql_lower = sql.lower()
        
        # 투수 키워드 체크 (공통 키워드 제외)
        pitcher_score = sum(1 for keyword in pitcher_keywords if keyword in sql_lower)
        
        # 타자 키워드 체크 (공통 키워드 제외)
        batter_score = sum(1 for keyword in batter_keywords if keyword in sql_lower)
        
        # ORDER BY 절에서 컬럼명으로도 판단 (가중치 매우 높게)
        # 테이블 별칭이 있는 경우와 없는 경우 모두 처리
        order_by_match = re.search(r'ORDER BY\s+(?:[\w.]+\.)?(\w+)', sql, re.IGNORECASE)
        if order_by_match:
            column = order_by_match.group(1).lower()
            if column in pitcher_keywords:
                pitcher_score += 10  # ORDER BY는 매우 중요한 단서
            elif column in batter_keywords:
                batter_score += 10  # ORDER BY는 매우 중요한 단서
        
        # SELECT 절에서 컬럼명으로도 판단 (가중치 높게)
        select_match = re.search(r'SELECT\s+(.+?)\s+FROM', sql, re.IGNORECASE | re.DOTALL)
        if select_match:
            select_columns = select_match.group(1).lower()
            for keyword in pitcher_keywords:
                if keyword in select_columns:
                    pitcher_score += 3  # SELECT 절도 중요
            for keyword in batter_keywords:
                if keyword in select_columns:
                    batter_score += 3  # SELECT 절도 중요
        
        
        print(f"🔍 투수 점수: {pitcher_score}, 타자 점수: {batter_score}")
        
        if pitcher_score > batter_score:
            return "pitcher"
        elif batter_score > pitcher_score:
            return "batter"
        else:
            return "both"  # 구분이 어려운 경우
    
    def _extract_player_names_from_sql(self, sql: str) -> list:
        """SQL에서 선수명 추출"""
        import re
        
        # 팀 코드 목록 (선수명이 아닌 것들)
        team_codes = {'HH', 'OB', 'HT', 'WO', 'LT', 'SS', 'SK', 'KT', 'NC', 'LG'}
        
        all_matches = []
        
        # 1. player_name IN ('선수명1', '선수명2') 패턴 찾기
        pattern1 = r"player_name\s+IN\s*\(\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)"
        matches1 = re.findall(pattern1, sql, re.IGNORECASE)
        for match in matches1:
            all_matches.extend(match)
        
        # 2. player_name = '선수명' OR player_name = '선수명' 패턴 찾기
        pattern2 = r"player_name\s*=\s*['\"]([^'\"]+)['\"]"
        matches2 = re.findall(pattern2, sql, re.IGNORECASE)
        all_matches.extend(matches2)
        
        # 3. (p.player_name = '선수명' OR p.player_name = '선수명') 패턴 찾기
        pattern3 = r"p\.player_name\s*=\s*['\"]([^'\"]+)['\"]"
        matches3 = re.findall(pattern3, sql, re.IGNORECASE)
        all_matches.extend(matches3)
        
        print(f"🔍 SQL 패턴 매칭 결과: {all_matches}")
        
        # 팀 코드가 아닌 실제 선수명만 필터링
        player_names = [name for name in all_matches if name.upper() not in team_codes]
        
        # 만약 WHERE 절에서 선수명을 찾지 못했다면, 이는 통계 조회 쿼리이므로 빈 리스트 반환
        # (예: SELECT player_name, hr FROM ... WHERE team = '한화' ORDER BY hr DESC)
        if not player_names:
            print("🔍 WHERE 절에서 선수명을 찾지 못함 - 통계 조회 쿼리로 판단")
        
        return player_names
    
    def _get_team_games_count(self) -> dict:
        """각 팀의 최대 경기 수를 계산"""
        try:
            result = self.supabase.supabase.table("player_season_stats").select("team, gamenum").eq("gyear", "2025").execute()
            
            team_games = {}
            for player in result.data:
                team = player['team']
                gamenum = player['gamenum']
                if team not in team_games or gamenum > team_games[team]:
                    team_games[team] = gamenum
            
            return team_games
        except Exception as e:
            print(f"❌ 팀 경기 수 조회 오류: {e}")
            # 기본값 반환
            return {"한화": 128, "두산": 123, "LG": 128, "NC": 126, "SSG": 125, 
                   "KIA": 117, "KT": 116, "롯데": 130, "삼성": 129, "키움": 130}
    
    def _get_game_schedule_data(self, sql: str, question: str = "") -> list:
        """경기 일정 데이터 조회"""
        try:
            from datetime import datetime, timedelta
            
            # game_schedule 테이블에서 데이터 조회
            result = self.supabase.supabase.table("game_schedule").select("*").execute()
            
            if not result.data:
                return []
            
            # 날짜 필터링
            today = datetime.now()
            today_str = today.strftime("%Y-%m-%d")
            
            # 질문에 따른 날짜 필터링
            if "오늘" in question or "today" in question.lower():
                filtered_games = [game for game in result.data if game.get('game_date') == today_str]
                print(f"📅 오늘({today_str}) 경기 조회: {len(filtered_games)}개")
            elif "내일" in question or "tomorrow" in question.lower():
                tomorrow = today + timedelta(days=1)
                tomorrow_str = tomorrow.strftime("%Y-%m-%d")
                filtered_games = [game for game in result.data if game.get('game_date') == tomorrow_str]
                print(f"📅 내일({tomorrow_str}) 경기 조회: {len(filtered_games)}개")
            else:
                # 기본적으로 최근 7일간의 경기만 조회
                week_ago = today - timedelta(days=7)
                week_ago_str = week_ago.strftime("%Y-%m-%d")
                filtered_games = [
                    game for game in result.data 
                    if game.get('game_date', '') >= week_ago_str and game.get('game_date', '') <= today_str
                ]
                print(f"📅 최근 7일간({week_ago_str} ~ {today_str}) 경기 조회: {len(filtered_games)}개")
            
            # 특정 팀 관련 질문인지 확인
            team_mappings = {
                '한화': 'HH', '두산': 'OB', 'KIA': 'HT', '키움': 'WO',
                '롯데': 'LT', '삼성': 'SS', 'SSG': 'SK', 'KT': 'KT',
                'NC': 'NC', 'LG': 'LG'
            }
            
            # 질문에서 팀명 추출
            mentioned_team = None
            for team_name, team_code in team_mappings.items():
                if team_name in question or team_code.lower() in question.lower():
                    mentioned_team = team_code
                    break
            
            if mentioned_team:
                # 해당 팀 경기만 필터링
                team_games = [
                    game for game in filtered_games 
                    if game.get('home_team_code') == mentioned_team or game.get('away_team_code') == mentioned_team
                ]
                print(f"📅 {mentioned_team} 팀 경기 조회: {len(team_games)}개")
                return team_games
            else:
                return filtered_games
            
        except Exception as e:
            print(f"❌ 경기 일정 조회 오류: {e}")
            return []
    
    def _get_game_result_data(self, sql: str) -> list:
        """팀 순위 및 통계 데이터 조회"""
        try:
            result = self.supabase.supabase.table("game_result").select("*").execute()
            
            if not result.data:
                return []
            
            print(f"📊 팀 순위 및 통계 조회: {len(result.data)}개")
            return result.data
            
        except Exception as e:
            print(f"❌ 팀 순위 및 통계 조회 오류: {e}")
            return []
    
    def analyze_results(self, question: str, data: list) -> str:
        """조회 결과를 분석해서 답변 생성"""
        try:
            print(f"🔍 analyze_results 호출 - 데이터 개수: {len(data) if data else 0}개")
            
            # 실제 데이터 값 로그 출력
            if data:
                print(f"📊 조회된 데이터 내용:")
                for i, item in enumerate(data[:3]):  # 최대 3개만 출력
                    print(f"  [{i+1}] {item}")
                if len(data) > 3:
                    print(f"  ... 외 {len(data)-3}개 더")
            
            if not data:
                print("❌ 데이터가 없어서 상황별 적절한 응답 반환")
                return self._get_no_data_message(question)
            
            # DB 에러 메시지가 포함된 데이터인지 확인
            if isinstance(data, list) and len(data) > 0:
                if isinstance(data[0], str) and data[0].startswith("DB_ERROR:"):
                    print("❌ DB 에러 감지 - 에러 메시지 반환")
                    return data[0]
            
            # 경기 상태 확인 및 분기 처리 (동기 버전)
            game_status_info = self._check_game_status_from_data(data)
            
            # 경기 상태에 따른 다른 처리 (데이터베이스 상태만 사용)
            if game_status_info['is_game_question']:
                if game_status_info['status'] == 'in_progress':
                    return self._generate_in_progress_game_answer(question, data, game_status_info)
                elif game_status_info['status'] == 'finished':
                    return self._generate_finished_game_answer(question, data, game_status_info)
                elif game_status_info['status'] == 'not_started':
                    return self._generate_not_started_game_answer(question, data, game_status_info)
            
            # 데이터를 컨텍스트로 변환
            context = json.dumps(data, ensure_ascii=False, indent=2)
            
            # 분석 프롬프트 생성
            prompt = f"""
당신은 KBO 전문 분석가입니다. 다음 데이터를 바탕으로 사용자의 질문에 답변해주세요.

질문: {question}

데이터 (이미 정렬되어 있음):
{context}

답변 규칙:
1. 데이터를 기반으로 정확한 답변을 제공하세요
2. 구체적인 수치와 선수명을 포함하세요
3. 간결하고 필요한 정보만 제공하세요 (과도한 설명 금지)
4. 줄바꿈을 활용하여 읽기 쉽게 작성하세요
5. **마크다운 문법 사용 금지** (**, *, ~~, # 등 사용하지 마세요)
6. **경기 예측 질문의 경우**: 팀별 최근 성적과 상대 전적을 바탕으로 구체적인 예측을 제공하세요
7. **홈구장 정보**: 롯데는 사직, 한화는 대전, 삼성은 대구, SSG는 문학, KT는 수원, NC는 창원, KIA는 광주, 키움은 고척, 두산/LG는 잠실
8. ⚠️ 중요: 데이터베이스에서 조회된 실제 데이터만 사용하세요
9. ⚠️ CRITICAL: 데이터는 이미 정렬되어 있습니다. 절대로 순서를 바꾸지 마세요!
10. ⚠️ 순위 질문의 경우: 데이터의 순서를 그대로 따라가세요 (1번째 데이터 = 1위, 2번째 데이터 = 2위...)
11. ⚠️ 타율/홈런 등 통계 질문의 경우: 데이터의 순서를 정확히 유지하여 답변하세요

답변:"""
            
            response = self.llm.invoke(prompt)
            return response.content
            
        except Exception as e:
            print(f"❌ 결과 분석 오류: {e}")
            return "DB_ERROR: 데이터 분석 중 오류가 발생했습니다."
    
    def _check_game_status_from_data(self, data: list) -> dict:
        """데이터에서 경기 상태 확인"""
        try:
            if not data or not isinstance(data, list) or len(data) == 0:
                return {'is_game_question': False, 'status': 'unknown'}
            
            # 첫 번째 데이터 확인 (보통 가장 최근 경기)
            first_game = data[0]
            
            # 경기 관련 질문인지 확인 (status_code나 game_id가 있는지)
            is_game_question = (
                'status_code' in first_game or 
                'game_id' in first_game or
                'game_date' in first_game or
                'home_team' in first_game or
                'away_team' in first_game
            )
            
            if not is_game_question:
                return {'is_game_question': False, 'status': 'unknown'}
            
            # 상태 코드 확인
            status_code = first_game.get('status_code', '')
            status_info = first_game.get('status_info', '')
            
            print(f"🔍 경기 상태 확인: status_code={status_code}, status_info={status_info}")
            
            # 상태 분류
            if status_code == 'RESULT' or status_code == '4' or status_code == '3':
                # 경기 종료 (9회말, 9회초 등)
                if '9회' in status_info or status_info == '':
                    return {'is_game_question': True, 'status': 'finished', 'game_data': first_game}
                else:
                    return {'is_game_question': True, 'status': 'finished', 'game_data': first_game}
            
            elif status_code == 'LIVE' or status_code == '2':
                # 경기 진행중
                return {'is_game_question': True, 'status': 'in_progress', 'game_data': first_game}
            
            elif status_code == 'BEFORE' or status_code == '0' or status_code == '1':
                # 경기 시작 전
                return {'is_game_question': True, 'status': 'not_started', 'game_data': first_game}
            
            else:
                # 상태 불명
                return {'is_game_question': True, 'status': 'unknown', 'game_data': first_game}
                
        except Exception as e:
            print(f"❌ 경기 상태 확인 오류: {e}")
            return {'is_game_question': False, 'status': 'unknown'}
    
    def _is_current_game_question(self, question: str) -> bool:
        """현재 진행중인 경기에 대한 질문인지 판단"""
        current_keywords = [
            '지금', '현재', '지금은', '현재는', '지금 몇회', '현재 몇회',
            '지금 점수', '현재 점수', '지금 상황', '현재 상황',
            '지금 어떻게', '현재 어떻게', '지금 어때', '현재 어때'
        ]
        
        question_lower = question.lower()
        return any(keyword in question_lower for keyword in current_keywords)
    
    def _generate_in_progress_game_answer(self, question: str, data: list, status_info: dict) -> str:
        """진행중인 경기에 대한 답변 생성"""
        try:
            game_data = status_info.get('game_data', {})
            question_lower = question.lower()
            
            # 기본 경기 정보
            home_team = game_data.get('home_team_name', game_data.get('home_team', ''))
            away_team = game_data.get('away_team_name', game_data.get('away_team', ''))
            stadium = game_data.get('stadium', '')
            game_date = game_data.get('game_date', '')
            status_info_text = game_data.get('status_info', '')
            
            # API 데이터에서 현재 이닝과 점수 정보 가져오기
            current_inning = status_info.get('current_inning', status_info_text)
            current_score = status_info.get('current_score', {})
            
            # 진행중인 경기임을 명시
            answer = f"🔴 현재 진행중인 경기입니다!\n"
            answer += f"🏟️ {away_team} vs {home_team} ({stadium})\n"
            
            # 현재 이닝 정보
            if current_inning:
                answer += f"⚾ 현재 상황: {current_inning}\n"
            elif status_info_text:
                answer += f"⚾ 현재 상황: {status_info_text}\n"
            
            # 현재 점수 정보
            if current_score:
                # API에서 받은 점수 정보 처리 (hScore: 홈팀, aScore: 원정팀)
                home_score = current_score.get('hScore', current_score.get('home', game_data.get('home_team_score', '0')))
                away_score = current_score.get('aScore', current_score.get('away', game_data.get('away_team_score', '0')))
                answer += f"📊 현재 점수: {away_team} {away_score} - {home_score} {home_team}\n"
            else:
                home_score = game_data.get('home_team_score', '0')
                away_score = game_data.get('away_team_score', '0')
                answer += f"📊 현재 점수: {away_team} {away_score} - {home_score} {home_team}\n"
            
            # 질문 유형별 답변
            if any(keyword in question_lower for keyword in ['선발', '투수']):
                home_starter = game_data.get('home_starter_name', '')
                away_starter = game_data.get('away_starter_name', '')
                
                if home_starter and away_starter:
                    answer += f"🎯 선발 투수: {away_team} {away_starter} vs {home_team} {home_starter}\n"
                else:
                    answer += f"🎯 선발 투수 정보를 확인할 수 없습니다.\n"
            
            elif any(keyword in question_lower for keyword in ['몇회', '몇 회', '이닝']):
                if current_inning:
                    answer += f"⚾ 현재 {current_inning}입니다.\n"
                else:
                    answer += f"⚾ 현재 이닝 정보를 확인할 수 없습니다.\n"
            
            elif any(keyword in question_lower for keyword in ['어떻게', '상황', '현황']):
                answer += f"📈 경기가 현재 진행중이므로 실시간으로 상황이 변할 수 있습니다.\n"
            
            return answer
            
        except Exception as e:
            print(f"❌ 진행중 경기 답변 생성 오류: {e}")
            return "현재 진행중인 경기 정보를 처리하는 중 오류가 발생했습니다."
    
    def _generate_finished_game_answer(self, question: str, data: list, status_info: dict) -> str:
        """종료된 경기에 대한 답변 생성"""
        try:
            game_data = status_info.get('game_data', {})
            
            # 기본 경기 정보
            home_team = game_data.get('home_team_name', game_data.get('home_team', ''))
            away_team = game_data.get('away_team_name', game_data.get('away_team', ''))
            stadium = game_data.get('stadium', '')
            game_date = game_data.get('game_date', '')
            home_score = game_data.get('home_team_score', '0')
            away_score = game_data.get('away_team_score', '0')
            winner = game_data.get('winner', '')
            
            # 경기 결과 요약
            if winner == 'HOME':
                result_text = f"{home_team} {home_score} - {away_score} {away_team}로 승리"
            elif winner == 'AWAY':
                result_text = f"{away_team} {away_score} - {home_score} {home_team}로 승리"
            else:
                result_text = f"{away_team} {away_score} - {home_score} {home_team}"
            
            # 날짜 포맷팅
            if game_date and len(game_date) == 8:
                formatted_date = f"{game_date[:4]}년 {game_date[4:6]}월 {game_date[6:8]}일"
            else:
                formatted_date = game_date
            
            answer = f"📅 {formatted_date} {stadium}에서 열린 경기 결과입니다.\n"
            answer += f"🏆 {result_text}했습니다.\n"
            
            # 질문 유형별 추가 정보
            question_lower = question.lower()
            if any(keyword in question_lower for keyword in ['선발', '투수']):
                home_starter = game_data.get('home_starter_name', '')
                away_starter = game_data.get('away_starter_name', '')
                win_pitcher = game_data.get('win_pitcher_name', '')
                lose_pitcher = game_data.get('lose_pitcher_name', '')
                
                if home_starter and away_starter:
                    answer += f"🎯 선발 투수: {away_team} {away_starter} vs {home_team} {home_starter}\n"
                
                if win_pitcher and lose_pitcher:
                    answer += f"🏅 승리 투수: {win_pitcher}, 패전 투수: {lose_pitcher}\n"
            
            return answer
            
        except Exception as e:
            print(f"❌ 종료된 경기 답변 생성 오류: {e}")
            return "경기 결과 정보를 처리하는 중 오류가 발생했습니다."
    
    def _generate_not_started_game_answer(self, question: str, data: list, status_info: dict) -> str:
        """시작 전 경기에 대한 답변 생성"""
        try:
            game_data = status_info.get('game_data', {})
            
            # 기본 경기 정보
            home_team = game_data.get('home_team_name', game_data.get('home_team', ''))
            away_team = game_data.get('away_team_name', game_data.get('away_team', ''))
            stadium = game_data.get('stadium', '')
            game_date = game_data.get('game_date', '')
            game_time = game_data.get('game_date_time', '')
            
            # 날짜 포맷팅
            if game_date and len(game_date) == 8:
                formatted_date = f"{game_date[:4]}년 {game_date[4:6]}월 {game_date[6:8]}일"
            else:
                formatted_date = game_date
            
            # 시간 포맷팅
            if game_time and len(game_time) >= 16:
                time_part = game_time[11:16]
            else:
                time_part = "시간 미정"
            
            answer = f"⏰ 아직 시작하지 않은 경기입니다.\n"
            answer += f"🏟️ {away_team} vs {home_team} ({stadium})\n"
            answer += f"📅 {formatted_date} {time_part} 예정\n"
            
            # 질문 유형별 답변
            question_lower = question.lower()
            if any(keyword in question_lower for keyword in ['선발', '투수']):
                home_starter = game_data.get('home_starter_name', '')
                away_starter = game_data.get('away_starter_name', '')
                
                if home_starter and away_starter:
                    answer += f"🎯 예정 선발 투수: {away_team} {away_starter} vs {home_team} {home_starter}\n"
                else:
                    answer += f"🎯 선발 투수 정보는 경기 시작 전에 공개됩니다.\n"
            
            return answer
            
        except Exception as e:
            print(f"❌ 시작 전 경기 답변 생성 오류: {e}")
            return "예정된 경기 정보를 처리하는 중 오류가 발생했습니다."
    
    async def _check_live_game_status_from_api(self, game_data: dict) -> dict:
        """API 호출을 통해 실제 경기 상태 확인"""
        try:
            game_id = game_data.get('game_id')
            if not game_id:
                return {'status': 'unknown', 'game_data': game_data}
            
            print(f"🔍 API 호출로 경기 상태 확인: {game_id}")
            
            # game_record_service를 통해 실제 API 호출
            from data.game_record_service import GameRecordService
            game_record_service = GameRecordService()
            
            record_data = await game_record_service.get_game_record(game_id)
            
            if not record_data:
                print(f"❌ API 응답 없음: {game_id}")
                return {'status': 'unknown', 'game_data': game_data}
            
            # API 응답에서 경기 상태 확인
            game_status = self._parse_api_game_status(record_data)
            print(f"🔍 API 상태 확인 결과: {game_status}")
            
            return {
                'status': game_status['status'],
                'game_data': game_data,
                'api_data': record_data,
                'current_inning': game_status.get('current_inning', ''),
                'current_score': game_status.get('current_score', {}),
                'status_info': game_status.get('status_info', '')
            }
            
        except Exception as e:
            print(f"❌ API 경기 상태 확인 오류: {e}")
            return {'status': 'unknown', 'game_data': game_data}
    
    def _parse_api_game_status(self, record_data: dict) -> dict:
        """API 응답에서 경기 상태 파싱"""
        try:
            # result.recordData.games 구조에서 찾기
            if ('result' in record_data and 'recordData' in record_data['result'] and 
                'games' in record_data['result']['recordData'] and 
                len(record_data['result']['recordData']['games']) > 0):
                
                game_info = record_data['result']['recordData']['games'][0]
                
                # 상태 코드 확인
                status_code = game_info.get('statusCode', '')
                current_inning = game_info.get('inn', '')
                score_info = game_info.get('score', {})
                
                print(f"🔍 API 상태 정보: statusCode={status_code}, inn={current_inning}")
                
                # 상태 분류
                if str(status_code) == '2':  # 진행중
                    return {
                        'status': 'in_progress',
                        'current_inning': current_inning,
                        'current_score': score_info,
                        'status_info': current_inning
                    }
                elif str(status_code) in ['3', '4']:  # 종료
                    return {
                        'status': 'finished',
                        'current_inning': current_inning,
                        'current_score': score_info,
                        'status_info': current_inning
                    }
                elif str(status_code) in ['0', '1']:  # 경기 전
                    return {
                        'status': 'not_started',
                        'current_inning': '',
                        'current_score': {},
                        'status_info': '경기 예정'
                    }
                else:
                    return {
                        'status': 'unknown',
                        'current_inning': current_inning,
                        'current_score': score_info,
                        'status_info': current_inning
                    }
            else:
                # recordData가 null인 경우 (경기 시작 전)
                if ('result' in record_data and 
                    'recordData' in record_data['result'] and 
                    record_data['result']['recordData'] is None):
                    return {
                        'status': 'not_started',
                        'current_inning': '',
                        'current_score': {},
                        'status_info': '경기 예정'
                    }
                else:
                    return {
                        'status': 'unknown',
                        'current_inning': '',
                        'current_score': {},
                        'status_info': '상태 불명'
                    }
                    
        except Exception as e:
            print(f"❌ API 상태 파싱 오류: {e}")
            return {
                'status': 'unknown',
                'current_inning': '',
                'current_score': {},
                'status_info': '파싱 오류'
            }
    
    def _get_no_data_message(self, question: str) -> str:
        """질문 유형에 따른 적절한 '데이터 없음' 메시지 반환"""
        question_lower = question.lower()
        
        # 경기 일정 관련 질문
        if any(keyword in question for keyword in ['경기', '일정', '스케줄', '오늘', '내일', '어제']):
            if '오늘' in question:
                return "오늘은 경기가 없습니다. 다른 날짜의 경기를 확인해보세요! 😊"
            elif '내일' in question:
                return "내일은 경기가 없습니다. 다른 날짜의 경기를 확인해보세요! 😊"
            else:
                return "해당 날짜에는 경기 정보가 없습니다. 다른 날짜를 조회해주세요! 😊"
        
        # 선수 관련 질문
        elif any(keyword in question for keyword in ['선수', '선발', '타자', '투수', '성적', '기록', '통계']):
            # 선수명이 포함된 질문인지 확인
            import re
            player_name_pattern = r'[가-힣]{2,4}(?= 선수|의|이|가|은|는)'
            player_matches = re.findall(player_name_pattern, question)
            
            if player_matches:
                player_name = player_matches[0]
                return f"'{player_name}' 선수 정보를 찾을 수 없습니다. 선수 이름을 다시 확인해주세요! 😊"
            else:
                return "해당 조건에 맞는 선수 정보가 없습니다. 다른 조건으로 검색해보세요! 😊"
        
        # 팀 순위/통계 관련 질문
        elif any(keyword in question for keyword in ['순위', '등수', '우승', '포스트시즌', '플레이오프']):
            return "해당 조건의 팀 순위 정보를 찾을 수 없습니다. 다른 조건으로 검색해보세요! 😊"
        
        # 일반적인 경우
        else:
            return "해당 질문에 대한 데이터를 찾을 수 없습니다. 다른 질문을 시도해보세요! 😊"
    
    async def process_question(self, question: str) -> str:
        """질문을 RAG 기반 Text-to-SQL로 처리"""
        try:
            print(f"🔍 RAG 기반 Text-to-SQL 처리 시작: {question}")
            
            # 현재 진행중인 경기 질문인지 확인
            if self._is_current_game_question(question):
                print(f"🔍 현재 진행중인 경기 질문 감지: {question}")
                import asyncio
                import threading
                
                def run_in_thread():
                    # 새로운 스레드에서 새로운 이벤트 루프 실행
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    try:
                        return loop.run_until_complete(self._handle_current_game_question(question))
                    finally:
                        loop.close()
                
                try:
                    # 스레드에서 비동기 함수 실행
                    result = [None]
                    thread = threading.Thread(target=lambda: result.__setitem__(0, run_in_thread()))
                    thread.start()
                    thread.join()
                    return result[0] if result[0] else "현재 경기 정보 처리 중 오류가 발생했습니다."
                except Exception as e:
                    print(f"❌ 비동기 처리 오류: {e}")
                    return "현재 경기 정보 처리 중 오류가 발생했습니다."
            
            # 하루치 경기 일정 질문인지 확인
            elif self._is_daily_schedule_question(question):
                print(f"🔍 하루치 경기 일정 질문 감지: {question}")
                import asyncio
                import threading
                
                def run_in_thread():
                    # 새로운 스레드에서 새로운 이벤트 루프 실행
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    try:
                        return loop.run_until_complete(self._handle_daily_schedule_question(question))
                    finally:
                        loop.close()
                
                try:
                    # 스레드에서 비동기 함수 실행
                    result = [None]
                    thread = threading.Thread(target=lambda: result.__setitem__(0, run_in_thread()))
                    thread.start()
                    thread.join()
                    return result[0] if result[0] else "하루치 경기 일정 처리 중 오류가 발생했습니다."
                except Exception as e:
                    print(f"❌ 비동기 처리 오류: {e}")
                    return "하루치 경기 일정 처리 중 오류가 발생했습니다."
            
            # 하루치 경기 결과 질문인지 확인
            elif self._is_daily_games_question(question):
                print(f"🔍 하루치 경기 결과 질문 감지: {question}")
                import asyncio
                import threading
                
                def run_in_thread():
                    # 새로운 스레드에서 새로운 이벤트 루프 실행
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    try:
                        return loop.run_until_complete(self._handle_daily_games_analysis(question))
                    finally:
                        loop.close()
                
                try:
                    # 스레드에서 비동기 함수 실행
                    result = [None]
                    thread = threading.Thread(target=lambda: result.__setitem__(0, run_in_thread()))
                    thread.start()
                    thread.join()
                    return result[0] if result[0] else "하루치 경기 분석 처리 중 오류가 발생했습니다."
                except Exception as e:
                    print(f"❌ 비동기 처리 오류: {e}")
                    return "하루치 경기 분석 처리 중 오류가 발생했습니다."
            
            # 경기 분석 질문인지 확인
            elif self._is_game_analysis_question(question):
                print(f"🔍 경기 분석 질문 감지: {question}")
                import asyncio
                import threading
                
                def run_in_thread():
                    # 새로운 스레드에서 새로운 이벤트 루프 실행
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    try:
                        return loop.run_until_complete(self._handle_game_analysis_question(question))
                    finally:
                        loop.close()
                
                try:
                    # 스레드에서 비동기 함수 실행
                    result = [None]
                    thread = threading.Thread(target=lambda: result.__setitem__(0, run_in_thread()))
                    thread.start()
                    thread.join()
                    return result[0] if result[0] else "경기 분석 처리 중 오류가 발생했습니다."
                except Exception as e:
                    print(f"❌ 비동기 처리 오류: {e}")
                    return "경기 분석 처리 중 오류가 발생했습니다."
            
            # SQL 생성
            sql = self.generate_sql(question)
            if not sql:
                return "SQL 생성에 실패했습니다."
            
            # SQL 실행
            data = self.execute_sql(sql, question)
            
            # 결과 분석
            answer = self.analyze_results(question, data)
            
            print(f"✅ RAG 기반 Text-to-SQL 처리 완료")
            return answer
            
        except Exception as e:
            print(f"❌ RAG 기반 Text-to-SQL 처리 오류: {e}")
            return f"처리 중 오류가 발생했습니다: {str(e)}"
    
    def _is_game_analysis_question(self, question: str) -> bool:
        """경기 분석 질문인지 판단 (RAG 기반)"""
        try:
            # 스키마 매니저를 통해 관련 정보 검색
            relevant_schema = self.schema_manager.get_relevant_schema(question, top_k=3)
            
            # 질문 유형 확인
            question_types = relevant_schema.get("question_types", [])
            
            # 경기 분석 관련 질문 유형들
            game_analysis_types = [
                "game_analysis", "game_review", "game_summary", 
                "game_result", "game_detail", "game_record"
            ]
            
            # 질문 유형에서 경기 분석 관련 키워드 확인
            for qtype_info in question_types:
                content = qtype_info.get("content", "").lower()
                if any(keyword in content for keyword in ["경기", "game", "결과", "분석", "요약", "리뷰"]):
                    # 날짜 정보도 함께 있는지 확인
                    if self._has_date_reference(question):
                        return True
            
            # 직접적인 경기 분석 키워드 확인 (최소한의 키워드)
            question_lower = question.lower()
            direct_keywords = ["경기 분석", "경기 결과", "경기 요약", "경기 리뷰"]
            
            if any(keyword in question_lower for keyword in direct_keywords):
                return True
            
            # 날짜 + 경기 관련 질문인지 확인
            if self._has_date_reference(question) and self._has_game_reference(question):
                return True
                
            return False
            
        except Exception as e:
            print(f"❌ 경기 분석 질문 판단 오류: {e}")
            return False
    
    def _has_date_reference(self, question: str) -> bool:
        """날짜 참조가 있는지 확인"""
        # 구체적 날짜 패턴
        specific_date_patterns = [
            r'\d{4}년\s*\d{1,2}월\s*\d{1,2}일',
            r'\d{4}-\d{1,2}-\d{1,2}',
            r'\d{1,2}/\d{1,2}',
            r'\d{1,2}월\s*\d{1,2}일'
        ]
        
        # 상대적 날짜 패턴
        relative_date_patterns = [
            '어제', '오늘', '내일', '최근', '지난', '이번', '저번'
        ]
        
        has_specific_date = any(re.search(pattern, question) for pattern in specific_date_patterns)
        has_relative_date = any(pattern in question.lower() for pattern in relative_date_patterns)
        
        return has_specific_date or has_relative_date
    
    def _has_game_reference(self, question: str) -> bool:
        """경기 관련 참조가 있는지 확인"""
        question_lower = question.lower()
        game_keywords = [
            '경기', '게임', '승부', '결과', '스코어', '점수',
            '승리', '패배', '무승부', '어땠어', '어땠나', '어떻게'
        ]
        
        return any(keyword in question_lower for keyword in game_keywords)
    
    def _is_daily_games_question(self, question: str) -> bool:
        """하루치 모든 경기 결과를 요청하는 질문인지 판단"""
        question_lower = question.lower()
        
        # 경기 결과 관련 키워드들 (과거 경기 결과)
        result_keywords = [
            '경기 결과', '경기들', '모든 경기', '전체 경기', '그날 경기',
            '경기 현황', '경기 상황', '오늘의 경기', '어제의 경기', 
            '경기 요약', '어땠어', '어땠나', '어떻게 됐', '분석'
        ]
        
        # 경기 일정 관련 키워드들 (미래 경기 일정)
        schedule_keywords = [
            '경기 일정', '일정', '스케줄', '예정', '앞으로', '다음', '내일의 경기'
        ]
        
        # 특정 팀이 언급되지 않은 경우
        team_keywords = [
            '한화', '두산', 'KIA', '키움', '롯데', '삼성', 'SSG', 'KT', 'NC', 'LG',
            '이글스', '베어스', '타이거즈', '히어로즈', '자이언츠', '라이온즈',
            '랜더스', '위즈', '다이노스', '트윈스'
        ]
        
        has_result_keyword = any(keyword in question_lower for keyword in result_keywords)
        has_schedule_keyword = any(keyword in question_lower for keyword in schedule_keywords)
        has_team_keyword = any(keyword in question_lower for keyword in team_keywords)
        
        # 경기 결과 키워드가 있고, 일정 키워드가 없으며, 특정 팀이 언급되지 않은 경우
        return has_result_keyword and not has_schedule_keyword and not has_team_keyword
    
    def _is_daily_schedule_question(self, question: str) -> bool:
        """하루치 경기 일정을 요청하는 질문인지 판단"""
        question_lower = question.lower()
        
        # 경기 일정 관련 키워드들
        schedule_keywords = [
            '경기 일정', '일정', '스케줄', '예정', '앞으로', '다음'
        ]
        
        # 특정 팀이 언급되지 않은 경우
        team_keywords = [
            '한화', '두산', 'KIA', '키움', '롯데', '삼성', 'SSG', 'KT', 'NC', 'LG',
            '이글스', '베어스', '타이거즈', '히어로즈', '자이언츠', '라이온즈',
            '랜더스', '위즈', '다이노스', '트윈스'
        ]
        
        has_schedule_keyword = any(keyword in question_lower for keyword in schedule_keywords)
        has_team_keyword = any(keyword in question_lower for keyword in team_keywords)
        
        # 일정 키워드가 있고 특정 팀이 언급되지 않은 경우
        return has_schedule_keyword and not has_team_keyword
    
    async def _handle_current_game_question(self, question: str) -> str:
        """현재 진행중인 경기 질문 처리"""
        try:
            print(f"🔍 현재 진행중인 경기 질문 처리 시작: {question}")
            
            # 질문에서 팀 정보 추출
            team_info = self._extract_team_from_question(question)
            print(f"🔍 추출된 팀: {team_info}")
            
            # 오늘 날짜로 경기 조회
            from datetime import datetime
            today = datetime.now().strftime("%Y-%m-%d")
            
            # 해당 팀의 오늘 경기 조회
            if team_info:
                team_code_mapping = {
                    '한화': 'HH', '두산': 'OB', 'KIA': 'HT', '키움': 'WO',
                    '롯데': 'LT', '삼성': 'SS', 'SSG': 'SK', 'KT': 'KT',
                    'NC': 'NC', 'LG': 'LG'
                }
                team_code = team_code_mapping.get(team_info, team_info)
                
                # 홈팀 또는 원정팀 조건으로 조회
                home_query = self.supabase.supabase.table("game_schedule").select("*").eq("game_date", today).eq("home_team_code", team_code)
                home_result = home_query.execute()
                
                away_query = self.supabase.supabase.table("game_schedule").select("*").eq("game_date", today).eq("away_team_code", team_code)
                away_result = away_query.execute()
                
                # 결과 합치기
                games = []
                if home_result.data:
                    games.extend(home_result.data)
                if away_result.data:
                    games.extend(away_result.data)
                
                if not games:
                    return f"오늘 {team_info} 팀의 경기가 없습니다."
                
                # 가장 최근 경기 선택 (시간순으로 정렬)
                games.sort(key=lambda x: x.get('game_date_time', ''), reverse=True)
                current_game = games[0]
            else:
                # 팀이 지정되지 않은 경우 오늘의 모든 경기 중 진행중인 경기 찾기
                all_games_query = self.supabase.supabase.table("game_schedule").select("*").eq("game_date", today)
                all_games_result = all_games_query.execute()
                
                if not all_games_result.data:
                    return "오늘 경기가 없습니다."
                
                # 진행중인 경기 찾기 (LIVE 상태)
                live_games = [game for game in all_games_result.data if game.get('status_code') == 'LIVE']
                
                if not live_games:
                    return "현재 진행중인 경기가 없습니다."
                
                current_game = live_games[0]
            
            # 경기 상태 확인 및 API 호출
            game_id = current_game.get('game_id')
            if game_id:
                # API를 통해 실제 경기 상태 확인
                api_status = await self._check_live_game_status_from_api(current_game)
                
                if api_status['status'] == 'in_progress':
                    return self._generate_in_progress_game_answer(question, [current_game], api_status)
                elif api_status['status'] == 'finished':
                    return self._generate_finished_game_answer(question, [current_game], api_status)
                elif api_status['status'] == 'not_started':
                    return self._generate_not_started_game_answer(question, [current_game], api_status)
            
            # API 호출 실패 시 데이터베이스 상태로 처리
            game_status_info = self._check_game_status_from_data([current_game])
            
            if game_status_info['is_game_question']:
                if game_status_info['status'] == 'in_progress':
                    return self._generate_in_progress_game_answer(question, [current_game], game_status_info)
                elif game_status_info['status'] == 'finished':
                    return self._generate_finished_game_answer(question, [current_game], game_status_info)
                elif game_status_info['status'] == 'not_started':
                    return self._generate_not_started_game_answer(question, [current_game], game_status_info)
            
            return "현재 경기 정보를 확인할 수 없습니다."
                
        except Exception as e:
            print(f"❌ 현재 경기 질문 처리 오류: {e}")
            return f"현재 경기 정보 처리 중 오류가 발생했습니다: {str(e)}"
    
    async def _handle_daily_schedule_question(self, question: str) -> str:
        """하루치 경기 일정 처리"""
        try:
            print(f"🔍 하루치 경기 일정 질문 처리 시작: {question}")
            
            # 하루치 경기 일정 조회
            daily_games = await self._find_daily_games_via_sql(question)
            
            if not daily_games:
                return "해당 날짜의 경기 일정을 찾을 수 없습니다."
            
            print(f"🔍 조회된 경기 일정 수: {len(daily_games)}개")
            
            # 경기 일정 요약 생성
            schedule_summary = self._generate_daily_schedule_summary(daily_games)
            
            print(f"✅ 하루치 경기 일정 처리 완료: {len(daily_games)}개 경기")
            return schedule_summary
                
        except Exception as e:
            print(f"❌ 하루치 경기 일정 처리 오류: {e}")
            return f"경기 일정 처리 중 오류가 발생했습니다: {str(e)}"
    
    def _generate_daily_schedule_summary(self, daily_games: list) -> str:
        """하루치 경기 일정 요약 생성"""
        try:
            if not daily_games:
                return "경기 일정이 없습니다."
            
            # 첫 번째 경기의 날짜 사용
            first_game_date = daily_games[0].get('game_date', '')
            if first_game_date and len(first_game_date) >= 10:
                formatted_date = f"{first_game_date[:4]}년 {first_game_date[5:7]}월 {first_game_date[8:10]}일"
            else:
                formatted_date = first_game_date
            
            # 전체 일정 시작
            summary = f"📅 {formatted_date} KBO 경기 일정 ({len(daily_games)}경기)\n"
            summary += "=" * 50 + "\n\n"
            
            # 각 경기 일정 추가
            for i, game in enumerate(daily_games, 1):
                home_team = game.get('home_team_name', '')
                away_team = game.get('away_team_name', '')
                stadium = game.get('stadium', '')
                game_time = game.get('game_date_time', '')
                status_code = game.get('status_code', '')
                
                # 시간 포맷팅
                if game_time and len(game_time) >= 16:
                    # 2025-09-22T18:30:00+00:00 -> 18:30
                    time_part = game_time[11:16]
                else:
                    time_part = "시간 미정"
                
                summary += f"🏟️ 경기 {i}: {away_team} vs {home_team}\n"
                summary += f"   📍 경기장: {stadium}\n"
                summary += f"   ⏰ 경기시간: {time_part}\n"
                
                # 경기 상태에 따른 추가 정보
                if status_code == 'BEFORE':
                    summary += f"   📋 상태: 예정\n"
                elif status_code == 'LIVE':
                    summary += f"   📋 상태: 진행중\n"
                elif status_code == 'RESULT':
                    summary += f"   📋 상태: 종료\n"
                else:
                    summary += f"   📋 상태: {status_code}\n"
                
                summary += "\n"
            
            return summary
            
        except Exception as e:
            print(f"❌ 하루치 일정 요약 생성 오류: {e}")
            return f"{len(daily_games)}개 경기가 예정되어 있습니다."
    
    async def _handle_daily_games_analysis(self, question: str) -> str:
        """하루치 모든 경기 분석 처리"""
        try:
            print(f"🔍 하루치 경기 분석 질문 처리 시작: {question}")
            
            # 하루치 모든 경기 정보 조회
            daily_games = await self._find_daily_games_via_sql(question)
            
            if not daily_games:
                return "해당 날짜의 경기 정보를 찾을 수 없습니다."
            
            print(f"🔍 조회된 경기 수: {len(daily_games)}개")
            
            # 각 경기에 대해 분석 수행
            game_summaries = []
            
            for i, game_info in enumerate(daily_games):
                game_id = game_info.get('game_id')
                if not game_id:
                    continue
                
                print(f"🔍 경기 {i+1}/{len(daily_games)} 분석 중: {game_id}")
                
                try:
                    # 경기 기록 데이터 가져오기
                    record_data = await game_record_service.get_game_record(game_id)
                    
                    if not record_data:
                        # API 데이터가 없는 경우 기본 정보만 사용
                        summary = self._generate_basic_game_summary(game_info)
                        game_summaries.append(summary)
                        continue
                    
                    # 경기 데이터 분석
                    analysis = game_record_service.analyze_game_record(record_data)
                    
                    # 자연어 요약 생성
                    summary = game_record_service.generate_game_summary(analysis)
                    game_summaries.append(summary)
                    
                except Exception as e:
                    print(f"❌ 경기 {game_id} 분석 오류: {e}")
                    # 오류 발생 시 기본 정보라도 제공
                    summary = self._generate_basic_game_summary(game_info)
                    game_summaries.append(summary)
            
            # 전체 요약 생성
            if game_summaries:
                final_summary = self._generate_daily_summary(daily_games, game_summaries)
                print(f"✅ 하루치 경기 분석 완료: {len(daily_games)}개 경기")
                return final_summary
            else:
                return "경기 분석 중 오류가 발생했습니다."
                
        except Exception as e:
            print(f"❌ 하루치 경기 분석 오류: {e}")
            return f"경기 분석 중 오류가 발생했습니다: {str(e)}"
    
    def _generate_basic_game_summary(self, game_info: dict) -> str:
        """기본 경기 정보로 요약 생성"""
        try:
            game_date = game_info.get('game_date', '')
            home_team = game_info.get('home_team_name', '')
            away_team = game_info.get('away_team_name', '')
            stadium = game_info.get('stadium', '')
            home_score = game_info.get('home_team_score', 0)
            away_score = game_info.get('away_team_score', 0)
            winner = game_info.get('winner', '')
            status_info = game_info.get('status_info', '')
            
            # 날짜 포맷팅
            if game_date and len(game_date) >= 10:
                formatted_date = f"{game_date[:4]}년 {game_date[5:7]}월 {game_date[8:10]}일"
            else:
                formatted_date = game_date
            
            # 기본 요약
            summary = f"📅 {formatted_date} {stadium}에서 열린 {away_team} vs {home_team} 경기\n"
            
            # 승부 결과
            if winner == 'HOME':
                summary += f"🏆 {home_team} {home_score} - {away_score} {away_team}로 승리"
            elif winner == 'AWAY':
                summary += f"🏆 {away_team} {away_score} - {home_score} {home_team}로 승리"
            else:
                summary += f"🏆 {away_team} {away_score} - {home_score} {home_team}"
            
            if status_info:
                summary += f"\n⚾ 경기 상황: {status_info}"
            
            return summary
            
        except Exception as e:
            print(f"❌ 기본 경기 요약 생성 오류: {e}")
            return f"경기 정보: {game_info.get('home_team_name', '')} vs {game_info.get('away_team_name', '')}"
    
    def _generate_daily_summary(self, daily_games: list, game_summaries: list) -> str:
        """하루치 경기 전체 요약 생성"""
        try:
            if not daily_games:
                return "경기 정보가 없습니다."
            
            # 첫 번째 경기의 날짜 사용
            first_game_date = daily_games[0].get('game_date', '')
            if first_game_date and len(first_game_date) >= 10:
                formatted_date = f"{first_game_date[:4]}년 {first_game_date[5:7]}월 {first_game_date[8:10]}일"
            else:
                formatted_date = first_game_date
            
            # 전체 요약 시작
            summary = f"📅 {formatted_date} KBO 경기 결과 ({len(daily_games)}경기)\n"
            summary += "=" * 50 + "\n\n"
            
            # 각 경기 요약 추가
            for i, game_summary in enumerate(game_summaries, 1):
                summary += f"🏟️ 경기 {i}:\n"
                summary += game_summary + "\n\n"
            
            # 간단한 통계 추가
            home_wins = sum(1 for game in daily_games if game.get('winner') == 'HOME')
            away_wins = sum(1 for game in daily_games if game.get('winner') == 'AWAY')
            
            summary += f"📊 경기 결과 요약:\n"
            summary += f"   홈팀 승리: {home_wins}경기\n"
            summary += f"   원정팀 승리: {away_wins}경기\n"
            
            return summary
            
        except Exception as e:
            print(f"❌ 하루치 요약 생성 오류: {e}")
            return f"{len(daily_games)}개 경기가 있었습니다."
    
    async def _handle_game_analysis_question(self, question: str) -> str:
        """경기 분석 질문 처리"""
        try:
            print(f"🔍 경기 분석 질문 처리 시작: {question}")
            
            # SQL을 통해 경기 정보 조회
            game_info = await self._find_game_info_via_sql(question)
            
            if not game_info:
                return "해당 조건에 맞는 경기 정보를 찾을 수 없습니다."
            
            game_id = game_info.get('game_id')
            if not game_id:
                return "경기 ID를 찾을 수 없습니다."
            
            print(f"🔍 찾은 게임 ID: {game_id}")
            
            # 경기 기록 데이터 가져오기
            record_data = await game_record_service.get_game_record(game_id)
            
            if not record_data:
                return f"경기 기록 데이터를 가져올 수 없습니다."
            
            # API 호출을 통한 실제 경기 상태 확인
            api_game_status = self._parse_api_game_status(record_data)
            
            # 경기 상태에 따른 다른 처리
            if api_game_status['status'] == 'in_progress':
                # 진행중인 경기에 대한 답변
                return self._generate_in_progress_game_answer(question, [game_info], {
                    'status': 'in_progress',
                    'game_data': game_info,
                    'api_data': record_data,
                    'current_inning': api_game_status.get('current_inning', ''),
                    'current_score': api_game_status.get('current_score', {}),
                    'status_info': api_game_status.get('status_info', '')
                })
            elif api_game_status['status'] == 'finished':
                # 종료된 경기에 대한 답변
                return self._generate_finished_game_answer(question, [game_info], {
                    'status': 'finished',
                    'game_data': game_info,
                    'api_data': record_data,
                    'current_inning': api_game_status.get('current_inning', ''),
                    'current_score': api_game_status.get('current_score', {}),
                    'status_info': api_game_status.get('status_info', '')
                })
            elif api_game_status['status'] == 'not_started':
                # 시작 전 경기에 대한 답변
                return self._generate_not_started_game_answer(question, [game_info], {
                    'status': 'not_started',
                    'game_data': game_info,
                    'api_data': record_data,
                    'current_inning': '',
                    'current_score': {},
                    'status_info': '경기 예정'
                })
            else:
                # 기존 방식으로 처리 (API 상태 불명인 경우)
                analysis = game_record_service.analyze_game_record(record_data)
                summary = game_record_service.generate_game_summary(analysis)
                print(f"✅ 경기 분석 완료")
                return summary
            
        except Exception as e:
            print(f"❌ 경기 분석 처리 오류: {e}")
            return f"경기 분석 중 오류가 발생했습니다: {str(e)}"
    
    def _extract_date_from_question(self, question: str) -> str:
        """질문에서 날짜 정보 추출"""
        # YYYY년 MM월 DD일 패턴
        pattern1 = r'(\d{4})년\s*(\d{1,2})월\s*(\d{1,2})일'
        match1 = re.search(pattern1, question)
        if match1:
            year, month, day = match1.groups()
            return f"{year}{month.zfill(2)}{day.zfill(2)}"
        
        # YYYY-MM-DD 패턴
        pattern2 = r'(\d{4})-(\d{1,2})-(\d{1,2})'
        match2 = re.search(pattern2, question)
        if match2:
            year, month, day = match2.groups()
            return f"{year}{month.zfill(2)}{day.zfill(2)}"
        
        # MM/DD 패턴 (올해 기준)
        pattern3 = r'(\d{1,2})/(\d{1,2})'
        match3 = re.search(pattern3, question)
        if match3:
            from datetime import datetime
            month, day = match3.groups()
            current_year = datetime.now().year
            return f"{current_year}{month.zfill(2)}{day.zfill(2)}"
        
        # MM월 DD일 패턴 (올해 기준)
        pattern4 = r'(\d{1,2})월\s*(\d{1,2})일'
        match4 = re.search(pattern4, question)
        if match4:
            from datetime import datetime
            month, day = match4.groups()
            current_year = datetime.now().year
            return f"{current_year}{month.zfill(2)}{day.zfill(2)}"
        
        return None
    
    def _extract_team_from_question(self, question: str) -> str:
        """질문에서 팀 정보 추출"""
        team_mappings = {
            '한화': '한화', '한화이글스': '한화', '이글스': '한화',
            '두산': '두산', '두산베어스': '두산', '베어스': '두산',
            'KIA': 'KIA', 'KIA타이거즈': 'KIA', '타이거즈': 'KIA',
            '키움': '키움', '키움히어로즈': '키움', '히어로즈': '키움',
            '롯데': '롯데', '롯데자이언츠': '롯데', '자이언츠': '롯데',
            '삼성': '삼성', '삼성라이온즈': '삼성', '라이온즈': '삼성',
            'SSG': 'SSG', 'SSG랜더스': 'SSG', '랜더스': 'SSG',
            'KT': 'KT', 'KT위즈': 'KT', '위즈': 'KT',
            'NC': 'NC', 'NC다이노스': 'NC', '다이노스': 'NC',
            'LG': 'LG', 'LG트윈스': 'LG', '트윈스': 'LG'
        }
        
        for team_keyword, team_name in team_mappings.items():
            if team_keyword in question:
                return team_name
        
        return None
    
    async def _find_game_info_via_sql(self, question: str) -> dict:
        """SQL을 통해 경기 정보 조회"""
        try:
            from datetime import datetime, timedelta
            
            # 질문에서 날짜와 팀 정보 추출
            date_info = self._extract_date_from_question(question)
            team_info = self._extract_team_from_question(question)
            
            print(f"🔍 추출된 날짜: {date_info}")
            print(f"🔍 추출된 팀: {team_info}")
            
            # 상대적 날짜 처리
            if not date_info:
                date_info = self._extract_relative_date(question)
                print(f"🔍 상대적 날짜 추출 결과: {date_info}")
            
            # SQL 쿼리 구성
            query = self.supabase.supabase.table("game_schedule").select("*")
            
            # 날짜 조건 추가 (있는 경우에만) - 실제 컬럼명은 game_date
            if date_info:
                # YYYYMMDD 형식을 YYYY-MM-DD 형식으로 변환
                if len(date_info) == 8:
                    formatted_date = f"{date_info[:4]}-{date_info[4:6]}-{date_info[6:8]}"
                    query = query.eq("game_date", formatted_date)
                else:
                    query = query.eq("game_date", date_info)
            
            # 팀 조건 추가
            if team_info:
                # 팀 코드 매핑
                team_code_mapping = {
                    '한화': 'HH', '두산': 'OB', 'KIA': 'HT', '키움': 'WO',
                    '롯데': 'LT', '삼성': 'SS', 'SSG': 'SK', 'KT': 'KT',
                    'NC': 'NC', 'LG': 'LG'
                }
                
                team_code = team_code_mapping.get(team_info, team_info)
                # Supabase OR 조건을 두 개의 쿼리로 분리하여 처리
                # 먼저 홈팀 조건으로 조회
                home_query = self.supabase.supabase.table("game_schedule").select("*")
                if date_info:
                    # YYYYMMDD 형식을 YYYY-MM-DD 형식으로 변환
                    if len(date_info) == 8:
                        formatted_date = f"{date_info[:4]}-{date_info[4:6]}-{date_info[6:8]}"
                        home_query = home_query.eq("game_date", formatted_date)
                    else:
                        home_query = home_query.eq("game_date", date_info)
                home_query = home_query.eq("home_team_code", team_code).order("game_date", desc=True).limit(1)
                home_result = home_query.execute()
                
                if home_result.data:
                    return home_result.data[0]
                
                # 홈팀 조건에서 결과가 없으면 원정팀 조건으로 조회
                away_query = self.supabase.supabase.table("game_schedule").select("*")
                if date_info:
                    # YYYYMMDD 형식을 YYYY-MM-DD 형식으로 변환
                    if len(date_info) == 8:
                        formatted_date = f"{date_info[:4]}-{date_info[4:6]}-{date_info[6:8]}"
                        away_query = away_query.eq("game_date", formatted_date)
                    else:
                        away_query = away_query.eq("game_date", date_info)
                away_query = away_query.eq("away_team_code", team_code).order("game_date", desc=True).limit(1)
                away_result = away_query.execute()
                
                if away_result.data:
                    return away_result.data[0]
                
                return None
            
            # 최신 경기 우선 정렬
            query = query.order("game_date", desc=True).limit(1)
            
            result = query.execute()
            
            if result.data and len(result.data) > 0:
                return result.data[0]
            
            # 날짜 정보가 없는 경우 최근 경기 조회 시도
            if not date_info and team_info:
                print("🔍 날짜 정보가 없어서 최근 경기 조회 시도")
                return await self._find_recent_games_without_date(team_info)
            
            return None
            
        except Exception as e:
            print(f"❌ SQL 기반 경기 정보 조회 오류: {e}")
            return None
    
    async def _find_daily_games_via_sql(self, question: str) -> list:
        """SQL을 통해 하루치 모든 경기 정보 조회"""
        try:
            from datetime import datetime, timedelta
            
            # 질문에서 날짜와 팀 정보 추출
            date_info = self._extract_date_from_question(question)
            team_info = self._extract_team_from_question(question)
            
            print(f"🔍 추출된 날짜: {date_info}")
            print(f"🔍 추출된 팀: {team_info}")
            
            # 상대적 날짜 처리 (날짜가 없는 경우)
            if not date_info:
                relative_date = self._extract_relative_date(question)
                if relative_date:
                    date_info = relative_date
                    print(f"🔍 상대적 날짜 추출 결과: {date_info}")
            
            # 날짜가 없으면 최근 경기 날짜 조회
            if not date_info:
                # 가장 최근 경기 날짜 조회
                recent_query = self.supabase.supabase.table("game_schedule").select("game_date").order("game_date", desc=True).limit(1)
                recent_result = recent_query.execute()
                if recent_result.data:
                    date_info = recent_result.data[0]['game_date']
                    print(f"🔍 최근 경기 날짜: {date_info}")
            
            if not date_info:
                print("❌ 조회할 날짜를 찾을 수 없습니다.")
                return []
            
            # SQL 쿼리 구성 - 해당 날짜의 모든 경기
            query = self.supabase.supabase.table("game_schedule").select("*")
            
            # 날짜 조건 추가 - 실제 컬럼명은 game_date
            if len(date_info) == 8:  # YYYYMMDD 형식
                formatted_date = f"{date_info[:4]}-{date_info[4:6]}-{date_info[6:8]}"
                query = query.eq("game_date", formatted_date)
            else:  # YYYY-MM-DD 형식
                query = query.eq("game_date", date_info)
            
            # 팀 조건이 있는 경우 필터링
            if team_info:
                team_code_mapping = {
                    '한화': 'HH', '두산': 'OB', 'KIA': 'HT', '키움': 'WO',
                    '롯데': 'LT', '삼성': 'SS', 'SSG': 'SK', 'KT': 'KT',
                    'NC': 'NC', 'LG': 'LG'
                }
                team_code = team_code_mapping.get(team_info, team_info)
                
                # 홈팀 또는 원정팀 조건으로 필터링 - 두 개의 쿼리로 분리
                # 먼저 홈팀 조건으로 조회
                home_query = self.supabase.supabase.table("game_schedule").select("*")
                if len(date_info) == 8:  # YYYYMMDD 형식
                    formatted_date = f"{date_info[:4]}-{date_info[4:6]}-{date_info[6:8]}"
                    home_query = home_query.eq("game_date", formatted_date)
                else:  # YYYY-MM-DD 형식
                    home_query = home_query.eq("game_date", date_info)
                home_query = home_query.eq("home_team_code", team_code).order("game_date_time")
                home_result = home_query.execute()
                
                # 원정팀 조건으로 조회
                away_query = self.supabase.supabase.table("game_schedule").select("*")
                if len(date_info) == 8:  # YYYYMMDD 형식
                    formatted_date = f"{date_info[:4]}-{date_info[4:6]}-{date_info[6:8]}"
                    away_query = away_query.eq("game_date", formatted_date)
                else:  # YYYY-MM-DD 형식
                    away_query = away_query.eq("game_date", date_info)
                away_query = away_query.eq("away_team_code", team_code).order("game_date_time")
                away_result = away_query.execute()
                
                # 결과 합치기
                all_games = []
                if home_result.data:
                    all_games.extend(home_result.data)
                if away_result.data:
                    all_games.extend(away_result.data)
                
                # 중복 제거 (game_id 기준)
                seen_ids = set()
                unique_games = []
                for game in all_games:
                    game_id = game.get('game_id')
                    if game_id and game_id not in seen_ids:
                        seen_ids.add(game_id)
                        unique_games.append(game)
                
                return unique_games
            
            # 시간 순으로 정렬
            result = query.order("game_date_time").execute()
            
            if result.data:
                print(f"✅ {date_info} 날짜 경기 {len(result.data)}개 조회 성공")
                return result.data
            else:
                print(f"❌ {date_info} 날짜에 경기를 찾을 수 없음")
                return []
                
        except Exception as e:
            print(f"❌ 하루치 경기 정보 조회 오류: {e}")
            return []
    
    def _extract_relative_date(self, question: str) -> str:
        """질문에서 상대적 날짜 추출 (YYYY-MM-DD 형식)"""
        from datetime import datetime, timedelta
        
        question_lower = question.lower()
        
        if '어제' in question_lower:
            yesterday = datetime.now() - timedelta(days=1)
            return yesterday.strftime("%Y-%m-%d")
        elif '오늘' in question_lower:
            today = datetime.now()
            return today.strftime("%Y-%m-%d")
        elif '내일' in question_lower:
            tomorrow = datetime.now() + timedelta(days=1)
            return tomorrow.strftime("%Y-%m-%d")
        elif '최근' in question_lower or '지난' in question_lower:
            # 최근 7일 내의 경기 중 가장 최근 경기
            recent_date = datetime.now() - timedelta(days=1)
            return recent_date.strftime("%Y-%m-%d")
        
        return None
    
    async def _find_recent_games_without_date(self, team_info: str = None) -> dict:
        """날짜 정보가 없는 경우 최근 경기 조회"""
        try:
            query = self.supabase.supabase.table("game_schedule").select("*")
            
            # 팀 조건 추가
            if team_info:
                team_code_mapping = {
                    '한화': 'HH', '두산': 'OB', 'KIA': 'HT', '키움': 'WO',
                    '롯데': 'LT', '삼성': 'SS', 'SSG': 'SK', 'KT': 'KT',
                    'NC': 'NC', 'LG': 'LG'
                }
                
                team_code = team_code_mapping.get(team_info, team_info)
                # Supabase OR 조건을 두 개의 쿼리로 분리하여 처리
                # 먼저 홈팀 조건으로 조회
                home_query = self.supabase.supabase.table("game_schedule").select("*")
                home_query = home_query.eq("home_team_code", team_code).order("game_date", desc=True).limit(1)
                home_result = home_query.execute()
                
                if home_result.data:
                    return home_result.data[0]
                
                # 홈팀 조건에서 결과가 없으면 원정팀 조건으로 조회
                away_query = self.supabase.supabase.table("game_schedule").select("*")
                away_query = away_query.eq("away_team_code", team_code).order("game_date", desc=True).limit(1)
                away_result = away_query.execute()
                
                if away_result.data:
                    return away_result.data[0]
                
                return None
            
            # 최신 경기 우선 정렬 (날짜 내림차순)
            query = query.order("game_date", desc=True).limit(1)
            
            result = query.execute()
            
            if result.data and len(result.data) > 0:
                return result.data[0]
            
            return None
            
        except Exception as e:
            print(f"❌ 최근 경기 조회 오류: {e}")
            return None

def main():
    """테스트 함수"""
    try:
        rag_text_to_sql = RAGTextToSQL()
        
        # 테스트 질문들
        test_questions = [
            "한화 마지막 우승년도",
            "한화 올해 몇등이야?",
            "문동주 선수 성적이 어때?",
            "오늘 경기 일정",
            "두산 투수 중에 가장 잘하는 투수가 누구야?"
        ]
        
        for question in test_questions:
            print(f"\n{'='*50}")
            print(f"질문: {question}")
            answer = rag_text_to_sql.process_question(question)
            print(f"답변: {answer}")
            
    except Exception as e:
        print(f"❌ 테스트 실패: {e}")

if __name__ == "__main__":
    main()
