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
3. 한국어로 친근하게 답변하세요
4. 야구 팬의 관점에서 재미있게 설명하세요
5. ⚠️ 중요: 데이터베이스에서 조회된 실제 데이터만 사용하세요
6. ⚠️ CRITICAL: 데이터는 이미 정렬되어 있습니다. 절대로 순서를 바꾸지 마세요!
7. ⚠️ 순위 질문의 경우: 데이터의 순서를 그대로 따라가세요 (1번째 데이터 = 1위, 2번째 데이터 = 2위...)
8. ⚠️ 타율/홈런 등 통계 질문의 경우: 데이터의 순서를 정확히 유지하여 답변하세요

답변:"""
            
            response = self.llm.invoke(prompt)
            return response.content
            
        except Exception as e:
            print(f"❌ 결과 분석 오류: {e}")
            return "DB_ERROR: 데이터 분석 중 오류가 발생했습니다."
    
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
    
    def process_question(self, question: str) -> str:
        """질문을 RAG 기반 Text-to-SQL로 처리"""
        try:
            print(f"🔍 RAG 기반 Text-to-SQL 처리 시작: {question}")
            
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
