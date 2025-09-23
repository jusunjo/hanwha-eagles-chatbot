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
from data.game_preview_service import game_preview_service
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
            # RAG 시스템이 생성한 SQL을 직접 실행
            print(f"🔍 RAG SQL 실행: {sql}")
            
            # SQL에서 SELECT 절 추출
            if "SELECT" in sql.upper():
                # 간단한 SQL 파싱 (WHERE 절만 추출)
                where_clause = ""
                if "WHERE" in sql.upper():
                    where_start = sql.upper().find("WHERE")
                    where_clause = sql[where_start:]
                    # ORDER BY, LIMIT 제거
                    if "ORDER BY" in where_clause.upper():
                        where_clause = where_clause[:where_clause.upper().find("ORDER BY")]
                    if "LIMIT" in where_clause.upper():
                        where_clause = where_clause[:where_clause.upper().find("LIMIT")]
                    where_clause = where_clause.strip()
                
                # Supabase 쿼리 실행
                query = self.supabase.supabase.table("game_schedule").select("*")
                
                # WHERE 절이 있으면 적용
                if where_clause:
                    # 팀명 조건 파싱
                    if "한화" in where_clause:
                        # 한화 홈 경기와 원정 경기를 각각 조회
                        home_games = self.supabase.supabase.table("game_schedule").select("*").eq("home_team_name", "한화").execute()
                        away_games = self.supabase.supabase.table("game_schedule").select("*").eq("away_team_name", "한화").execute()
                        
                        # 날짜 조건 적용
                        if "game_date::date >= CURRENT_DATE" in where_clause:
                            from datetime import datetime
                            today = datetime.now().strftime("%Y-%m-%d")
                            
                            # 홈 경기 필터링
                            home_filtered = [game for game in home_games.data if game.get('game_date', '') >= today]
                            # 원정 경기 필터링
                            away_filtered = [game for game in away_games.data if game.get('game_date', '') >= today]
                            
                            # 결과 합치기
                            all_games = home_filtered + away_filtered
                            # 날짜순 정렬
                            all_games.sort(key=lambda x: x.get('game_date', ''))
                            return all_games
                        else:
                            # 날짜 조건 없이 모든 경기 반환
                            all_games = home_games.data + away_games.data
                            all_games.sort(key=lambda x: x.get('game_date', ''))
                            return all_games
                
                result = query.execute()
                return result.data if result.data else []
            
            # 기본 조회 (SQL이 복잡한 경우)
            result = self.supabase.supabase.table("game_schedule").select("*").execute()
            return result.data if result.data else []
            
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
        
        # 경기 예측 관련 질문
        elif any(keyword in question for keyword in ['이길', '질 것', '예상', '승부', '누가', '어떤 팀', '결과', '예측', '이길거같', '질거같', '승리', '패배']):
            return "경기 예측을 위한 데이터가 부족합니다. 팀명을 포함해서 다시 질문해주세요! 😊"
        
        # 일반적인 경우
        else:
            return "해당 질문에 대한 데이터를 찾을 수 없습니다. 다른 질문을 시도해보세요! 😊"
    
    def process_question(self, question: str) -> str:
        """질문을 RAG 기반 Text-to-SQL로 처리"""
        try:
            print(f"🔍 RAG 기반 Text-to-SQL 처리 시작: {question}")
            print(f"📋 질문 처리 플로우 분석 시작")
            
            # 하루치 경기 일정 질문인지 확인
            is_daily_schedule = self._is_daily_schedule_question(question)
            print(f"🔍 하루치 경기 일정 질문 여부: {is_daily_schedule}")
            
            if is_daily_schedule:
                print(f"🔍 하루치 경기 일정 질문 감지: {question}")
                print(f"📋 플로우: _handle_daily_schedule_question() 실행")
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
                print(f"📋 플로우: _handle_daily_games_analysis() 실행")
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
            
            # 미래 경기 정보 질문인지 확인
            elif self._is_future_game_info_question(question):
                print(f"🔍 미래 경기 정보 질문 감지: {question}")
                print(f"📋 플로우: _handle_future_game_info() 실행")
                return self._handle_future_game_info(question)
            
            # 경기 예측 질문인지 확인
            elif self._is_game_prediction_question(question):
                print(f"🔍 경기 예측 질문 감지: {question}")
                print(f"📋 플로우: _analyze_game_prediction() 실행")
                return self._analyze_game_prediction([], question)
            
            # 경기 분석 질문인지 확인
            elif self._is_game_analysis_question(question):
                print(f"🔍 경기 분석 질문 감지: {question}")
                print(f"📋 플로우: _handle_game_analysis_question() 실행")
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
            
            # RAG 시스템으로 처리
            print(f"📋 플로우: RAG 시스템 (generate_sql -> execute_sql -> analyze_results) 실행")
            
            # SQL 생성
            sql = self.generate_sql(question)
            if not sql:
                print(f"❌ SQL 생성 실패")
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
    
    def _is_future_game_info_question(self, question: str) -> bool:
        """미래 경기 정보 질문인지 판단"""
        question_lower = question.lower()
        
        # 미래 경기 정보 관련 키워드들
        future_info_keywords = [
            '선발투수', '선발', '투수', '라인업', '출전', '선수', '누구', '어디서', '언제', '몇시', 
            '경기장', '상대팀', '내일', '모레', '다음', '이번 주', '다음 주', '앞으로', '예정', 
            '경기 정보', '경기 상세', '경기 세부사항', '경기 시간', '경기 장소', '어느 팀', '어떤 팀'
        ]
        
        # 예측 질문과 구분하기 위한 제외 키워드
        prediction_keywords = ['이길', '질 것', '예상', '승부', '결과', '예측', '이길거같', '질거같', '승리', '패배']
        
        # 예측 키워드가 있으면 미래 경기 정보가 아님
        if any(kw in question_lower for kw in prediction_keywords):
            return False
        
        # 미래 경기 정보 키워드 매칭 확인
        matched_keywords = [kw for kw in future_info_keywords if kw in question_lower]
        if matched_keywords:
            print(f"  🔍 미래 경기 정보 키워드 매칭: {matched_keywords}")
        else:
            print(f"  🔍 미래 경기 정보 키워드 매칭 없음")
        
        return len(matched_keywords) > 0
    
    def _is_game_prediction_question(self, question: str) -> bool:
        """경기 예측 질문인지 판단"""
        question_lower = question.lower()
        
        # 경기 예측 관련 키워드들
        prediction_keywords = [
            '이길', '질 것', '예상', '승부', '누가', '어떤 팀', '결과', '예측', '이길거같', '질거같', 
            '승리', '패배', '누가 이길', '어떤 팀이', '승부 예상', '경기 예상', '이길까', '질까', 
            '승부는', '결과는', '이길 것 같', '질 것 같', '승부 예상', '경기 결과 예상', 
            '누가 이길까', '어떤 팀이 이길까', '경기 승부 예상', '경기 결과 예측'
        ]
        
        # 키워드 매칭 확인
        matched_keywords = [kw for kw in prediction_keywords if kw in question_lower]
        if matched_keywords:
            print(f"  🔍 경기 예측 키워드 매칭: {matched_keywords}")
        else:
            print(f"  🔍 경기 예측 키워드 매칭 없음")
        
        return len(matched_keywords) > 0
    
    def _is_game_analysis_question(self, question: str) -> bool:
        """경기 분석 질문인지 판단 (RAG 기반)"""
        try:
            # 먼저 경기 예측 질문인지 확인 (예측 질문은 분석 질문에서 제외)
            if self._is_game_prediction_question(question):
                return False
            
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
        
        # "다음 경기" 질문은 제외 (RAG 시스템에서 처리)
        if '다음 경기' in question_lower:
            print(f"  🔍 '다음 경기' 키워드 감지 - RAG 시스템으로 전달")
            return False
        
        # 경기 일정 관련 키워드들
        schedule_keywords = [
            '경기 일정', '일정', '스케줄', '예정', '앞으로'
        ]
        
        # 키워드 매칭 확인
        matched_keywords = [kw for kw in schedule_keywords if kw in question_lower]
        if matched_keywords:
            print(f"  🔍 하루치 경기 일정 키워드 매칭: {matched_keywords}")
        else:
            print(f"  🔍 하루치 경기 일정 키워드 매칭 없음")
        
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
                    # 경기 상태 확인 (game_data에서 statusCode 추출)
                    game_data = game_info.get('game_data', {})
                    status_code = game_data.get('statusCode', '0') if isinstance(game_data, dict) else '0'
                    print(f"🔍 경기 {i+1} 상태 코드: {status_code}")
                    
                    # 경기 기록 데이터 가져오기 (모든 경기에 대해 API 호출하여 실제 상태 확인)
                    record_data = await game_record_service.get_game_record(game_id)
                    print(f"🔍 경기 {i+1} API 데이터 수신: {record_data is not None}")
                    
                    # API에서 받은 실제 상태 확인
                    actual_status = "예정"  # 기본값
                    if record_data and isinstance(record_data, dict):
                        result = record_data.get("result", {})
                        if result and result.get("recordData"):
                            # recordData가 있으면 경기가 진행되었거나 종료됨
                            actual_status = "진행완료"
                        else:
                            # recordData가 null이면 예정
                            actual_status = "예정"
                    
                    print(f"🔍 경기 {i+1} 실제 상태: {actual_status}")
                    
                    if record_data and actual_status == "진행완료":
                        # 경기 데이터 분석 (실제로 진행된 경기만)
                        analysis = game_record_service.analyze_game_record(record_data)
                        
                        # 분석 결과 확인
                        if "error" in analysis:
                            print(f"⚠️ 경기 {game_id} 분석 오류: {analysis['error']}")
                            # 오류가 있어도 기본 정보라도 제공
                            summary = self._generate_basic_game_summary(game_info)
                            game_summaries.append(summary)
                            continue
                        
                        # 자연어 요약 생성
                        summary = game_record_service.generate_game_summary(analysis)
                        game_summaries.append(summary)
                    else:
                        # API 데이터가 없거나 경기가 예정인 경우 기본 정보 제공
                        print(f"🔍 경기 {i+1} API 데이터 없음 또는 예정 - 기본 정보로 요약 생성")
                        summary = self._generate_basic_game_summary(game_info)
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
            game_time = game_info.get('time', '')
            
            # game_data에서 statusCode 추출
            game_data = game_info.get('game_data', {})
            status_code = game_data.get('statusCode', '0') if isinstance(game_data, dict) else '0'
            
            # 날짜 포맷팅
            if game_date and len(game_date) >= 10:
                formatted_date = f"{game_date[:4]}년 {game_date[5:7]}월 {game_date[8:10]}일"
            else:
                formatted_date = game_date
            
            # 기본 요약
            summary = f"📅 {formatted_date} {stadium}에서 열린 {away_team} vs {home_team} 경기\n"
            
            # 경기 상태에 따른 처리 (statusCode 기반)
            if status_code == '0':
                # 경기 예정인 경우
                if game_time:
                    summary += f"⏰ 경기 시간: {game_time}\n"
                summary += f"📋 경기가 예정되어 있습니다.\n"
                summary += f"🏟️ 경기장: {stadium}\n"
                summary += f"⚾ {away_team} vs {home_team}의 경기를 기대해주세요!"
                
            elif status_code == '2':
                # 경기 진행 중인 경우
                summary += f"🔥 현재 경기가 진행 중입니다!\n"
                if home_score > 0 or away_score > 0:
                    summary += f"📊 현재 점수: {away_team} {away_score} - {home_score} {home_team}\n"
                summary += f"⚾ 실시간 경기 상황을 확인해보세요!"
                
            elif status_code == '4':
                # 경기 종료된 경우
                if winner == 'HOME':
                    summary += f"🏆 {home_team} {home_score} - {away_score} {away_team}로 승리"
                elif winner == 'AWAY':
                    summary += f"🏆 {away_team} {away_score} - {home_score} {home_team}로 승리"
                else:
                    summary += f"🏆 {away_team} {away_score} - {home_score} {home_team}"
                
                summary += f"\n⚾ 경기 상태: 종료"
                    
            else:
                # 기타 상태
                if home_score > 0 or away_score > 0:
                    summary += f"📊 점수: {away_team} {away_score} - {home_score} {home_team}\n"
                summary += f"📋 경기 정보를 확인해주세요. (상태코드: {status_code})"
            
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
            
            # 경기 상태 확인 (game_data에서 statusCode 추출)
            game_data = game_info.get('game_data', {})
            status_code = game_data.get('statusCode', '0') if isinstance(game_data, dict) else '0'
            print(f"🔍 경기 상태 코드: {status_code}")
            
            # 경기 기록 데이터 가져오기 (모든 경기에 대해 API 호출하여 실제 상태 확인)
            record_data = await game_record_service.get_game_record(game_id)
            print(f"🔍 API 데이터 수신: {record_data is not None}")
            
            # API에서 받은 실제 상태 확인
            actual_status = "예정"  # 기본값
            if record_data and isinstance(record_data, dict):
                result = record_data.get("result", {})
                if result and result.get("recordData"):
                    # recordData가 있으면 경기가 진행되었거나 종료됨
                    actual_status = "진행완료"
                else:
                    # recordData가 null이면 예정
                    actual_status = "예정"
            
            print(f"🔍 실제 경기 상태: {actual_status}")
            
            if record_data and actual_status == "진행완료":
                # 경기 데이터 분석 (실제로 진행된 경기만)
                analysis = game_record_service.analyze_game_record(record_data)
                
                # 분석 결과 확인
                if "error" in analysis:
                    print(f"⚠️ 경기 분석 오류: {analysis['error']}")
                    # 오류가 있어도 기본 정보라도 제공
                    summary = self._generate_basic_game_summary(game_info)
                else:
                    # 자연어 요약 생성
                    summary = game_record_service.generate_game_summary(analysis)
            else:
                # API 데이터가 없거나 경기가 예정인 경우 기본 정보 제공
                print(f"🔍 API 데이터 없음 또는 예정 - 기본 정보로 요약 생성")
                summary = self._generate_basic_game_summary(game_info)
            
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
    
    def _analyze_game_prediction(self, data: list, question: str) -> str:
        """경기 예측 질문에 대한 상대전적 기반 분석"""
        try:
            print(f"🔍 경기 예측 분석 시작: {question}")
            
            # 질문에서 팀명 추출
            team_names = self._extract_team_names_from_question(question)
            
            # 팀명이 명시되지 않은 경우, 미래 경기 확인
            if not team_names:
                print(f"🔍 팀명이 명시되지 않음 - 미래 경기 조회")
                future_games = self._get_future_games(question)
                if not future_games:
                    return "해당 날짜에 경기가 없습니다. 다른 날짜의 경기를 확인해보세요! 😊"
                
                # 미래 경기들에 대한 예측 답변 생성
                return self._generate_future_games_prediction(future_games, question)
            
            # game_schedule에서 해당 팀의 다음 경기 조회
            next_game = self._get_next_game_for_teams(team_names)
            if not next_game:
                return f"{', '.join(team_names)}의 다음 경기 일정을 찾을 수 없습니다."
            
            # 상대팀 정보 추출
            home_team = next_game.get('home_team_name', '')
            away_team = next_game.get('away_team_name', '')
            game_date = next_game.get('game_date', '')
            stadium = next_game.get('stadium', '')
            
            # 상대전적 분석
            prediction_analysis = self._analyze_head_to_head_stats(home_team, away_team)
            
            # 팀별 최근 성적 분석
            home_team_stats = self._get_team_recent_stats(home_team)
            away_team_stats = self._get_team_recent_stats(away_team)
            
            # 예측 답변 생성
            prediction = self._generate_prediction_response(
                home_team, away_team, game_date, stadium, 
                prediction_analysis, home_team_stats, away_team_stats
            )
            
            return prediction
            
        except Exception as e:
            print(f"❌ 경기 예측 분석 오류: {e}")
            return f"경기 예측 분석 중 오류가 발생했습니다: {str(e)}"
    
    def _get_future_games(self, question: str) -> list:
        """미래 경기 조회 (날짜 추출 기반)"""
        try:
            from datetime import datetime, timedelta
            import re
            
            # 질문에서 날짜 정보 추출
            target_date = self._extract_target_date(question)
            
            if not target_date:
                # 날짜가 명시되지 않은 경우, 오늘부터 앞으로 7일간의 경기 조회
                today = datetime.now().strftime("%Y-%m-%d")
                future_date = (datetime.now() + timedelta(days=7)).strftime("%Y-%m-%d")
                print(f"🔍 미래 경기 조회: {today} ~ {future_date}")
                
                # Supabase에서 미래 경기 조회
                result = self.supabase.supabase.table("game_schedule").select("*").gte("game_date", today).lte("game_date", future_date).order("game_date").execute()
            else:
                print(f"🔍 특정 날짜 경기 조회: {target_date}")
                
                # Supabase에서 특정 날짜 경기 조회
                result = self.supabase.supabase.table("game_schedule").select("*").eq("game_date", target_date).execute()
            
            if result.data:
                print(f"✅ 경기 {len(result.data)}개 발견")
                return result.data
            else:
                print(f"❌ 경기 없음")
                return []
                
        except Exception as e:
            print(f"❌ 미래 경기 조회 오류: {e}")
            return []
    
    def _extract_target_date(self, question: str) -> str:
        """질문에서 목표 날짜 추출"""
        try:
            from datetime import datetime, timedelta
            import re
            
            question_lower = question.lower()
            today = datetime.now()
            
            # 상대적 날짜 표현 처리
            if "내일" in question_lower or "tomorrow" in question_lower:
                return (today + timedelta(days=1)).strftime("%Y-%m-%d")
            elif "모레" in question_lower or "day after tomorrow" in question_lower:
                return (today + timedelta(days=2)).strftime("%Y-%m-%d")
            elif "글피" in question_lower:
                return (today + timedelta(days=3)).strftime("%Y-%m-%d")
            elif "다음 주" in question_lower or "next week" in question_lower:
                return (today + timedelta(days=7)).strftime("%Y-%m-%d")
            elif "이번 주" in question_lower or "this week" in question_lower:
                # 이번 주 남은 날들
                return None  # 특정 날짜가 아니므로 None 반환
            elif "앞으로" in question_lower or "앞으로 남은" in question_lower or "upcoming" in question_lower:
                return None  # 앞으로 남은 모든 경기
            elif "오늘" in question_lower or "today" in question_lower:
                return today.strftime("%Y-%m-%d")
            
            # 구체적인 날짜 패턴 찾기 (YYYY-MM-DD, MM/DD, MM월 DD일 등)
            date_patterns = [
                r'(\d{4})-(\d{1,2})-(\d{1,2})',  # 2025-09-25
                r'(\d{1,2})/(\d{1,2})',  # 9/25
                r'(\d{1,2})월\s*(\d{1,2})일',  # 9월 25일
                r'(\d{1,2})일',  # 25일 (이번 달)
            ]
            
            for pattern in date_patterns:
                match = re.search(pattern, question)
                if match:
                    if pattern == r'(\d{4})-(\d{1,2})-(\d{1,2})':
                        year, month, day = match.groups()
                        return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                    elif pattern == r'(\d{1,2})/(\d{1,2})':
                        month, day = match.groups()
                        return f"{today.year}-{month.zfill(2)}-{day.zfill(2)}"
                    elif pattern == r'(\d{1,2})월\s*(\d{1,2})일':
                        month, day = match.groups()
                        return f"{today.year}-{month.zfill(2)}-{day.zfill(2)}"
                    elif pattern == r'(\d{1,2})일':
                        day = match.group(1)
                        return f"{today.year}-{today.month:02d}-{day.zfill(2)}"
            
            return None
            
        except Exception as e:
            print(f"❌ 날짜 추출 오류: {e}")
            return None
    
    def _generate_future_games_prediction(self, games: list, question: str) -> str:
        """미래 경기들에 대한 예측 답변 생성"""
        try:
            if not games:
                return "해당 날짜에 경기가 없습니다."
            
            # 질문에서 날짜 정보 추출하여 제목 생성
            date_title = self._get_date_title(question, games)
            
            predictions = []
            for game in games:
                home_team = game.get('home_team_name', '')
                away_team = game.get('away_team_name', '')
                game_date = game.get('game_date', '')
                stadium = game.get('stadium', '')
                game_id = game.get('game_id', '')
                
                # Game Preview API로 상세 정보 조회
                preview_info = self._get_game_preview_info(game_id)
                
                if preview_info:
                    # 상세한 미리보기 정보로 예측 생성
                    game_prediction = self._generate_detailed_prediction_response(
                        home_team, away_team, game_date, stadium, preview_info
                    )
                else:
                    # 기본 상대전적 분석 (폴백)
                    prediction_analysis = self._analyze_head_to_head_stats(home_team, away_team)
                    home_team_stats = self._get_team_recent_stats(home_team)
                    away_team_stats = self._get_team_recent_stats(away_team)
                    
                    game_prediction = self._generate_prediction_response(
                        home_team, away_team, game_date, stadium, 
                        prediction_analysis, home_team_stats, away_team_stats
                    )
                
                predictions.append(f"🏟️ {home_team} vs {away_team}\n{game_prediction}")
            
            # 전체 답변 구성
            if len(games) == 1:
                return f"📅 {date_title}\n\n{predictions[0]}"
            else:
                return f"📅 {date_title} ({len(games)}경기)\n\n" + "\n\n".join(predictions)
                
        except Exception as e:
            print(f"❌ 미래 경기 예측 생성 오류: {e}")
            return f"경기 예측 생성 중 오류가 발생했습니다: {str(e)}"
    
    def _get_date_title(self, question: str, games: list) -> str:
        """질문과 경기 데이터를 바탕으로 제목 생성"""
        try:
            question_lower = question.lower()
            
            # 상대적 날짜 표현 처리
            if "내일" in question_lower:
                return "내일 경기 예측"
            elif "모레" in question_lower:
                return "모레 경기 예측"
            elif "글피" in question_lower:
                return "글피 경기 예측"
            elif "다음 주" in question_lower:
                return "다음 주 경기 예측"
            elif "이번 주" in question_lower:
                return "이번 주 경기 예측"
            elif "앞으로" in question_lower or "앞으로 남은" in question_lower:
                return "앞으로 남은 경기 예측"
            elif "오늘" in question_lower:
                return "오늘 경기 예측"
            
            # 구체적인 날짜가 있는 경우
            if games:
                first_game_date = games[0].get('game_date', '')
                if first_game_date:
                    from datetime import datetime
                    try:
                        date_obj = datetime.strptime(first_game_date, '%Y-%m-%d')
                        formatted_date = date_obj.strftime('%m월 %d일')
                        return f"{formatted_date} 경기 예측"
                    except:
                        pass
            
            # 기본값
            return "경기 예측"
            
        except Exception as e:
            print(f"❌ 날짜 제목 생성 오류: {e}")
            return "경기 예측"
    
    def _get_game_preview_info(self, game_id: str) -> dict:
        """Game Preview API로 경기 상세 정보 조회"""
        try:
            if not game_id:
                return None
            
            print(f"🔍 Game Preview API 호출: {game_id}")
            
            # 동기적으로 API 호출 (httpx를 동기 모드로 사용)
            import httpx
            
            url = f"https://api-gw.sports.naver.com/schedule/games/{game_id}/preview"
            with httpx.Client() as client:
                response = client.get(url)
                response.raise_for_status()
                
                data = response.json()
                
                if data.get("code") == 200 and data.get("success"):
                    preview_data = data.get("result", {}).get("previewData")
                    if preview_data:
                        print(f"✅ Game Preview 데이터 수신 성공: {game_id}")
                        return game_preview_service.analyze_game_preview(preview_data)
                
                print(f"❌ Game Preview API 실패: {game_id}")
                return None
                
        except Exception as e:
            print(f"❌ Game Preview API 오류: {e}")
            return None
    
    def _generate_detailed_prediction_response(self, home_team: str, away_team: str, 
                                            game_date: str, stadium: str, preview_info: dict) -> str:
        """Game Preview 정보를 활용한 상세 예측 답변 생성"""
        try:
            game_info = preview_info.get("game_info", {})
            team_standings = preview_info.get("team_standings", {})
            starters = preview_info.get("starters", {})
            key_players = preview_info.get("key_players", {})
            season_h2h = preview_info.get("season_head_to_head", {})
            
            # 기본 경기 정보
            response = f"📅 {game_date} {stadium}에서 열리는 {home_team} vs {away_team} 경기 예측\n\n"
            
            # 팀 순위 및 성적
            home_standings = team_standings.get("home", {})
            away_standings = team_standings.get("away", {})
            
            response += f"🏆 팀 순위 및 성적:\n"
            response += f"• {home_team}: {home_standings.get('rank', 'N/A')}위 (승률 {home_standings.get('wra', 'N/A')})\n"
            response += f"• {away_team}: {away_standings.get('rank', 'N/A')}위 (승률 {away_standings.get('wra', 'N/A')})\n\n"
            
            # 선발투수 정보
            home_starter = starters.get("home", {})
            away_starter = starters.get("away", {})
            
            response += f"⚾ 선발투수:\n"
            response += f"• {home_team} - {home_starter.get('name', 'N/A')} (ERA {home_starter.get('era', 'N/A')})\n"
            response += f"• {away_team} - {away_starter.get('name', 'N/A')} (ERA {away_starter.get('era', 'N/A')})\n\n"
            
            # 주요 선수 정보
            home_key_player = key_players.get("home", {})
            away_key_player = key_players.get("away", {})
            
            response += f"🔥 주요 선수:\n"
            response += f"• {home_team} - {home_key_player.get('name', 'N/A')} (타율 {home_key_player.get('hra', 'N/A')})\n"
            response += f"• {away_team} - {away_key_player.get('name', 'N/A')} (타율 {away_key_player.get('hra', 'N/A')})\n\n"
            
            # 시즌 상대전적
            hw = season_h2h.get("home_wins", 0)
            aw = season_h2h.get("away_wins", 0)
            
            response += f"📊 시즌 상대전적:\n"
            response += f"• {home_team} {hw}승 {aw}패 {away_team}\n\n"
            
            # 예측 분석
            response += f"🎯 경기 예상:\n"
            
            # 순위 비교
            home_rank = home_standings.get('rank', 999)
            away_rank = away_standings.get('rank', 999)
            
            if home_rank < away_rank:
                response += f"• {home_team}이 순위상 우세 ({home_rank}위 vs {away_rank}위)\n"
            elif away_rank < home_rank:
                response += f"• {away_team}이 순위상 우세 ({away_rank}위 vs {home_rank}위)\n"
            else:
                response += f"• 양팀 순위가 비슷함 ({home_rank}위 vs {away_rank}위)\n"
            
            # 홈구장 우세
            response += f"• {home_team}의 홈구장 우세\n"
            
            # 선발투수 비교
            home_era = float(home_starter.get('era', 999))
            away_era = float(away_starter.get('era', 999))
            
            if home_era < away_era:
                response += f"• {home_team} 선발투수가 상대적으로 우수 (ERA {home_era} vs {away_era})\n"
            elif away_era < home_era:
                response += f"• {away_team} 선발투수가 상대적으로 우수 (ERA {away_era} vs {home_era})\n"
            
            return response
            
        except Exception as e:
            print(f"❌ 상세 예측 답변 생성 오류: {e}")
            return f"경기 예측 분석 중 오류가 발생했습니다: {str(e)}"
    
    def _handle_future_game_info(self, question: str) -> str:
        """미래 경기 정보 질문 처리"""
        try:
            print(f"🔍 미래 경기 정보 처리 시작: {question}")
            
            # 질문에서 팀명 추출
            team_names = self._extract_team_names_from_question(question)
            
            # 질문에서 날짜 추출
            target_date = self._extract_target_date(question)
            
            # 미래 경기 조회
            if team_names:
                # 특정 팀의 경기 조회
                games = self._get_team_future_games(team_names, target_date)
            else:
                # 모든 미래 경기 조회
                games = self._get_future_games(question)
            
            if not games:
                return "해당 조건에 맞는 경기를 찾을 수 없습니다."
            
            # 질문 유형에 따른 답변 생성
            if any(keyword in question.lower() for keyword in ['선발투수', '선발', '투수']):
                return self._generate_pitcher_info_response(games, question)
            elif any(keyword in question.lower() for keyword in ['라인업', '출전', '선수']):
                return self._generate_lineup_info_response(games, question)
            elif any(keyword in question.lower() for keyword in ['어디서', '경기장', '언제', '몇시', '시간']):
                return self._generate_venue_time_info_response(games, question)
            else:
                return self._generate_general_game_info_response(games, question)
                
        except Exception as e:
            print(f"❌ 미래 경기 정보 처리 오류: {e}")
            return f"미래 경기 정보 처리 중 오류가 발생했습니다: {str(e)}"
    
    def _get_team_future_games(self, team_names: list, target_date: str = None) -> list:
        """특정 팀의 미래 경기 조회"""
        try:
            from datetime import datetime, timedelta
            
            if not target_date:
                today = datetime.now().strftime("%Y-%m-%d")
                future_date = (datetime.now() + timedelta(days=7)).strftime("%Y-%m-%d")
            else:
                today = target_date
                future_date = target_date
            
            games = []
            for team in team_names:
                # 홈 경기 조회
                home_games = self.supabase.supabase.table("game_schedule").select("*").eq("home_team_name", team).gte("game_date", today).lte("game_date", future_date).execute()
                # 원정 경기 조회
                away_games = self.supabase.supabase.table("game_schedule").select("*").eq("away_team_name", team).gte("game_date", today).lte("game_date", future_date).execute()
                
                if home_games.data:
                    games.extend(home_games.data)
                if away_games.data:
                    games.extend(away_games.data)
            
            # 날짜순 정렬
            games.sort(key=lambda x: x.get('game_date', ''))
            return games
            
        except Exception as e:
            print(f"❌ 팀별 미래 경기 조회 오류: {e}")
            return []
    
    def _generate_pitcher_info_response(self, games: list, question: str) -> str:
        """선발투수 정보 답변 생성"""
        try:
            if not games:
                return "해당 조건에 맞는 경기를 찾을 수 없습니다."
            
            responses = []
            for game in games:
                home_team = game.get('home_team_name', '')
                away_team = game.get('away_team_name', '')
                game_date = game.get('game_date', '')
                stadium = game.get('stadium', '')
                game_id = game.get('game_id', '')
                
                # Game Preview API로 선발투수 정보 조회
                preview_info = self._get_game_preview_info(game_id)
                
                if preview_info and preview_info.get('starters'):
                    starters = preview_info['starters']
                    home_starter = starters.get('home', {})
                    away_starter = starters.get('away', {})
                    
                    response = f"⚾ {game_date} {stadium} - {home_team} vs {away_team}\n"
                    response += f"• {home_team} 선발: {home_starter.get('name', '미정')} (등번호 {home_starter.get('backnum', 'N/A')})\n"
                    response += f"• {away_team} 선발: {away_starter.get('name', '미정')} (등번호 {away_starter.get('backnum', 'N/A')})\n"
                    
                    if home_starter.get('era') and home_starter.get('era') != '0.00':
                        response += f"  - {home_starter.get('name', '')} 시즌 성적: {home_starter.get('w', 0)}승 {home_starter.get('l', 0)}패, ERA {home_starter.get('era', 'N/A')}\n"
                    if away_starter.get('era') and away_starter.get('era') != '0.00':
                        response += f"  - {away_starter.get('name', '')} 시즌 성적: {away_starter.get('w', 0)}승 {away_starter.get('l', 0)}패, ERA {away_starter.get('era', 'N/A')}\n"
                else:
                    response = f"⚾ {game_date} {stadium} - {home_team} vs {away_team}\n"
                    response += "• 선발투수 정보를 가져올 수 없습니다.\n"
                
                responses.append(response)
            
            return "\n".join(responses)
            
        except Exception as e:
            print(f"❌ 선발투수 정보 답변 생성 오류: {e}")
            return f"선발투수 정보 조회 중 오류가 발생했습니다: {str(e)}"
    
    def _generate_lineup_info_response(self, games: list, question: str) -> str:
        """라인업 정보 답변 생성"""
        try:
            if not games:
                return "해당 조건에 맞는 경기를 찾을 수 없습니다."
            
            responses = []
            for game in games:
                home_team = game.get('home_team_name', '')
                away_team = game.get('away_team_name', '')
                game_date = game.get('game_date', '')
                stadium = game.get('stadium', '')
                game_id = game.get('game_id', '')
                
                # Game Preview API로 라인업 정보 조회
                preview_info = self._get_game_preview_info(game_id)
                
                if preview_info and preview_info.get('lineups'):
                    lineups = preview_info['lineups']
                    home_lineup = lineups.get('home', [])
                    away_lineup = lineups.get('away', [])
                    
                    response = f"📋 {game_date} {stadium} - {home_team} vs {away_team}\n"
                    
                    if home_lineup:
                        response += f"• {home_team} 라인업:\n"
                        for player in home_lineup[:9]:  # 선발 9명만
                            position = player.get('positionName', 'N/A')
                            name = player.get('playerName', 'N/A')
                            backnum = player.get('backnum', 'N/A')
                            response += f"  {position}: {name} ({backnum}번)\n"
                    
                    if away_lineup:
                        response += f"• {away_team} 라인업:\n"
                        for player in away_lineup[:9]:  # 선발 9명만
                            position = player.get('positionName', 'N/A')
                            name = player.get('playerName', 'N/A')
                            backnum = player.get('backnum', 'N/A')
                            response += f"  {position}: {name} ({backnum}번)\n"
                else:
                    response = f"📋 {game_date} {stadium} - {home_team} vs {away_team}\n"
                    response += "• 라인업 정보를 가져올 수 없습니다.\n"
                
                responses.append(response)
            
            return "\n".join(responses)
            
        except Exception as e:
            print(f"❌ 라인업 정보 답변 생성 오류: {e}")
            return f"라인업 정보 조회 중 오류가 발생했습니다: {str(e)}"
    
    def _generate_venue_time_info_response(self, games: list, question: str) -> str:
        """경기장/시간 정보 답변 생성"""
        try:
            if not games:
                return "해당 조건에 맞는 경기를 찾을 수 없습니다."
            
            responses = []
            for game in games:
                home_team = game.get('home_team_name', '')
                away_team = game.get('away_team_name', '')
                game_date = game.get('game_date', '')
                stadium = game.get('stadium', '')
                game_time = game.get('game_time', '18:30')
                
                response = f"🏟️ {game_date} - {home_team} vs {away_team}\n"
                response += f"• 경기장: {stadium}\n"
                response += f"• 경기시간: {game_time}\n"
                
                responses.append(response)
            
            return "\n".join(responses)
            
        except Exception as e:
            print(f"❌ 경기장/시간 정보 답변 생성 오류: {e}")
            return f"경기장/시간 정보 조회 중 오류가 발생했습니다: {str(e)}"
    
    def _generate_general_game_info_response(self, games: list, question: str) -> str:
        """일반적인 경기 정보 답변 생성"""
        try:
            if not games:
                return "해당 조건에 맞는 경기를 찾을 수 없습니다."
            
            responses = []
            for game in games:
                home_team = game.get('home_team_name', '')
                away_team = game.get('away_team_name', '')
                game_date = game.get('game_date', '')
                stadium = game.get('stadium', '')
                game_time = game.get('game_time', '18:30')
                game_id = game.get('game_id', '')
                
                response = f"📅 {game_date} - {home_team} vs {away_team}\n"
                response += f"• 경기장: {stadium}\n"
                response += f"• 경기시간: {game_time}\n"
                
                # Game Preview API로 추가 정보 조회
                preview_info = self._get_game_preview_info(game_id)
                if preview_info and preview_info.get('starters'):
                    starters = preview_info['starters']
                    home_starter = starters.get('home', {})
                    away_starter = starters.get('away', {})
                    
                    if home_starter.get('name'):
                        response += f"• {home_team} 선발: {home_starter.get('name')}\n"
                    if away_starter.get('name'):
                        response += f"• {away_team} 선발: {away_starter.get('name')}\n"
                
                responses.append(response)
            
            return "\n".join(responses)
            
        except Exception as e:
            print(f"❌ 일반 경기 정보 답변 생성 오류: {e}")
            return f"경기 정보 조회 중 오류가 발생했습니다: {str(e)}"
    
    def _extract_team_names_from_question(self, question: str) -> list:
        """질문에서 팀명 추출"""
        team_names = []
        team_keywords = ['한화', '두산', 'KIA', '키움', '롯데', '삼성', 'SSG', 'KT', 'NC', 'LG']
        
        for team in team_keywords:
            if team in question:
                team_names.append(team)
        
        return team_names
    
    def _get_next_game_for_teams(self, team_names: list) -> dict:
        """해당 팀들의 다음 경기 조회"""
        try:
            from datetime import datetime
            today = datetime.now().strftime("%Y-%m-%d")
            
            for team in team_names:
                # 홈팀으로 참여하는 경기
                home_query = self.supabase.supabase.table("game_schedule").select("*")
                home_query = home_query.eq("home_team_name", team)
                home_query = home_query.gte("game_date", today)
                home_query = home_query.order("game_date").limit(1)
                home_result = home_query.execute()
                
                if home_result.data:
                    return home_result.data[0]
                
                # 원정팀으로 참여하는 경기
                away_query = self.supabase.supabase.table("game_schedule").select("*")
                away_query = away_query.eq("away_team_name", team)
                away_query = away_query.gte("game_date", today)
                away_query = away_query.order("game_date").limit(1)
                away_result = away_query.execute()
                
                if away_result.data:
                    return away_result.data[0]
            
            return None
            
        except Exception as e:
            print(f"❌ 다음 경기 조회 오류: {e}")
            return None
    
    def _analyze_head_to_head_stats(self, home_team: str, away_team: str) -> dict:
        """상대전적 분석"""
        try:
            # game_result 테이블에서 두 팀의 현재 성적 조회
            home_stats = self.supabase.supabase.table("game_result").select("*").eq("team_name", home_team).execute()
            away_stats = self.supabase.supabase.table("game_result").select("*").eq("team_name", away_team).execute()
            
            if not home_stats.data or not away_stats.data:
                return {"error": "팀 통계 데이터를 찾을 수 없습니다."}
            
            home_data = home_stats.data[0]
            away_data = away_stats.data[0]
            
            # 상대전적 분석 결과
            analysis = {
                "home_team": {
                    "name": home_team,
                    "ranking": home_data.get("ranking", 0),
                    "wra": home_data.get("wra", 0.0),
                    "last_five": home_data.get("last_five_games", ""),
                    "offense_ops": home_data.get("offense_ops", 0.0),
                    "defense_era": home_data.get("defense_era", 0.0)
                },
                "away_team": {
                    "name": away_team,
                    "ranking": away_data.get("ranking", 0),
                    "wra": away_data.get("wra", 0.0),
                    "last_five": away_data.get("last_five_games", ""),
                    "offense_ops": away_data.get("offense_ops", 0.0),
                    "defense_era": away_data.get("defense_era", 0.0)
                }
            }
            
            return analysis
            
        except Exception as e:
            print(f"❌ 상대전적 분석 오류: {e}")
            return {"error": f"상대전적 분석 중 오류: {str(e)}"}
    
    def _get_team_recent_stats(self, team_name: str) -> dict:
        """팀의 최근 성적 조회"""
        try:
            result = self.supabase.supabase.table("game_result").select("*").eq("team_name", team_name).execute()
            
            if result.data:
                return result.data[0]
            return {}
            
        except Exception as e:
            print(f"❌ 팀 성적 조회 오류: {e}")
            return {}
    
    def _generate_prediction_response(self, home_team: str, away_team: str, game_date: str, 
                                    stadium: str, prediction_analysis: dict, 
                                    home_team_stats: dict, away_team_stats: dict) -> str:
        """예측 답변 생성"""
        try:
            if "error" in prediction_analysis:
                return f"📅 {game_date} {stadium}에서 열리는 {home_team} vs {away_team} 경기\n\n{prediction_analysis['error']}"
            
            home_data = prediction_analysis["home_team"]
            away_data = prediction_analysis["away_team"]
            
            # 예측 로직 (간단한 비교)
            home_advantage = 0
            if home_data["ranking"] < away_data["ranking"]:  # 순위가 높으면 (숫자가 작으면)
                home_advantage += 1
            if home_data["wra"] > away_data["wra"]:  # 승률이 높으면
                home_advantage += 1
            if home_data["offense_ops"] > away_data["offense_ops"]:  # 공격력이 좋으면
                home_advantage += 1
            if home_data["defense_era"] < away_data["defense_era"]:  # 수비력이 좋으면 (ERA가 낮으면)
                home_advantage += 1
            
            # 최근 5경기 분석
            home_recent = home_data["last_five"].count("W") if home_data["last_five"] else 0
            away_recent = away_data["last_five"].count("W") if away_data["last_five"] else 0
            
            # 예측 결과
            if home_advantage >= 3:
                prediction = f"🏆 {home_team} 승리 예상"
                confidence = "높음"
            elif home_advantage <= 1:
                prediction = f"🏆 {away_team} 승리 예상"
                confidence = "높음"
            else:
                prediction = "⚖️ 접전 예상"
                confidence = "보통"
            
            # 답변 생성
            response = f"""📅 {game_date} {stadium}에서 열리는 {home_team} vs {away_team} 경기 예측

🏟️ 경기 정보:
• 날짜: {game_date}
• 경기장: {stadium}
• 홈팀: {home_team}
• 원정팀: {away_team}

📊 상대전적 분석:
• {home_team}: {home_data['ranking']}위 (승률 {home_data['wra']:.3f})
• {away_team}: {away_data['ranking']}위 (승률 {away_data['wra']:.3f})

⚾ 공격력 비교:
• {home_team} OPS: {home_data['offense_ops']:.3f}
• {away_team} OPS: {away_data['offense_ops']:.3f}

🥎 수비력 비교:
• {home_team} ERA: {home_data['defense_era']:.2f}
• {away_team} ERA: {away_data['defense_era']:.2f}

📈 최근 5경기:
• {home_team}: {home_data['last_five']} ({home_recent}승)
• {away_team}: {away_data['last_five']} ({away_recent}승)

🎯 예측 결과: {prediction} (신뢰도: {confidence})

💡 팁: 실제 경기 결과는 예측과 다를 수 있으니 경기를 직접 관람해보세요!"""
            
            return response
            
        except Exception as e:
            print(f"❌ 예측 답변 생성 오류: {e}")
            return f"예측 답변 생성 중 오류가 발생했습니다: {str(e)}"

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
