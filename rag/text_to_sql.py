#!/usr/bin/env python3
"""
새로운 정규화된 테이블 구조를 사용하는 KBO 챗봇 Text-to-SQL 기능 구현
"""

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from langchain_openai import ChatOpenAI
from langchain.prompts import ChatPromptTemplate
from data.supabase_client import SupabaseManager
import re
import json

class TextToSQL:
    def __init__(self):
        """Text-to-SQL 초기화"""
        try:
            self.llm = ChatOpenAI(
                model="gpt-4o-mini",
                temperature=0.1,
                api_key=os.getenv("OPENAI_API_KEY")
            )
            self.supabase = SupabaseManager()
            print("✅ Text-to-SQL 초기화 완료")
            
        except Exception as e:
            print(f"❌ Text-to-SQL 초기화 실패: {e}")
            raise e
    
    def should_use_text_to_sql(self, question: str) -> bool:
        """Text-to-SQL을 사용해야 하는 질문인지 판단"""
        # 복합 질문 키워드들
        complex_keywords = [
            "가장", "최고", "1위", "순위", "비교", "평균", "총합", "합계",
            "상위", "하위", "최대", "최소", "최고", "최저", "평균",
            "투수 중에", "타자 중에", "선수 중에", "팀 중에",
            "어떤", "누가", "몇 명", "얼마나", "어느"
        ]
        
        # 경기 일정 관련 키워드들
        schedule_keywords = [
            "경기 일정", "일정", "경기", "내일", "오늘", "어제", "다음", "이번 주",
            "경기표", "스케줄", "대진표", "경기 시간", "경기장", "구장",
            "누구랑", "누구와", "vs", "대", "상대", "상대팀", "경기 상대"
        ]
        
        # 경기 결과 관련 키워드들
        result_keywords = [
            "경기 결과", "결과", "승부", "이겼", "졌", "무승부", "승리", "패배",
            "점수", "스코어", "승수", "패수", "몇승", "몇패"
        ]
        
        # 팀 통계 관련 키워드들
        team_stats_keywords = [
            "몇승", "승수", "승리수", "몇패", "패수", "패배수", "승률", "순위",
            "이번 시즌", "시즌", "현재", "지금", "몇위", "등수", "랭킹",
            "타율", "홈런", "타점", "안타", "출루율", "장타율", "OPS",
            "ERA", "WHIP", "세이브", "홀드", "완투", "퀄리티스타트"
        ]
        
        return (any(keyword in question for keyword in complex_keywords) or
                any(keyword in question for keyword in schedule_keywords) or
                any(keyword in question for keyword in result_keywords) or
                any(keyword in question for keyword in team_stats_keywords))
    
    def generate_sql(self, question: str) -> str:
        """자연어 질문을 SQL로 변환"""
        try:
            prompt = ChatPromptTemplate.from_template("""
당신은 KBO 데이터베이스 전문가입니다. 사용자의 질문을 SQL 쿼리로 변환해주세요.

⚠️ 중요한 규칙 ⚠️
1. 팀명을 팀 코드로 변환하세요:
   - "한화" → "HH" (절대 "한화" 문자열 사용 금지)
   - "두산" → "OB", "KIA" → "HT", "키움" → "WO"
   - "롯데" → "LT", "삼성" → "SS", "SSG" → "SK"
   - "KT" → "KT", "NC" → "NC", "LG" → "LG"

2. 선수명은 그대로 사용하세요:
   - "문동주", "이정후", "김하성" 등 선수명은 팀 코드로 변환하지 마세요
   - 선수명은 players.player_name에서 직접 검색

3. 타율 필드명:
   - 타율은 "hra" 필드만 사용 (절대 "avg", "battingAverage" 사용 금지)

4. 새로운 정규화된 테이블 구조:
   - players: id, player_name, pcode, team, position
   - player_season_stats: player_id, player_name, gyear, team, hra, hr, rbi, era, w, l, kk, whip 등
   - player_game_stats: player_id, player_name, gameId, gday, opponent, hra, hr, rbi, era, w, l 등
   - game_schedule: date, home_team, away_team, home_team_code, away_team_code, stadium, time

질문: {question}

올바른 SQL 예시:
한화 타자 순위 조회:
SELECT p.player_name, p.team, s.hra, s.hr, s.rbi 
FROM players p
JOIN player_season_stats s ON p.id = s.player_id
WHERE p.team = 'HH' AND s.gyear = '2025'
ORDER BY s.hra DESC
LIMIT 5;

특정 선수 성적 조회 (문동주):
SELECT p.player_name, s.hra, s.hr, s.rbi, s.ab
FROM players p
JOIN player_season_stats s ON p.id = s.player_id
WHERE p.player_name = '문동주' AND s.gyear = '2025';

투수 ERA 순위 조회:
SELECT p.player_name, p.team, s.era, s.w, s.l, s.kk
FROM players p
JOIN player_season_stats s ON p.id = s.player_id
WHERE s.gyear = '2025' AND s.era IS NOT NULL
ORDER BY s.era ASC
LIMIT 10;

내일 경기 일정 조회:
SELECT date, home_team, away_team, stadium, time
FROM game_schedule 
WHERE date = '2025-01-15'
ORDER BY time;

한화 내일 경기 상대 조회:
SELECT home_team, away_team, stadium, time
FROM game_schedule 
WHERE date = '2025-01-15' 
AND (home_team_code = 'HH' OR away_team_code = 'HH');

SQL:""")
            
            response = self.llm.invoke(prompt.format(question=question))
            sql = response.content.strip()
            
            # SQL 정리
            sql = re.sub(r'```sql\s*', '', sql)
            sql = re.sub(r'```\s*', '', sql)
            sql = sql.strip()
            
            # 설명 텍스트가 포함된 경우 SQL만 추출
            sql_match = re.search(r'SELECT.*?;', sql, re.DOTALL | re.IGNORECASE)
            if sql_match:
                sql = sql_match.group(0).strip()
            
            # 잘못된 필드명 자동 수정
            sql = re.sub(r'battingAverage', 'hra', sql, flags=re.IGNORECASE)
            sql = re.sub(r'\bavg\b', 'hra', sql, flags=re.IGNORECASE)
            
            # 정규식으로 팀명 수정 (더 강력함)
            team_mappings = {
                "'한화'": "'HH'", "'두산'": "'OB'", "'KIA'": "'HT'", "'키움'": "'WO'",
                "'롯데'": "'LT'", "'삼성'": "'SS'", "'SSG'": "'SK'", "'KT'": "'KT'",
                "'NC'": "'NC'", "'LG'": "'LG'",
                # 따옴표 없는 경우도 처리
                "한화": "HH", "두산": "OB", "KIA": "HT", "키움": "WO",
                "롯데": "LT", "삼성": "SS", "SSG": "SK", "KT": "KT",
                "NC": "NC", "LG": "LG"
            }
            
            print(f"🔧 SQL 수정 전: {sql}")
            
            for wrong_team, correct_team in team_mappings.items():
                if wrong_team in sql:
                    sql = sql.replace(wrong_team, correct_team)
                    print(f"🔧 팀명 수정: {wrong_team} → {correct_team}")
            
            print(f"🔧 SQL 수정 후: {sql}")
            
            print(f"🔍 생성된 SQL: {sql}")
            return sql
            
        except Exception as e:
            print(f"❌ SQL 생성 오류: {e}")
            return ""
    
    def execute_sql(self, sql: str) -> list:
        """SQL 실행 (새로운 테이블 구조 기반)"""
        try:
            # 간단한 SELECT 쿼리만 지원
            if not sql.upper().startswith('SELECT'):
                return []
            
            # game_schedule 테이블 조회
            if "game_schedule" in sql.lower():
                return self._get_game_schedule_data(sql)
            
            # 새로운 테이블 구조 기반 데이터 조회
            return self._query_normalized_tables(sql)
                
        except Exception as e:
            print(f"❌ 데이터 조회 오류: {e}")
            return []
    
    def _query_normalized_tables(self, sql: str) -> list:
        """정규화된 테이블에서 데이터 조회"""
        try:
            # SQL에서 테이블과 조건 파악
            sql_lower = sql.lower()
            
            # 선수 관련 질문인지 확인
            if any(table in sql_lower for table in ['players', 'player_season_stats', 'player_game_stats']):
                return self._query_player_data(sql)
            
            # 경기 일정 관련 질문
            if 'game_schedule' in sql_lower:
                return self._get_game_schedule_data(sql)
            
            return []
            
        except Exception as e:
            print(f"❌ 정규화된 테이블 조회 오류: {e}")
            return []
    
    def _query_player_data(self, sql: str) -> list:
        """선수 데이터 조회"""
        try:
            sql_lower = sql.lower()
            
            # 특정 선수명이 포함된 경우
            player_names = self._extract_player_names_from_sql(sql)
            if player_names:
                return self._get_specific_players_data(player_names)
            
            # 팀별 선수 조회
            team_code = self._extract_team_code_from_sql(sql)
            if team_code:
                return self._get_team_players_data(team_code)
            
            # 포지션별 선수 조회
            position = self._extract_position_from_sql(sql)
            if position:
                return self._get_position_players_data(position)
            
            # 통계 기준 상위 선수 조회
            stat_field = self._extract_stat_field_from_sql(sql)
            if stat_field:
                return self._get_top_players_by_stat(stat_field, sql)
            
            # 기본: 모든 선수 조회
            return self._get_all_players_data()
            
        except Exception as e:
            print(f"❌ 선수 데이터 조회 오류: {e}")
            return []
    
    def _extract_player_names_from_sql(self, sql: str) -> list:
        """SQL에서 선수명 추출"""
        # 일반적인 선수명들
        common_players = [
            "문동주", "이정후", "김하성", "류현진", "오승환", "최지만", 
            "박건우", "김현수", "양의지", "김재환", "이승엽", "박병호",
            "강백호", "이정후", "김하성", "문동주", "류현진", "오승환"
        ]
        
        found_players = []
        for player in common_players:
            if player in sql:
                found_players.append(player)
        
        return found_players
    
    def _extract_team_code_from_sql(self, sql: str) -> str:
        """SQL에서 팀 코드 추출"""
        team_mappings = {
            "한화": "HH", "두산": "OB", "KIA": "HT", "키움": "WO",
            "롯데": "LT", "삼성": "SS", "SSG": "SK", "KT": "KT",
            "NC": "NC", "LG": "LG"
        }
        
        for team_name, team_code in team_mappings.items():
            if team_name in sql or f"'{team_code}'" in sql:
                return team_code
        
        return None
    
    def _extract_position_from_sql(self, sql: str) -> str:
        """SQL에서 포지션 추출"""
        if "투수" in sql or "pitcher" in sql.lower():
            return "투수"
        elif "타자" in sql or "hitter" in sql.lower():
            return "타자"
        elif "포수" in sql or "catcher" in sql.lower():
            return "포수"
        
        return None
    
    def _extract_stat_field_from_sql(self, sql: str) -> str:
        """SQL에서 통계 필드 추출"""
        stat_mappings = {
            "타율": "hra", "홈런": "hr", "타점": "rbi", "안타": "hit",
            "출루율": "obp", "장타율": "slg", "OPS": "ops",
            "ERA": "era", "WHIP": "whip", "승수": "w", "패수": "l",
            "삼진": "kk", "세이브": "sv", "홀드": "hold"
        }
        
        for stat_name, stat_field in stat_mappings.items():
            if stat_name in sql or stat_field in sql.lower():
                return stat_field
        
        return None
    
    def _get_specific_players_data(self, player_names: list) -> list:
        """특정 선수들의 데이터 조회"""
        try:
            all_data = []
            for player_name in player_names:
                player_data = self.supabase.get_player_complete_data(player_name)
                if player_data:
                    all_data.append(player_data)
            return all_data
        except Exception as e:
            print(f"❌ 특정 선수 데이터 조회 오류: {e}")
            return []
    
    def _get_team_players_data(self, team_code: str) -> list:
        """팀별 선수 데이터 조회"""
        try:
            players = self.supabase.get_players_by_team(team_code)
            all_data = []
            for player in players:
                player_data = self.supabase.get_player_complete_data(player['player_name'])
                if player_data:
                    all_data.append(player_data)
            return all_data
        except Exception as e:
            print(f"❌ 팀별 선수 데이터 조회 오류: {e}")
            return []
    
    def _get_position_players_data(self, position: str) -> list:
        """포지션별 선수 데이터 조회"""
        try:
            players = self.supabase.get_players_by_position(position)
            all_data = []
            for player in players:
                player_data = self.supabase.get_player_complete_data(player['player_name'])
                if player_data:
                    all_data.append(player_data)
            return all_data
        except Exception as e:
            print(f"❌ 포지션별 선수 데이터 조회 오류: {e}")
            return []
    
    def _get_top_players_by_stat(self, stat_field: str, sql: str) -> list:
        """통계 기준 상위 선수 조회"""
        try:
            # SQL에서 포지션과 팀 필터 추출
            position = self._extract_position_from_sql(sql)
            team_code = self._extract_team_code_from_sql(sql)
            
            # 상위 10명 조회
            top_players = self.supabase.get_top_players_by_stat(
                stat_field=stat_field,
                position=position,
                team=team_code,
                limit=10
            )
            
            # 완전한 데이터로 변환
            all_data = []
            for player_stat in top_players:
                if 'players' in player_stat:
                    player_name = player_stat['players']['player_name']
                    player_data = self.supabase.get_player_complete_data(player_name)
                    if player_data:
                        all_data.append(player_data)
            
            return all_data
        except Exception as e:
            print(f"❌ 상위 선수 조회 오류: {e}")
            return []
    
    def _get_all_players_data(self) -> list:
        """모든 선수 데이터 조회"""
        try:
            players = self.supabase.get_all_players()
            all_data = []
            for player in players[:50]:  # 최대 50명만
                player_data = self.supabase.get_player_complete_data(player['player_name'])
                if player_data:
                    all_data.append(player_data)
            return all_data
        except Exception as e:
            print(f"❌ 모든 선수 데이터 조회 오류: {e}")
            return []
    
    def _get_game_schedule_data(self, sql: str) -> list:
        """경기 일정 데이터 조회"""
        try:
            from datetime import datetime, date, timedelta
            
            # game_schedule 테이블에서 데이터 조회
            result = self.supabase.supabase.table("game_schedule").select("*").execute()
            
            if not result.data:
                return []
            
            # 내일 경기 필터링
            tomorrow = date.today() + timedelta(days=1)
            tomorrow_str = tomorrow.strftime("%Y-%m-%d")
            
            # 내일 경기만 필터링
            filtered_games = [
                game for game in result.data 
                if game.get('date', '').startswith(tomorrow_str)
            ]
            
            # 한화 관련 질문인지 확인
            is_hanwha_question = any(keyword in sql.lower() for keyword in ['한화', 'hh', '누구랑', '누구와', '상대'])
            
            if is_hanwha_question:
                # 한화 경기만 필터링
                hanwha_games = [
                    game for game in filtered_games 
                    if game.get('home_team_code') == 'HH' or game.get('away_team_code') == 'HH'
                ]
                print(f"📅 내일 한화 경기 조회: {len(hanwha_games)}개")
                return hanwha_games
            else:
                print(f"📅 내일 경기 일정 조회: {len(filtered_games)}개")
                return filtered_games
            
        except Exception as e:
            print(f"❌ 경기 일정 조회 오류: {e}")
            return []
    
    def analyze_results(self, question: str, data: list) -> str:
        """조회 결과를 분석해서 답변 생성"""
        try:
            print(f"🔍 analyze_results 호출 - 데이터 개수: {len(data) if data else 0}개")
            
            if not data:
                print("❌ 데이터가 없어서 '해당 질문에 대한 데이터를 찾을 수 없습니다.' 반환")
                return "해당 질문에 대한 데이터를 찾을 수 없습니다."
            
            # 데이터를 컨텍스트로 변환
            context = json.dumps(data, ensure_ascii=False, indent=2)
            
            # 질문 유형별 프롬프트 생성
            prompt = self._create_analysis_prompt(question, context)
            
            response = self.llm.invoke(prompt)
            return response.content
            
        except Exception as e:
            print(f"❌ 결과 분석 오류: {e}")
            return f"데이터 분석 중 오류가 발생했습니다: {str(e)}"
    
    def _create_analysis_prompt(self, question: str, context: str) -> str:
        """질문 유형에 따른 분석 프롬프트 생성"""
        # 경기 일정 관련 질문인지 확인
        is_schedule_question = any(keyword in question for keyword in [
            "경기 일정", "일정", "경기", "내일", "오늘", "어제", "다음", "이번 주",
            "경기표", "스케줄", "대진표", "경기 시간", "경기장", "구장"
        ])
        
        # 선수 성적 관련 질문인지 확인
        is_player_stats_question = any(keyword in question for keyword in [
            "성적", "어때", "어떻게", "요즘", "최근", "지금", "현재",
            "투수", "타자", "선수", "선발", "구원", "마무리", "순위", "최고", "가장"
        ])
        
        if is_schedule_question:
            return f"""
당신은 KBO 전문 분석가입니다. 다음 경기 일정 데이터를 바탕으로 사용자의 질문에 답변해주세요.

질문: {question}

경기 일정 데이터:
{context}

답변 규칙:
1. 경기 일정을 명확하고 읽기 쉽게 정리해서 보여주세요
2. 경기 시간, 경기장, 홈팀 vs 원정팀 정보를 포함하세요
3. 한국어로 친근하게 답변하세요
4. 야구 팬이 쉽게 이해할 수 있도록 설명하세요

답변:"""
        
        elif is_player_stats_question:
            return f"""
당신은 KBO 전문 분석가입니다. 다음 선수 성적 데이터를 바탕으로 사용자의 질문에 답변해주세요.

질문: {question}

선수 성적 데이터:
{context}

답변 규칙:
1. 선수의 성적을 명확하고 읽기 쉽게 정리해서 보여주세요
2. 2025년 시즌 성적을 우선적으로 보여주세요
3. 구체적인 수치(타율, 홈런, 타점, ERA, 승수, 패수 등)를 포함하세요
4. 한국어로 친근하게 답변하세요
5. 야구 팬이 쉽게 이해할 수 있도록 설명하세요
6. 순위나 비교 질문인 경우 명확한 순위를 제시하세요

답변:"""
        
        else:
            return f"""
당신은 KBO 전문 분석가입니다. 다음 데이터를 바탕으로 사용자의 질문에 답변해주세요.

질문: {question}

데이터:
{context}

답변 규칙:
1. 데이터를 기반으로 정확한 답변을 제공하세요
2. 구체적인 수치와 선수명을 포함하세요
3. 한국어로 친근하게 답변하세요
4. 야구 팬의 관점에서 재미있게 설명하세요

답변:"""
    
    def process_question(self, question: str) -> str:
        """질문을 Text-to-SQL로 처리"""
        try:
            print(f"🔍 Text-to-SQL 처리 시작: {question}")
            
            # SQL 생성
            sql = self.generate_sql(question)
            if not sql:
                return "SQL 생성에 실패했습니다."
            
            # SQL 실행
            data = self.execute_sql(sql)
            
            # 결과 분석
            answer = self.analyze_results(question, data)
            
            print(f"✅ Text-to-SQL 처리 완료")
            return answer
            
        except Exception as e:
            print(f"❌ Text-to-SQL 처리 오류: {e}")
            return f"처리 중 오류가 발생했습니다: {str(e)}"

def main():
    """테스트 함수"""
    try:
        text_to_sql = TextToSQL()
        
        # 테스트 질문들
        test_questions = [
            "한화 투수 중에 가장 잘하는 투수가 누구야?",
            "KBO 타자 중 타율이 가장 높은 선수는?",
            "문동주 선수 성적이 어때?",
            "내일 한화 경기 일정이 뭐야?"
        ]
        
        for question in test_questions:
            print(f"\n{'='*50}")
            print(f"질문: {question}")
            answer = text_to_sql.process_question(question)
            print(f"답변: {answer}")
            
    except Exception as e:
        print(f"❌ 테스트 실패: {e}")

if __name__ == "__main__":
    main()
