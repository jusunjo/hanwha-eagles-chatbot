#!/usr/bin/env python3
"""
KBO 챗봇 Text-to-SQL 기능 구현
"""

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from langchain.chat_models import ChatOpenAI
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
            "경기표", "스케줄", "대진표", "경기 시간", "경기장", "구장"
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

2. 타율 필드명:
   - 타율은 "hra" 필드만 사용 (절대 "avg", "battingAverage" 사용 금지)

3. 시즌 데이터:
   - record.season은 JSON 배열이므로 WHERE에서 직접 비교하지 마세요

데이터베이스 스키마:
- pcode 테이블: playerName, team, pcode
- player_info 테이블: playerName, record, basicRecord
- game_schedule 테이블: game_id, game_date, game_date_time, stadium, home_team_code, home_team_name, away_team_code, away_team_name, status_code, status_info, home_team_score, away_team_score, winner
- game_result 테이블: team_id, team_name, year, ranking, win_game_count, lose_game_count, wra

질문: {question}

올바른 SQL 예시:
한화 타자 순위 조회:
SELECT p.playerName, pi.record 
FROM pcode p 
JOIN player_info pi ON p.playerName = pi.playerName 
WHERE p.team = 'HH' 
LIMIT 5;

내일 경기 일정 조회:
SELECT game_date, game_date_time, stadium, home_team_name, away_team_name, status_info
FROM game_schedule 
WHERE DATE(game_date) = DATE(NOW() + INTERVAL 1 DAY)
ORDER BY game_date_time;

한화 팀 순위 조회:
SELECT team_name, ranking, win_game_count, lose_game_count, wra 
FROM game_result 
WHERE team_id = 'HH' AND year = 2025;

SQL:""")
            
            response = self.llm.invoke(prompt.format(question=question))
            sql = response.content.strip()
            
            # SQL 정리
            sql = re.sub(r'```sql\s*', '', sql)
            sql = re.sub(r'```\s*', '', sql)
            sql = sql.strip()
            
            # 잘못된 필드명 자동 수정
            sql = re.sub(r'battingAverage', 'hra', sql, flags=re.IGNORECASE)
            sql = re.sub(r'\bavg\b', 'hra', sql, flags=re.IGNORECASE)
            
            # 정규식으로 팀명 수정 (더 강력함)
            sql = re.sub(r"= '한화'", "= 'HH'", sql)
            sql = re.sub(r"= '두산'", "= 'OB'", sql)
            sql = re.sub(r"= 'KIA'", "= 'HT'", sql)
            sql = re.sub(r"= '키움'", "= 'WO'", sql)
            sql = re.sub(r"= '롯데'", "= 'LT'", sql)
            sql = re.sub(r"= '삼성'", "= 'SS'", sql)
            sql = re.sub(r"= 'SSG'", "= 'SK'", sql)
            sql = re.sub(r"= 'KT'", "= 'KT'", sql)
            sql = re.sub(r"= 'NC'", "= 'NC'", sql)
            sql = re.sub(r"= 'LG'", "= 'LG'", sql)
            
            # 잘못된 팀명 자동 수정 (더 강력한 패턴)
            team_mappings = {
                "'한화'": "'HH'", 
                "'두산'": "'OB'", 
                "'KIA'": "'HT'", 
                "'키움'": "'WO'",
                "'롯데'": "'LT'", 
                "'삼성'": "'SS'", 
                "'SSG'": "'SK'", 
                "'KT'": "'KT'",
                "'NC'": "'NC'", 
                "'LG'": "'LG'",
                # 따옴표 없는 경우도 처리
                "한화": "HH",
                "두산": "OB", 
                "KIA": "HT",
                "키움": "WO",
                "롯데": "LT",
                "삼성": "SS",
                "SSG": "SK",
                "KT": "KT",
                "NC": "NC",
                "LG": "LG"
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
        """SQL 실행 (수동 데이터 조회 사용)"""
        try:
            # 간단한 SELECT 쿼리만 지원
            if not sql.upper().startswith('SELECT'):
                return []
            
            # game_schedule 테이블 조회
            if "game_schedule" in sql.lower():
                return self._get_game_schedule_data(sql)
            
            # game_result 테이블 조회
            if "game_result" in sql.lower():
                return self._get_team_stats_data(sql)
            
            # Supabase RPC 함수가 없으므로 직접 수동 데이터 조회 사용
            return self._manual_data_query(sql)
                
        except Exception as e:
            print(f"❌ 데이터 조회 오류: {e}")
            return []
    
    def _manual_data_query(self, sql: str) -> list:
        """수동으로 데이터 조회 (RPC가 없을 때)"""
        try:
            # 투수 데이터 조회
            if "투수" in sql or "pitcher" in sql.lower():
                return self._get_kbo_pitchers()
            
            # 타자 데이터 조회
            if "타자" in sql or "hitter" in sql.lower():
                return self._get_kbo_hitters()
            
            # 특정 팀 선수 조회
            team_keywords = {
                "한화": "HH", "두산": "OB", "KIA": "HT", "키움": "WO", 
                "롯데": "LT", "삼성": "SS", "SSG": "SK", "KT": "KT", 
                "NC": "NC", "LG": "LG"
            }
            
            for team_name, team_code in team_keywords.items():
                if team_name in sql or team_code in sql:
                    return self._get_team_players(team_code, team_name)
            
            # 일반 선수 데이터 조회 (모든 KBO 팀)
            result = self.supabase.supabase.table("pcode").select("*").execute()
            return result.data if result.data else []
            
        except Exception as e:
            print(f"❌ 수동 데이터 조회 오류: {e}")
            return []
    
    def _get_kbo_pitchers(self) -> list:
        """KBO 투수 데이터 조회"""
        try:
            # pcode에서 모든 KBO 선수들 조회
            kbo_players = self.supabase.supabase.table('pcode').select('*').execute()
            
            kbo_pitchers = []
            for player in kbo_players.data:
                player_name = player['playerName']
                player_info = self.supabase.supabase.table('player_info').select('*').eq('playerName', player_name).execute()
                
                if player_info.data:
                    data = player_info.data[0]
                    record = data.get('record', {})
                    if 'season' in record:
                        # 2025년 시즌 데이터 찾기
                        for season in record['season']:
                            if season.get('gyear') == '2025':
                                # 투수 데이터인지 확인 (ERA가 있으면 투수)
                                if season.get('era') and season.get('era') != 'N/A':
                                    kbo_pitchers.append({
                                        'playerName': player_name,
                                        'team': season.get('team', ''),
                                        'era': season.get('era'),
                                        'w': season.get('w'),
                                        'l': season.get('l'),
                                        'kk': season.get('kk'),
                                        'whip': season.get('whip'),
                                        'gyear': '2025'
                                    })
                                break
            
            return kbo_pitchers
            
        except Exception as e:
            print(f"❌ KBO 투수 데이터 조회 오류: {e}")
            return []
    
    def _get_kbo_hitters(self) -> list:
        """KBO 타자 데이터 조회"""
        try:
            # pcode에서 모든 KBO 선수들 조회
            kbo_players = self.supabase.supabase.table('pcode').select('*').execute()
            
            kbo_hitters = []
            for player in kbo_players.data:
                player_name = player['playerName']
                player_info = self.supabase.supabase.table('player_info').select('*').eq('playerName', player_name).execute()
                
                if player_info.data:
                    data = player_info.data[0]
                    record = data.get('record', {})
                    if 'season' in record:
                        # 2025년 시즌 데이터 찾기
                        for season in record['season']:
                            if season.get('gyear') == '2025':
                                # 타자 데이터인지 확인 (hra가 있으면 타자)
                                if season.get('hra') and season.get('hra') != 'N/A':
                                    kbo_hitters.append({
                                        'playerName': player_name,
                                        'team': season.get('team', ''),
                                        'hra': season.get('hra'),  # 타율
                                        'hr': season.get('hr'),    # 홈런
                                        'rbi': season.get('rbi'),  # 타점
                                        'hit': season.get('hit'),  # 안타
                                        'ab': season.get('ab'),    # 타석
                                        'obp': season.get('obp'),  # 출루율
                                        'slg': season.get('slg'),  # 장타율
                                        'ops': season.get('ops'),  # OPS
                                        'gyear': '2025'
                                    })
                                break
            
            return kbo_hitters
            
        except Exception as e:
            print(f"❌ KBO 타자 데이터 조회 오류: {e}")
            return []
    
    def _get_team_players(self, team_code: str, team_name: str) -> list:
        """특정 팀 선수 데이터 조회 (투수 + 타자)"""
        try:
            # pcode에서 해당 팀 선수들만 조회
            team_players = self.supabase.supabase.table('pcode').select('*').eq('team', team_code).execute()
            
            all_team_players = []
            for player in team_players.data:
                player_name = player['playerName']
                player_info = self.supabase.supabase.table('player_info').select('*').eq('playerName', player_name).execute()
                
                if player_info.data:
                    data = player_info.data[0]
                    record = data.get('record', {})
                    basic_record = data.get('basicRecord', {})
                    
                    player_data = {
                        'playerName': player_name,
                        'team': team_code,
                        'teamName': team_name,
                        'position': basic_record.get('position', ''),
                        'gyear': '2025'
                    }
                    
                    if 'season' in record:
                        # 2025년 시즌 데이터 찾기
                        for season in record['season']:
                            if season.get('gyear') == '2025':
                                # 투수 데이터 (ERA가 있으면)
                                if season.get('era') and season.get('era') != 'N/A':
                                    player_data.update({
                                        'type': 'pitcher',
                                        'era': season.get('era'),
                                        'w': season.get('w'),
                                        'l': season.get('l'),
                                        'kk': season.get('kk'),
                                        'whip': season.get('whip')
                                    })
                                # 타자 데이터 (hra가 있으면)
                                elif season.get('hra') and season.get('hra') != 'N/A':
                                    player_data.update({
                                        'type': 'hitter',
                                        'hra': season.get('hra'),  # 타율
                                        'hr': season.get('hr'),    # 홈런
                                        'rbi': season.get('rbi'),  # 타점
                                        'hit': season.get('hit'),  # 안타
                                        'ab': season.get('ab'),    # 타석
                                        'obp': season.get('obp'),  # 출루율
                                        'slg': season.get('slg'),  # 장타율
                                        'ops': season.get('ops')   # OPS
                                    })
                                break
                    
                    all_team_players.append(player_data)
            
            return all_team_players
            
        except Exception as e:
            print(f"❌ {team_name} 선수 데이터 조회 오류: {e}")
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
                if game.get('game_date', '').startswith(tomorrow_str)
            ]
            
            print(f"📅 내일 경기 일정 조회: {len(filtered_games)}개")
            
            return filtered_games
            
        except Exception as e:
            print(f"❌ 경기 일정 조회 오류: {e}")
            return []
    
    def _get_team_stats_data(self, sql: str) -> list:
        """팀 통계 데이터 조회"""
        try:
            # game_result 테이블에서 모든 데이터 조회
            result = self.supabase.supabase.table("game_result").select("*").execute()
            
            if not result.data:
                return []
            
            # SQL에서 팀 필터링이 있는지 확인
            team_keywords = {
                "한화": "HH", "두산": "OB", "KIA": "HT", "키움": "WO", 
                "롯데": "LT", "삼성": "SS", "SSG": "SK", "KT": "KT", 
                "NC": "NC", "LG": "LG"
            }
            
            filtered_data = result.data
            
            # 특정 팀의 데이터만 필터링
            for team_name, team_code in team_keywords.items():
                if team_name in sql or team_code in sql:
                    filtered_data = [
                        team for team in result.data 
                        if team.get('team_id') == team_code
                    ]
                    break
            
            # 2025년 데이터만 필터링
            filtered_data = [
                team for team in filtered_data 
                if team.get('year') == 2025
            ]
            
            # 순위순 정렬 (랭킹이 있는 경우)
            if any(keyword in sql.lower() for keyword in ['순위', 'ranking', 'rank']):
                filtered_data.sort(key=lambda x: x.get('ranking', 999))
            
            print(f"📊 팀 통계 데이터 조회: {len(filtered_data)}개")
            
            return filtered_data
            
        except Exception as e:
            print(f"❌ 팀 통계 데이터 조회 오류: {e}")
            return []
    
    def analyze_results(self, question: str, data: list) -> str:
        """조회 결과를 분석해서 답변 생성"""
        try:
            if not data:
                return "해당 질문에 대한 데이터를 찾을 수 없습니다."
            
            # 데이터를 컨텍스트로 변환
            context = json.dumps(data, ensure_ascii=False, indent=2)
            
            # 경기 일정 관련 질문인지 확인
            is_schedule_question = any(keyword in question for keyword in [
                "경기 일정", "일정", "경기", "내일", "오늘", "어제", "다음", "이번 주",
                "경기표", "스케줄", "대진표", "경기 시간", "경기장", "구장"
            ])
            
            # 팀 통계 관련 질문인지 확인
            is_team_stats_question = any(keyword in question for keyword in [
                "몇승", "승수", "승리수", "몇패", "패수", "패배수", "승률", "순위",
                "이번 시즌", "시즌", "현재", "지금", "몇위", "등수", "랭킹",
                "타율", "홈런", "타점", "안타", "출루율", "장타율", "OPS",
                "ERA", "WHIP", "세이브", "홀드", "완투", "퀄리티스타트"
            ])
            
            if is_schedule_question:
                prompt = ChatPromptTemplate.from_template("""
당신은 KBO 전문 분석가입니다. 다음 경기 일정 데이터를 바탕으로 사용자의 질문에 답변해주세요.

질문: {question}

경기 일정 데이터:
{context}

답변 규칙:
1. 경기 일정을 명확하고 읽기 쉽게 정리해서 보여주세요
2. 경기 시간, 경기장, 홈팀 vs 원정팀 정보를 포함하세요
3. 한국어로 친근하게 답변하세요
4. 야구 팬이 쉽게 이해할 수 있도록 설명하세요

답변:""")
            elif is_team_stats_question:
                prompt = ChatPromptTemplate.from_template("""
당신은 KBO 전문 분석가입니다. 다음 팀 통계 데이터를 바탕으로 사용자의 질문에 답변해주세요.

질문: {question}

팀 통계 데이터:
{context}

답변 규칙:
1. 팀 통계를 명확하고 읽기 쉽게 정리해서 보여주세요
2. 순위, 승수, 패수, 승률, 타율, 홈런, ERA 등 구체적인 수치를 포함하세요
3. 한국어로 친근하게 답변하세요
4. 야구 팬이 쉽게 이해할 수 있도록 설명하세요
5. 팀명은 정확히 표시하세요

답변:""")
            else:
                prompt = ChatPromptTemplate.from_template("""
당신은 KBO 전문 분석가입니다. 다음 데이터를 바탕으로 사용자의 질문에 답변해주세요.

질문: {question}

데이터:
{context}

답변 규칙:
1. 데이터를 기반으로 정확한 답변을 제공하세요
2. 구체적인 수치와 선수명을 포함하세요
3. 한국어로 친근하게 답변하세요
4. 야구 팬의 관점에서 재미있게 설명하세요

답변:""")
            
            response = self.llm.invoke(prompt.format(question=question, context=context))
            return response.content
            
        except Exception as e:
            print(f"❌ 결과 분석 오류: {e}")
            return f"데이터 분석 중 오류가 발생했습니다: {str(e)}"
    
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
            "KBO 투수 중에 가장 잘하는 투수가 누구야?",
            "KBO 투수 중 ERA가 가장 낮은 투수는?",
            "KBO 투수 중 승수가 가장 많은 투수는?"
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
