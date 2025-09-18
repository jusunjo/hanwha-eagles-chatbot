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

4. game_schedule 테이블 구조 (실제 컬럼들):
   - game_id, super_category_id, category_id, category_name
   - game_date, game_date_time, time_tbd, stadium, title
   - home_team_code, home_team_name, home_team_score
   - away_team_code, away_team_name, away_team_score
   - winner, status_code, status_num, status_info
   - cancel, suspended, has_video, round_code, reversed_home_away
   - home_team_emblem_url, away_team_emblem_url, game_on_air, widget_enable
   - special_match_info, series_outcome
   - home_starter_name, away_starter_name, win_pitcher_name, lose_pitcher_name
   - home_current_pitcher_name, away_current_pitcher_name, series_game_no
   - broad_channel, round_name, round_game_no, created_at, updated_at

5. game_result 테이블 구조 (팀 순위 및 통계):
   - team_id, team_name, season_id, year, ranking, order_no, wra (승률)
   - game_count, win_game_count, drawn_game_count, lose_game_count, game_behind
   - continuous_game_result, last_five_games
   - 공격 통계: offense_hra, offense_run, offense_rbi, offense_ab, offense_hr, offense_hit, offense_h2, offense_h3, offense_sb, offense_bbhp, offense_kk, offense_gd, offense_obp, offense_slg, offense_ops
   - 수비 통계: defense_era, defense_r, defense_er, defense_inning, defense_hit, defense_hr, defense_kk, defense_bbhp, defense_err, defense_whip, defense_qs, defense_save, defense_hold, defense_wp
   - has_my_team, my_team_category_id, next_schedule_game_id, opposing_team_name, created_at, updated_at

6. players 테이블 구조:
   - id, player_name, pcode, team, position

7. player_season_stats 테이블 구조 (시즌 통계):
   - player_id, player_name, gyear, team, position
   - 타격 통계: hra (타율), hr (홈런), rbi (타점), ab (타석), hit (안타), h2 (2루타), h3 (3루타), sb (도루), bbhp (볼넷+사구), kk (삼진), gd (병살타), obp (출루율), slg (장타율), ops (OPS)
   - 투수 통계: era (평균자책점), w (승수), l (패수), sv (세이브), hold (홀드), wp (완투), qs (퀄리티스타트), whip (WHIP), kk (삼진), bbhp (볼넷+사구), er (자책점), r (실점), inning (이닝), hit (피안타), hr (피홈런), err (실책)

8. player_game_stats 테이블 구조 (경기별 통계):
   - player_id, player_name, gameId, gday, opponent, team, position
   - 타격 통계: hra (타율), hr (홈런), rbi (타점), ab (타석), hit (안타), h2 (2루타), h3 (3루타), sb (도루), bbhp (볼넷+사구), kk (삼진), gd (병살타), obp (출루율), slg (장타율), ops (OPS)
   - 투수 통계: era (평균자책점), w (승수), l (패수), sv (세이브), hold (홀드), wp (완투), qs (퀄리티스타트), whip (WHIP), kk (삼진), bbhp (볼넷+사구), er (자책점), r (실점), inning (이닝), hit (피안타), hr (피홈런), err (실책)

9. 경기 일정 관련 질문 처리 규칙:
   - "앞으로 남은 경기", "남은 일정", "앞으로의 경기" → game_date >= 오늘 날짜
   - "이번 달", "이번 월", "9월", "10월" → 해당 월의 모든 경기
   - "이번 시즌", "올해", "2025년" → 2025년 모든 경기
   - "다음 경기", "다음번 경기" → 가장 가까운 미래 경기 1개
   - "마지막 경기", "최근 경기" → 가장 최근 경기 1개
   - "홈 경기", "원정 경기" → home_team_code 또는 away_team_code로 구분
   - "경기장별", "구장별" → stadium으로 그룹화
   - "주말 경기", "주중 경기" → 요일로 구분 (토요일, 일요일 vs 월~금)

10. 팀 순위 및 통계 관련 질문 처리 규칙:
   - "순위", "랭킹", "몇 위", "등수" → game_result.ranking 사용
   - "승률", "승수", "패수", "몇승", "몇패" → game_result.wra, win_game_count, lose_game_count 사용
   - "팀 타율", "팀 홈런", "팀 ERA" → game_result.offense_hra, offense_hr, defense_era 사용
   - "한화 순위", "한화 승률", "한화 전적" → team_id = 'HH'로 필터링
   - "1위", "2위", "3위" → ranking = 1, 2, 3으로 필터링
   - "상위권", "하위권" → ranking <= 5 (상위권), ranking >= 6 (하위권)

11. 선수 성적 관련 질문 처리 규칙:
   - "타율", "홈런", "타점", "안타", "출루율", "장타율", "OPS" → hra, hr, rbi, hit, obp, slg, ops 사용
   - "ERA", "WHIP", "승수", "패수", "세이브", "홀드" → era, whip, w, l, sv, hold 사용
   - "시즌 성적", "이번 시즌" → player_season_stats 테이블 사용
   - "경기별 성적", "특정 경기" → player_game_stats 테이블 사용
   - "한화 선수", "특정 팀 선수" → team 필드로 필터링
   - "투수", "타자" → position 필드로 필터링

질문: {question}

올바른 SQL 예시:

=== 선수 성적 관련 ===
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

=== 경기 일정 관련 ===
오늘 경기 일정:
SELECT game_date, home_team_name, away_team_name, stadium, game_date_time, status_info
FROM game_schedule 
WHERE game_date = '2025-09-18'
ORDER BY game_date_time;

내일 경기 일정:
SELECT game_date, home_team_name, away_team_name, stadium, game_date_time
FROM game_schedule 
WHERE game_date = '2025-09-19'
ORDER BY game_date_time;

한화 내일 경기 상대 조회:
SELECT home_team_name, away_team_name, stadium, game_date_time, home_team_score, away_team_score
FROM game_schedule 
WHERE game_date = '2025-09-19' 
AND (home_team_code = 'HH' OR away_team_code = 'HH');

=== 앞으로 남은 경기 일정 ===
한화 앞으로 남은 경기 일정:
SELECT game_date, home_team_name, away_team_name, stadium, game_date_time, status_info
FROM game_schedule 
WHERE (home_team_code = 'HH' OR away_team_code = 'HH')
AND game_date >= '2025-09-18'
ORDER BY game_date, game_date_time;

모든 팀 앞으로 남은 경기:
SELECT game_date, home_team_name, away_team_name, stadium, game_date_time, status_info
FROM game_schedule 
WHERE game_date >= '2025-09-18'
ORDER BY game_date, game_date_time;

=== 특정 달/월 경기 일정 ===
9월 경기 일정:
SELECT game_date, home_team_name, away_team_name, stadium, game_date_time, status_info
FROM game_schedule 
WHERE game_date >= '2025-09-01' AND game_date < '2025-10-01'
ORDER BY game_date, game_date_time;

한화 9월 경기 일정:
SELECT game_date, home_team_name, away_team_name, stadium, game_date_time, status_info
FROM game_schedule 
WHERE (home_team_code = 'HH' OR away_team_code = 'HH')
AND game_date >= '2025-09-01' AND game_date < '2025-10-01'
ORDER BY game_date, game_date_time;

=== 이번 시즌/올해 경기 ===
2025년 모든 경기:
SELECT game_date, home_team_name, away_team_name, stadium, game_date_time, status_info
FROM game_schedule 
WHERE game_date >= '2025-01-01' AND game_date < '2026-01-01'
ORDER BY game_date, game_date_time;

=== 다음/최근 경기 ===
한화 다음 경기:
SELECT game_date, home_team_name, away_team_name, stadium, game_date_time, status_info
FROM game_schedule 
WHERE (home_team_code = 'HH' OR away_team_code = 'HH')
AND game_date >= '2025-09-18'
ORDER BY game_date, game_date_time
LIMIT 1;

한화 최근 경기:
SELECT game_date, home_team_name, away_team_name, stadium, game_date_time, home_team_score, away_team_score, winner, status_info
FROM game_schedule 
WHERE (home_team_code = 'HH' OR away_team_code = 'HH')
AND game_date < '2025-09-18'
ORDER BY game_date DESC, game_date_time DESC
LIMIT 1;

=== 홈/원정 경기 ===
한화 홈 경기:
SELECT game_date, home_team_name, away_team_name, stadium, game_date_time, status_info
FROM game_schedule 
WHERE home_team_code = 'HH'
AND game_date >= '2025-09-18'
ORDER BY game_date, game_date_time;

한화 원정 경기:
SELECT game_date, home_team_name, away_team_name, stadium, game_date_time, status_info
FROM game_schedule 
WHERE away_team_code = 'HH'
AND game_date >= '2025-09-18'
ORDER BY game_date, game_date_time;

=== 경기장별 경기 ===
특정 경기장 경기 (예: 잠실):
SELECT game_date, home_team_name, away_team_name, stadium, game_date_time, status_info
FROM game_schedule 
WHERE stadium = '잠실'
AND game_date >= '2025-09-18'
ORDER BY game_date, game_date_time;

=== 주말/주중 경기 ===
주말 경기 (토요일, 일요일):
SELECT game_date, home_team_name, away_team_name, stadium, game_date_time, status_info
FROM game_schedule 
WHERE game_date >= '2025-09-18'
AND (EXTRACT(DOW FROM game_date::date) = 0 OR EXTRACT(DOW FROM game_date::date) = 6)
ORDER BY game_date, game_date_time;

=== 경기 결과 ===
완료된 경기 결과:
SELECT game_date, home_team_name, away_team_name, home_team_score, away_team_score, winner, status_info
FROM game_schedule 
WHERE status_code = 'RESULT' AND game_date = '2025-09-17'
ORDER BY game_date_time;

한화 vs 두산 경기 결과:
SELECT game_date, home_team_name, away_team_name, home_team_score, away_team_score, winner, status_info
FROM game_schedule 
WHERE ((home_team_code = 'HH' AND away_team_code = 'OB') OR (home_team_code = 'OB' AND away_team_code = 'HH'))
AND status_code = 'RESULT'
ORDER BY game_date DESC
LIMIT 5;

=== 팀 순위 및 통계 관련 ===
전체 팀 순위:
SELECT team_name, ranking, wra, win_game_count, lose_game_count, game_behind
FROM game_result 
WHERE year = '2025'
ORDER BY ranking;

한화 순위 및 전적:
SELECT team_name, ranking, wra, win_game_count, lose_game_count, game_behind, last_five_games
FROM game_result 
WHERE team_id = 'HH' AND year = '2025';

한화 승률:
SELECT team_name, wra, win_game_count, lose_game_count
FROM game_result 
WHERE team_id = 'HH' AND year = '2025';

한화 팀 타율:
SELECT team_name, offense_hra, offense_hr, offense_rbi, offense_ops
FROM game_result 
WHERE team_id = 'HH' AND year = '2025';

한화 팀 ERA:
SELECT team_name, defense_era, defense_whip, defense_save, defense_hold
FROM game_result 
WHERE team_id = 'HH' AND year = '2025';

상위권 팀들 (1-5위):
SELECT team_name, ranking, wra, win_game_count, lose_game_count
FROM game_result 
WHERE year = '2025' AND ranking <= 5
ORDER BY ranking;

하위권 팀들 (6위 이하):
SELECT team_name, ranking, wra, win_game_count, lose_game_count
FROM game_result 
WHERE year = '2025' AND ranking >= 6
ORDER BY ranking;

팀 타율 순위:
SELECT team_name, offense_hra, offense_hr, offense_rbi
FROM game_result 
WHERE year = '2025'
ORDER BY offense_hra DESC;

팀 ERA 순위:
SELECT team_name, defense_era, defense_whip, defense_save
FROM game_result 
WHERE year = '2025'
ORDER BY defense_era ASC;

팀 홈런 순위:
SELECT team_name, offense_hr, offense_rbi, offense_ops
FROM game_result 
WHERE year = '2025'
ORDER BY offense_hr DESC;

특정 순위 팀 (예: 1위):
SELECT team_name, ranking, wra, win_game_count, lose_game_count, last_five_games
FROM game_result 
WHERE year = '2025' AND ranking = 1;

=== 선수 성적 관련 ===
한화 타자 시즌 성적 순위:
SELECT p.player_name, s.hra, s.hr, s.rbi, s.hit, s.obp, s.slg, s.ops
FROM players p
JOIN player_season_stats s ON p.id = s.player_id
WHERE p.team = 'HH' AND s.gyear = '2025' AND p.position != '투수'
ORDER BY s.hra DESC
LIMIT 10;

한화 투수 시즌 성적 순위:
SELECT p.player_name, s.era, s.w, s.l, s.sv, s.hold, s.whip, s.kk
FROM players p
JOIN player_season_stats s ON p.id = s.player_id
WHERE p.team = 'HH' AND s.gyear = '2025' AND p.position = '투수'
ORDER BY s.era ASC
LIMIT 10;

특정 선수 시즌 성적 (문동주):
SELECT p.player_name, s.hra, s.hr, s.rbi, s.hit, s.ab, s.obp, s.slg, s.ops
FROM players p
JOIN player_season_stats s ON p.id = s.player_id
WHERE p.player_name = '문동주' AND s.gyear = '2025';

KBO 타율 1위:
SELECT p.player_name, p.team, s.hra, s.hr, s.rbi, s.hit
FROM players p
JOIN player_season_stats s ON p.id = s.player_id
WHERE s.gyear = '2025' AND p.position != '투수'
ORDER BY s.hra DESC
LIMIT 1;

KBO ERA 1위 투수:
SELECT p.player_name, p.team, s.era, s.w, s.l, s.sv, s.whip
FROM players p
JOIN player_season_stats s ON p.id = s.player_id
WHERE s.gyear = '2025' AND p.position = '투수' AND s.era IS NOT NULL
ORDER BY s.era ASC
LIMIT 1;

특정 경기 선수 성적:
SELECT p.player_name, g.hra, g.hr, g.rbi, g.hit, g.opponent, g.gday
FROM players p
JOIN player_game_stats g ON p.id = g.player_id
WHERE p.player_name = '문동주' AND g.gameId = '20250916HHHT02025';

한화 홈런 1위:
SELECT p.player_name, s.hr, s.rbi, s.hra, s.ops
FROM players p
JOIN player_season_stats s ON p.id = s.player_id
WHERE p.team = 'HH' AND s.gyear = '2025' AND p.position != '투수'
ORDER BY s.hr DESC
LIMIT 1;

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
    
    def execute_sql(self, sql: str, question: str = "") -> list:
        """SQL 실행 (새로운 테이블 구조 기반)"""
        try:
            # 간단한 SELECT 쿼리만 지원
            if not sql.upper().startswith('SELECT'):
                return []
            
            # game_schedule 테이블 조회
            if "game_schedule" in sql.lower():
                return self._get_game_schedule_data(sql, question)
            
            # game_result 테이블 조회
            if "game_result" in sql.lower():
                return self._get_game_result_data(sql)
            
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
    
    def _get_game_schedule_data(self, sql: str, question: str = "") -> list:
        """경기 일정 데이터 조회"""
        try:
            from datetime import datetime, date, timedelta
            
            # game_schedule 테이블에서 데이터 조회
            result = self.supabase.supabase.table("game_schedule").select("*").execute()
            
            if not result.data:
                return []
            
            # 원본 질문에서 날짜 조건 추출
            target_date = self._extract_date_from_question(question)
            
            # 날짜 필터링
            if target_date:
                filtered_games = [
                    game for game in result.data 
                    if game.get('game_date', '').startswith(target_date)
                ]
                print(f"📅 {target_date} 경기 조회: {len(filtered_games)}개")
            else:
                # 기본적으로 한 달 동안의 경기 필터링 (오늘부터 30일 후까지)
                today = date.today()
                one_month_later = today + timedelta(days=30)
                today_str = today.strftime("%Y-%m-%d")
                one_month_later_str = one_month_later.strftime("%Y-%m-%d")
                
                filtered_games = [
                    game for game in result.data 
                    if game.get('game_date', '') >= today_str and game.get('game_date', '') <= one_month_later_str
                ]
                print(f"📅 한 달간 경기 조회 ({today_str} ~ {one_month_later_str}): {len(filtered_games)}개")
            
            # 한화 관련 질문인지 확인
            is_hanwha_question = any(keyword in question.lower() for keyword in ['한화', 'hh', '누구랑', '누구와', '상대'])
            
            if is_hanwha_question:
                # 한화 경기만 필터링
                hanwha_games = [
                    game for game in filtered_games 
                    if game.get('home_team_code') == 'HH' or game.get('away_team_code') == 'HH'
                ]
                print(f"📅 한화 경기 조회: {len(hanwha_games)}개")
                return hanwha_games
            else:
                return filtered_games
            
        except Exception as e:
            print(f"❌ 경기 일정 조회 오류: {e}")
            return []
    
    def _extract_date_from_question(self, question: str) -> str:
        """원본 질문에서 날짜 추출 - 다양한 날짜 표현 지원"""
        import re
        from datetime import date, timedelta, datetime
        
        if not question:
            return None
        
        question_lower = question.lower()
        today = date.today()
        
        # 1. 명시적 날짜 패턴들
        date_patterns = [
            # YYYY-MM-DD 형식
            r'(\d{4}-\d{1,2}-\d{1,2})',
            # YYYY/MM/DD 형식  
            r'(\d{4}/\d{1,2}/\d{1,2})',
            # YYYY.MM.DD 형식
            r'(\d{4}\.\d{1,2}\.\d{1,2})',
            # MM/DD 형식 (현재 연도)
            r'(\d{1,2}/\d{1,2})(?![0-9])',
            # MM-DD 형식 (현재 연도)
            r'(\d{1,2}-\d{1,2})(?![0-9])',
            # MM월 DD일 형식
            r'(\d{1,2})월\s*(\d{1,2})일',
            # YYYY년 MM월 DD일 형식
            r'(\d{4})년\s*(\d{1,2})월\s*(\d{1,2})일',
        ]
        
        for i, pattern in enumerate(date_patterns):
            match = re.search(pattern, question)
            if match:
                if i == 5:  # MM월 DD일 형식
                    month, day = match.groups()
                    current_year = today.year
                    return f"{current_year}-{month.zfill(2)}-{day.zfill(2)}"
                elif i == 6:  # YYYY년 MM월 DD일 형식
                    year, month, day = match.groups()
                    return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                else:
                    date_str = match.group(1)
                    # MM/DD 또는 MM-DD 형식인 경우 현재 연도 추가
                    if ('/' in date_str or '-' in date_str) and len(date_str.split('/' if '/' in date_str else '-')) == 2:
                        separator = '/' if '/' in date_str else '-'
                        month, day = date_str.split(separator)
                        current_year = today.year
                        date_str = f"{current_year}-{month.zfill(2)}-{day.zfill(2)}"
                    return date_str
        
        # 2. 상대적 날짜 표현들
        relative_dates = {
            # 오늘 관련
            '오늘': 0, 'today': 0, '금일': 0,
            
            # 어제 관련  
            '어제': -1, 'yesterday': -1, '전일': -1,
            
            # 내일 관련
            '내일': 1, 'tomorrow': 1, '명일': 1,
            
            # 이번 주 관련
            '이번주': 0, '이번 주': 0, 'this week': 0,
            '이번주 월요일': self._get_weekday_offset(today, 0),  # 월요일
            '이번주 화요일': self._get_weekday_offset(today, 1),  # 화요일
            '이번주 수요일': self._get_weekday_offset(today, 2),  # 수요일
            '이번주 목요일': self._get_weekday_offset(today, 3),  # 목요일
            '이번주 금요일': self._get_weekday_offset(today, 4),  # 금요일
            '이번주 토요일': self._get_weekday_offset(today, 5),  # 토요일
            '이번주 일요일': self._get_weekday_offset(today, 6),  # 일요일
            
            # 다음 주 관련
            '다음주': 7, '다음 주': 7, 'next week': 7,
            '다음주 월요일': self._get_weekday_offset(today, 7),  # 다음주 월요일
            '다음주 화요일': self._get_weekday_offset(today, 8),  # 다음주 화요일
            '다음주 수요일': self._get_weekday_offset(today, 9),  # 다음주 수요일
            '다음주 목요일': self._get_weekday_offset(today, 10), # 다음주 목요일
            '다음주 금요일': self._get_weekday_offset(today, 11), # 다음주 금요일
            '다음주 토요일': self._get_weekday_offset(today, 12), # 다음주 토요일
            '다음주 일요일': self._get_weekday_offset(today, 13), # 다음주 일요일
            
            # 지난 주 관련
            '지난주': -7, '지난 주': -7, 'last week': -7,
            '지난주 월요일': self._get_weekday_offset(today, -7),  # 지난주 월요일
            '지난주 화요일': self._get_weekday_offset(today, -6),  # 지난주 화요일
            '지난주 수요일': self._get_weekday_offset(today, -5),  # 지난주 수요일
            '지난주 목요일': self._get_weekday_offset(today, -4),  # 지난주 목요일
            '지난주 금요일': self._get_weekday_offset(today, -3),  # 지난주 금요일
            '지난주 토요일': self._get_weekday_offset(today, -2),  # 지난주 토요일
            '지난주 일요일': self._get_weekday_offset(today, -1),  # 지난주 일요일
        }
        
        # 상대적 날짜 키워드 검색
        for keyword, days_offset in relative_dates.items():
            if keyword in question_lower:
                if isinstance(days_offset, int):
                    target_date = today + timedelta(days=days_offset)
                    return target_date.strftime("%Y-%m-%d")
                else:
                    return days_offset  # 이미 계산된 날짜
        
        # 3. 숫자 + 일/날/일자 표현
        day_patterns = [
            r'(\d+)일\s*후',  # N일 후
            r'(\d+)일\s*전',  # N일 전
            r'(\d+)일\s*뒤',  # N일 뒤
            r'(\d+)일\s*앞',  # N일 앞
            r'(\d+)일\s*지나면',  # N일 지나면
        ]
        
        for pattern in day_patterns:
            match = re.search(pattern, question)
            if match:
                days = int(match.group(1))
                if '후' in pattern or '뒤' in pattern or '지나면' in pattern:
                    target_date = today + timedelta(days=days)
                else:  # 전, 앞
                    target_date = today - timedelta(days=days)
                return target_date.strftime("%Y-%m-%d")
        
        # 4. 요일 표현 (이번 주, 다음 주)
        weekdays = {
            '월요일': 0, '화요일': 1, '수요일': 2, '목요일': 3,
            '금요일': 4, '토요일': 5, '일요일': 6,
            'monday': 0, 'tuesday': 1, 'wednesday': 2, 'thursday': 3,
            'friday': 4, 'saturday': 5, 'sunday': 6
        }
        
        for weekday, weekday_num in weekdays.items():
            if weekday in question_lower:
                # 이번 주인지 다음 주인지 확인
                if '다음' in question_lower or 'next' in question_lower:
                    days_ahead = weekday_num - today.weekday()
                    if days_ahead <= 0:  # 다음 주
                        days_ahead += 7
                    target_date = today + timedelta(days=days_ahead)
                else:  # 이번 주
                    days_ahead = weekday_num - today.weekday()
                    if days_ahead < 0:  # 이번 주가 지났으면 다음 주
                        days_ahead += 7
                    target_date = today + timedelta(days=days_ahead)
                return target_date.strftime("%Y-%m-%d")
        
        # 5. 특정 월/일 표현
        month_patterns = [
            r'(\d{1,2})월\s*(\d{1,2})일',  # MM월 DD일
            r'(\d{1,2})월\s*(\d{1,2})',    # MM월 DD
        ]
        
        for pattern in month_patterns:
            match = re.search(pattern, question)
            if match:
                month, day = match.groups()
                current_year = today.year
                return f"{current_year}-{month.zfill(2)}-{day.zfill(2)}"
        
        return None
    
    def _get_weekday_offset(self, base_date, target_weekday: int) -> str:
        """특정 요일의 날짜 계산"""
        from datetime import timedelta, date
        
        days_ahead = target_weekday - base_date.weekday()
        if days_ahead < 0:  # 이번 주가 지났으면
            days_ahead += 7
        target_date = base_date + timedelta(days=days_ahead)
        return target_date.strftime("%Y-%m-%d")
    
    def _get_game_result_data(self, sql: str) -> list:
        """팀 순위 및 통계 데이터 조회"""
        try:
            # game_result 테이블에서 데이터 조회
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
            "투수", "타자", "선수", "선발", "구원", "마무리", "최고", "가장"
        ])
        
        # 팀 순위 및 통계 관련 질문인지 확인
        is_team_rank_question = any(keyword in question for keyword in [
            "순위", "랭킹", "몇 위", "등수", "승률", "승수", "패수", "몇승", "몇패",
            "팀 타율", "팀 홈런", "팀 ERA", "전적", "상위권", "하위권", "1위", "2위", "3위"
        ])
        
        if is_schedule_question:
            return f"""
당신은 KBO 전문 분석가입니다. 다음 경기 일정 데이터를 바탕으로 사용자의 질문에 답변해주세요.

질문: {question}

경기 일정 데이터:
{context}

답변 규칙:
1. 경기 일정을 명확하고 읽기 쉽게 정리해서 보여주세요
2. 경기 정보를 다음 순서로 포함하세요:
   - 경기 날짜 (game_date)
   - 경기 시간 (game_date_time)
   - 홈팀 vs 원정팀 (home_team_name vs away_team_name)
   - 경기장 (stadium)
   - 경기 상태 (status_info) - 완료된 경기인 경우 점수도 포함
3. 완료된 경기인 경우 승부 결과와 점수를 명확히 표시하세요
4. 한국어로 친근하게 답변하세요
5. 야구 팬이 쉽게 이해할 수 있도록 설명하세요

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
            
            # SQL 실행 (원본 질문 전달)
            data = self.execute_sql(sql, question)
            
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
            "오늘 경기 일정",  # 원본 문제
            "내일 한화 경기 일정이 뭐야?",
            "3월 8일 한화 경기 결과가 어때?",
            "한화 vs 두산 경기 결과 알려줘",
            "어제 경기 결과",
            "다음주 토요일 경기 일정",
            "이번주 금요일 한화 경기",
            "3일 후 경기 일정",
            "9월 18일 경기 결과",
            "2025-09-18 경기 일정",
            # 새로운 경기 일정 관련 질문들
            "한화 앞으로 남은 경기 일정",
            "앞으로 남은 경기들",
            "이번 달 경기 일정",
            "9월 경기 일정",
            "한화 9월 경기 일정",
            "이번 시즌 경기 일정",
            "2025년 모든 경기",
            "한화 다음 경기",
            "한화 최근 경기",
            "한화 홈 경기",
            "한화 원정 경기",
            "잠실 경기 일정",
            "주말 경기 일정",
            "주중 경기 일정",
            "한화 vs 두산 경기 결과",
            "최근 한화 경기 결과"
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
