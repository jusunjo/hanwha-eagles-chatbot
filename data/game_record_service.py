"""
경기 기록 API 호출 및 분석 서비스
"""

import httpx
import json
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime

logger = logging.getLogger(__name__)


class GameRecordService:
    """경기 기록 데이터를 가져오고 분석하는 서비스"""
    
    def __init__(self):
        self.base_url = "https://api-gw.sports.naver.com/schedule/games"
        
    async def get_game_record(self, game_id: str) -> Optional[Dict[str, Any]]:
        """
        특정 경기의 상세 기록을 가져옵니다.
        
        Args:
            game_id: 경기 ID (예: "20250920HHKT02025")
            
        Returns:
            경기 기록 데이터 또는 None
        """
        try:
            url = f"{self.base_url}/{game_id}/record"
            logger.info(f"경기 기록 API 호출: {url}")
            
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(url)
                
                if response.status_code == 200:
                    data = response.json()
                    logger.info(f"경기 기록 데이터 수신 성공: {game_id}")
                    return data
                else:
                    logger.error(f"경기 기록 API 호출 실패: {response.status_code} - {response.text}")
                    return None
                    
        except Exception as e:
            logger.error(f"경기 기록 API 호출 중 오류: {str(e)}")
            return None
    
    def analyze_game_record(self, record_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        경기 기록 데이터를 분석하여 요약 정보를 생성합니다.
        
        Args:
            record_data: 경기 기록 API 응답 데이터
            
        Returns:
            분석된 경기 요약 정보
        """
        try:
            if not record_data or record_data.get("code") != 200:
                return {"error": "유효하지 않은 경기 기록 데이터입니다."}
            
            result = record_data.get("result", {}).get("recordData", {})
            
            # 기본 경기 정보
            game_info = result.get("gameInfo", {})
            score_board = result.get("scoreBoard", {})
            
            # 팀별 기록
            team_pitching = result.get("teamPitchingBoxscore", {})
            batters_boxscore = result.get("battersBoxscore", {})
            
            # 투수 기록
            pitchers_boxscore = result.get("pitchersBoxscore", {})
            
            # 특별 기록들
            etc_records = result.get("etcRecords", [])
            
            analysis = {
                "game_info": self._analyze_game_info(game_info),
                "score_analysis": self._analyze_score(score_board),
                "team_performance": self._analyze_team_performance(team_pitching, batters_boxscore),
                "pitching_analysis": self._analyze_pitching(pitchers_boxscore),
                "special_records": self._analyze_special_records(etc_records),
                "key_moments": self._analyze_key_moments(result)
            }
            
            return analysis
            
        except Exception as e:
            logger.error(f"경기 기록 분석 중 오류: {str(e)}")
            return {"error": f"경기 분석 중 오류가 발생했습니다: {str(e)}"}
    
    def _analyze_game_info(self, game_info: Dict[str, Any]) -> Dict[str, Any]:
        """경기 기본 정보 분석"""
        return {
            "date": game_info.get("gdate"),
            "home_team": game_info.get("hFullName", game_info.get("hName")),
            "away_team": game_info.get("aFullName", game_info.get("aName")),
            "stadium": game_info.get("stadium"),
            "time": game_info.get("gtime"),
            "status": game_info.get("statusCode")
        }
    
    def _analyze_score(self, score_board: Dict[str, Any]) -> Dict[str, Any]:
        """점수 분석"""
        rheb = score_board.get("rheb", {})
        inn = score_board.get("inn", {})
        
        home_score = rheb.get("home", {}).get("r", 0)
        away_score = rheb.get("away", {}).get("r", 0)
        
        # 이닝별 점수 분석
        home_innings = inn.get("home", [])
        away_innings = inn.get("away", [])
        
        # 경기 흐름 분석
        home_momentum = self._calculate_momentum(home_innings)
        away_momentum = self._calculate_momentum(away_innings)
        
        return {
            "final_score": f"{away_score} : {home_score}",
            "home_score": home_score,
            "away_score": away_score,
            "home_momentum": home_momentum,
            "away_momentum": away_momentum,
            "home_innings": home_innings,
            "away_innings": away_innings
        }
    
    def _calculate_momentum(self, innings: List[int]) -> str:
        """이닝별 점수를 바탕으로 경기 흐름 계산"""
        if not innings:
            return "데이터 없음"
        
        # 점수가 난 이닝들을 찾아서 흐름 파악
        scoring_innings = [i+1 for i, score in enumerate(innings) if score > 0]
        
        if not scoring_innings:
            return "득점 없음"
        
        if len(scoring_innings) == 1:
            return f"{scoring_innings[0]}회에만 득점"
        
        # 초반(1-3회), 중반(4-6회), 후반(7회+)으로 나누어 분석
        early = [inn for inn in scoring_innings if inn <= 3]
        middle = [inn for inn in scoring_innings if 4 <= inn <= 6]
        late = [inn for inn in scoring_innings if inn >= 7]
        
        if early and not middle and not late:
            return "초반 집중 득점"
        elif late and not early and not middle:
            return "후반 역전"
        elif early and late:
            return "초반-후반 득점"
        else:
            return "고른 득점"
    
    def _analyze_team_performance(self, team_pitching: Dict[str, Any], batters_boxscore: Dict[str, Any]) -> Dict[str, Any]:
        """팀 성과 분석"""
        away_pitching = team_pitching.get("away", {})
        home_pitching = team_pitching.get("home", {})
        
        away_total = batters_boxscore.get("awayTotal", {})
        home_total = batters_boxscore.get("homeTotal", {})
        
        return {
            "away": {
                "hits": away_total.get("hit", 0),
                "at_bats": away_total.get("ab", 0),
                "batting_avg": away_total.get("hra", "0.000"),
                "runs": away_total.get("run", 0),
                "strikeouts": away_pitching.get("kk", 0),
                "walks": away_pitching.get("bbhp", 0)
            },
            "home": {
                "hits": home_total.get("hit", 0),
                "at_bats": home_total.get("ab", 0),
                "batting_avg": home_total.get("hra", "0.000"),
                "runs": home_total.get("run", 0),
                "strikeouts": home_pitching.get("kk", 0),
                "walks": home_pitching.get("bbhp", 0)
            }
        }
    
    def _analyze_pitching(self, pitchers_boxscore: Dict[str, Any]) -> Dict[str, Any]:
        """투수 성과 분석"""
        away_pitchers = pitchers_boxscore.get("away", [])
        home_pitchers = pitchers_boxscore.get("home", [])
        
        # 선발 투수 찾기 (가장 많이 던진 투수)
        away_starter = max(away_pitchers, key=lambda x: float(x.get("inn", "0").split()[0]) if x.get("inn") else 0) if away_pitchers else {}
        home_starter = max(home_pitchers, key=lambda x: float(x.get("inn", "0").split()[0]) if x.get("inn") else 0) if home_pitchers else {}
        
        return {
            "away_starter": {
                "name": away_starter.get("name", ""),
                "innings": away_starter.get("inn", ""),
                "hits": away_starter.get("hit", 0),
                "runs": away_starter.get("r", 0),
                "strikeouts": away_starter.get("kk", 0),
                "walks": away_starter.get("bb", 0),
                "era": away_starter.get("era", "0.00")
            },
            "home_starter": {
                "name": home_starter.get("name", ""),
                "innings": home_starter.get("inn", ""),
                "hits": home_starter.get("hit", 0),
                "runs": home_starter.get("r", 0),
                "strikeouts": home_starter.get("kk", 0),
                "walks": home_starter.get("bb", 0),
                "era": home_starter.get("era", "0.00")
            }
        }
    
    def _analyze_special_records(self, etc_records: List[Dict[str, Any]]) -> Dict[str, Any]:
        """특별 기록 분석"""
        home_runs = []
        stolen_bases = []
        errors = []
        game_winners = []
        
        for record in etc_records:
            how = record.get("how", "")
            result = record.get("result", "")
            
            if how == "홈런":
                home_runs.append(result)
            elif how == "도루":
                stolen_bases.append(result)
            elif how == "실책":
                errors.append(result)
            elif how == "결승타":
                game_winners.append(result)
        
        return {
            "home_runs": home_runs,
            "stolen_bases": stolen_bases,
            "errors": errors,
            "game_winners": game_winners
        }
    
    def _analyze_key_moments(self, result: Dict[str, Any]) -> List[str]:
        """주요 순간 분석"""
        key_moments = []
        
        # 홈런 기록이 있으면 추가
        etc_records = result.get("etcRecords", [])
        for record in etc_records:
            if record.get("how") == "홈런":
                key_moments.append(f"홈런: {record.get('result', '')}")
            elif record.get("how") == "결승타":
                key_moments.append(f"결승타: {record.get('result', '')}")
        
        return key_moments
    
    def generate_game_summary(self, analysis: Dict[str, Any]) -> str:
        """
        경기 분석 결과를 바탕으로 자연어 요약을 생성합니다.
        
        Args:
            analysis: 분석된 경기 데이터
            
        Returns:
            자연어로 작성된 경기 요약
        """
        try:
            if "error" in analysis:
                return analysis["error"]
            
            game_info = analysis.get("game_info", {})
            score_analysis = analysis.get("score_analysis", {})
            team_performance = analysis.get("team_performance", {})
            pitching_analysis = analysis.get("pitching_analysis", {})
            special_records = analysis.get("special_records", {})
            key_moments = analysis.get("key_moments", [])
            
            # 기본 정보
            home_team = game_info.get("home_team", "")
            away_team = game_info.get("away_team", "")
            stadium = game_info.get("stadium", "")
            date = game_info.get("date", "")
            
            # 날짜 포맷팅
            if date:
                try:
                    date_obj = datetime.strptime(str(date), "%Y%m%d")
                    formatted_date = date_obj.strftime("%Y년 %m월 %d일")
                except:
                    formatted_date = str(date)
            else:
                formatted_date = "날짜 미상"
            
            # 점수 정보
            final_score = score_analysis.get("final_score", "")
            home_score = score_analysis.get("home_score", 0)
            away_score = score_analysis.get("away_score", 0)
            
            # 승리팀 결정
            if home_score > away_score:
                winner = home_team
                loser = away_team
                win_score = home_score
                lose_score = away_score
            else:
                winner = away_team
                loser = home_team
                win_score = away_score
                lose_score = home_score
            
            # 경기 흐름
            home_momentum = score_analysis.get("home_momentum", "")
            away_momentum = score_analysis.get("away_momentum", "")
            
            # 투수 정보
            away_starter = pitching_analysis.get("away_starter", {})
            home_starter = pitching_analysis.get("home_starter", {})
            
            # 특별 기록
            home_runs = special_records.get("home_runs", [])
            game_winners = special_records.get("game_winners", [])
            
            # 요약 생성
            summary_parts = []
            
            # 기본 경기 정보
            summary_parts.append(f"📅 {formatted_date} {stadium}에서 열린 {away_team} vs {home_team} 경기 결과입니다.")
            
            # 경기 결과
            summary_parts.append(f"🏆 {winner} {win_score} - {lose_score} {loser}로 승리했습니다.")
            
            # 경기 흐름
            if home_momentum and away_momentum:
                summary_parts.append(f"⚾ 경기 흐름: {away_team}은 {away_momentum}, {home_team}은 {home_momentum}을 보였습니다.")
            
            # 선발 투수
            if away_starter.get("name") and home_starter.get("name"):
                summary_parts.append(f"🎯 선발 투수: {away_team} {away_starter['name']} ({away_starter['innings']}이닝) vs {home_team} {home_starter['name']} ({home_starter['innings']}이닝)")
            
            # 홈런 기록
            if home_runs:
                hr_text = ", ".join(home_runs)
                summary_parts.append(f"💥 홈런: {hr_text}")
            
            # 결승타
            if game_winners:
                gw_text = ", ".join(game_winners)
                summary_parts.append(f"🔥 결승타: {gw_text}")
            
            # 주요 순간
            if key_moments:
                summary_parts.append(f"⭐ 주요 순간: {', '.join(key_moments)}")
            
            return "\n".join(summary_parts)
            
        except Exception as e:
            logger.error(f"경기 요약 생성 중 오류: {str(e)}")
            return f"경기 요약 생성 중 오류가 발생했습니다: {str(e)}"


# 싱글톤 인스턴스 생성
game_record_service = GameRecordService()
