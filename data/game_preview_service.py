import httpx
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

class GamePreviewService:
    """경기 미리보기 정보를 가져오는 서비스"""
    
    def __init__(self):
        self.base_url = "https://api-gw.sports.naver.com/schedule/games"
    
    async def get_game_preview(self, game_id: str) -> Optional[Dict[str, Any]]:
        """경기 미리보기 정보 조회"""
        try:
            url = f"{self.base_url}/{game_id}/preview"
            logger.info(f"경기 미리보기 API 호출: {url}")
            
            async with httpx.AsyncClient() as client:
                response = await client.get(url)
                response.raise_for_status()
                
                data = response.json()
                logger.info(f"경기 미리보기 데이터 수신 성공: {game_id}")
                logger.debug(f"API 응답 데이터 구조: {type(data)}")
                logger.debug(f"API 응답 데이터 키: {list(data.keys()) if isinstance(data, dict) else 'Not a dict'}")
                
                if data.get("code") == 200 and data.get("success"):
                    logger.info(f"API 응답 코드: {data.get('code')}")
                    return data.get("result", {}).get("previewData")
                else:
                    logger.warning(f"API 응답 실패: {data.get('code')}, {data.get('success')}")
                    return None
                    
        except httpx.HTTPError as e:
            logger.error(f"HTTP 오류 발생: {e}")
            return None
        except Exception as e:
            logger.error(f"경기 미리보기 조회 중 오류: {str(e)}")
            return None
    
    def analyze_game_preview(self, preview_data: Dict[str, Any]) -> Dict[str, Any]:
        """경기 미리보기 데이터 분석"""
        try:
            if not preview_data:
                return {"error": "경기 미리보기 데이터가 없습니다."}
            
            game_info = preview_data.get("gameInfo", {})
            home_standings = preview_data.get("homeStandings", {})
            away_standings = preview_data.get("awayStandings", {})
            home_starter = preview_data.get("homeStarter", {})
            away_starter = preview_data.get("awayStarter", {})
            home_top_player = preview_data.get("homeTopPlayer", {})
            away_top_player = preview_data.get("awayTopPlayer", {})
            season_vs_result = preview_data.get("seasonVsResult", {})
            
            # 경기 기본 정보
            analysis = {
                "game_info": {
                    "date": game_info.get("gdate"),
                    "time": game_info.get("gtime"),
                    "stadium": game_info.get("stadium"),
                    "home_team": game_info.get("hFullName"),
                    "away_team": game_info.get("aFullName"),
                    "status": game_info.get("statusCode"),
                    "round": game_info.get("round")
                },
                "team_standings": {
                    "home": {
                        "name": home_standings.get("name"),
                        "rank": home_standings.get("rank"),
                        "wra": home_standings.get("wra"),
                        "w": home_standings.get("w"),
                        "l": home_standings.get("l"),
                        "d": home_standings.get("d"),
                        "hra": home_standings.get("hra"),
                        "era": home_standings.get("era"),
                        "hr": home_standings.get("hr")
                    },
                    "away": {
                        "name": away_standings.get("name"),
                        "rank": away_standings.get("rank"),
                        "wra": away_standings.get("wra"),
                        "w": away_standings.get("w"),
                        "l": away_standings.get("l"),
                        "d": away_standings.get("d"),
                        "hra": away_standings.get("hra"),
                        "era": away_standings.get("era"),
                        "hr": away_standings.get("hr")
                    }
                },
                "starters": {
                    "home": {
                        "name": home_starter.get("playerInfo", {}).get("name"),
                        "backnum": home_starter.get("playerInfo", {}).get("backnum"),
                        "era": home_starter.get("currentSeasonStats", {}).get("era"),
                        "w": home_starter.get("currentSeasonStats", {}).get("w"),
                        "l": home_starter.get("currentSeasonStats", {}).get("l"),
                        "vs_opponent_era": home_starter.get("currentSeasonStatsOnOpponents", {}).get("era"),
                        "vs_opponent_w": home_starter.get("currentSeasonStatsOnOpponents", {}).get("w"),
                        "vs_opponent_l": home_starter.get("currentSeasonStatsOnOpponents", {}).get("l")
                    },
                    "away": {
                        "name": away_starter.get("playerInfo", {}).get("name"),
                        "backnum": away_starter.get("playerInfo", {}).get("backnum"),
                        "era": away_starter.get("currentSeasonStats", {}).get("era"),
                        "w": away_starter.get("currentSeasonStats", {}).get("w"),
                        "l": away_starter.get("currentSeasonStats", {}).get("l"),
                        "vs_opponent_era": away_starter.get("currentSeasonStatsOnOpponents", {}).get("era"),
                        "vs_opponent_w": away_starter.get("currentSeasonStatsOnOpponents", {}).get("w"),
                        "vs_opponent_l": away_starter.get("currentSeasonStatsOnOpponents", {}).get("l")
                    }
                },
                "key_players": {
                    "home": {
                        "name": home_top_player.get("playerInfo", {}).get("name"),
                        "backnum": home_top_player.get("playerInfo", {}).get("backnum"),
                        "hra": home_top_player.get("currentSeasonStats", {}).get("hra"),
                        "hr": home_top_player.get("currentSeasonStats", {}).get("hr"),
                        "rbi": home_top_player.get("currentSeasonStats", {}).get("rbi"),
                        "recent_hra": home_top_player.get("recentFiveGamesStats", {}).get("hra"),
                        "vs_opponent_hra": home_top_player.get("currentSeasonStatsOnOpponents", {}).get("hra")
                    },
                    "away": {
                        "name": away_top_player.get("playerInfo", {}).get("name"),
                        "backnum": away_top_player.get("playerInfo", {}).get("backnum"),
                        "hra": away_top_player.get("currentSeasonStats", {}).get("hra"),
                        "hr": away_top_player.get("currentSeasonStats", {}).get("hr"),
                        "rbi": away_top_player.get("currentSeasonStats", {}).get("rbi"),
                        "recent_hra": away_top_player.get("recentFiveGamesStats", {}).get("hra"),
                        "vs_opponent_hra": away_top_player.get("currentSeasonStatsOnOpponents", {}).get("hra")
                    }
                },
                "season_head_to_head": {
                    "home_wins": season_vs_result.get("hw", 0),
                    "away_wins": season_vs_result.get("aw", 0),
                    "home_losses": season_vs_result.get("hl", 0),
                    "away_losses": season_vs_result.get("al", 0)
                },
                "lineups": {
                    "home": preview_data.get("homeTeamLineUp", {}).get("fullLineUp", []),
                    "away": preview_data.get("awayTeamLineUp", {}).get("fullLineUp", [])
                },
                "recent_games": {
                    "home": preview_data.get("homeTeamPreviousGames", []),
                    "away": preview_data.get("awayTeamPreviousGames", [])
                }
            }
            
            return analysis
            
        except Exception as e:
            logger.error(f"경기 미리보기 분석 중 오류: {str(e)}")
            return {"error": f"경기 미리보기 분석 중 오류가 발생했습니다: {str(e)}"}

# 전역 인스턴스
game_preview_service = GamePreviewService()
