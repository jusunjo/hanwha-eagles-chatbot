import os
import json
import re
import asyncio
import httpx
from datetime import datetime
from openai import OpenAI
from data_manager import HanwhaEaglesDataManager
from typing import Dict, Any, List
from dotenv import load_dotenv

# 환경 변수 로드
load_dotenv()

class HanwhaEaglesChatbot:
    def __init__(self):
        self.client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))
        self.data_manager = HanwhaEaglesDataManager()
        
    async def get_response_async(self, user_message: str, callback_url: str = None) -> Dict[str, Any]:
        """사용자 메시지에 대한 응답 생성 (비동기 + 콜백 지원)"""
        try:
            print(f"\n🤖 ===== 챗봇 응답 생성 시작 =====")
            print(f"📝 사용자 메시지: {user_message}")
            print(f"🔗 콜백 URL: {callback_url}")
            
            # 백그라운드에서 실제 처리를 하는 함수
            async def process_chatbot_background():
                try:
                    print(f"[BACKGROUND] 백그라운드 챗봇 처리 시작 - 질문: {user_message}")
                    
                    # 기존 동기 메서드를 비동기로 실행
                    response_text = await self._process_message_async(user_message)
                    
                    print(f"[BACKGROUND] 챗봇 답변 생성 완료: {response_text}")
                    
                    # 콜백으로 최종 결과 전송
                    if callback_url:
                        final_callback_response = {
                            "version": "2.0",
                            "useCallback": True,
                            "template": {
                                "outputs": [
                                    {
                                        "simpleText": {
                                            "text": response_text
                                        }
                                    }
                                ]
                            }
                        }
                        
                        async with httpx.AsyncClient(timeout=60.0) as client:
                            response = await client.post(
                                callback_url,
                                json=final_callback_response,
                                headers={"Content-Type": "application/json"}
                            )
                            print(f"[BACKGROUND] 최종 결과 콜백 전송 완료 - 상태코드: {response.status_code}")
                            
                except Exception as e:
                    print(f"[BACKGROUND ERROR] 백그라운드 처리 중 오류: {str(e)}")
                    
                    # 에러 발생 시에도 콜백으로 에러 메시지 전송
                    if callback_url:
                        try:
                            error_callback_response = {
                                "version": "2.0",
                                "useCallback": True,
                                "template": {
                                    "outputs": [
                                        {
                                            "simpleText": {
                                                "text": "AI 처리 중 오류가 발생했어요. 다시 시도해주세요."
                                            }
                                        }
                                    ]
                                }
                            }
                            
                            async with httpx.AsyncClient(timeout=60.0) as client:
                                await client.post(
                                    callback_url,
                                    json=error_callback_response,
                                    headers={"Content-Type": "application/json"}
                                )
                                print(f"[BACKGROUND] 에러 콜백 전송 완료")
                        except Exception as callback_error:
                            print(f"[BACKGROUND ERROR] 에러 콜백 전송 실패: {str(callback_error)}")
            
            # 콜백 URL이 있는 경우 백그라운드 처리
            if callback_url:
                # 백그라운드에서 챗봇 작업 시작
                background_task = asyncio.create_task(process_chatbot_background())
                
                # 3초 대기 (빠른 응답인지 확인)
                try:
                    print("[DEBUG] 3초 타임아웃 시작...")
                    # 3초 동안 처리가 완료되는지 기다림
                    result = await asyncio.wait_for(
                        self._process_message_async(user_message),
                        timeout=3.0
                    )
                    
                    # 3초 이내에 결과가 나온 경우
                    print("[SUCCESS] 3초 이내에 결과 완료")
                    background_task.cancel()  # 백그라운드 태스크 취소
                    
                    # 즉시 응답
                    immediate_response = {
                        "version": "2.0",
                        "template": {
                            "outputs": [
                                {
                                    "simpleText": {
                                        "text": result
                                    }
                                }
                            ]
                        }
                    }
                    
                    print(f"[DEBUG] 즉시 응답 데이터: {json.dumps(immediate_response, ensure_ascii=False, indent=2)}")
                    return immediate_response
                    
                except asyncio.TimeoutError:
                    # 3초가 지나서 타임아웃된 경우
                    print("[INFO] 3초 타임아웃 - 백그라운드 처리로 전환")
                    
                    # 즉시 "기다리는 메시지" 응답
                    waiting_response = {
                        "version": "2.0",
                        "useCallback": True,
                        "template": {
                            "outputs": [
                                {
                                    "simpleText": {
                                        "text": "잠시만 기다려주세요!"
                                    }
                                }
                            ]
                        }
                    }
                    
                    print(f"[DEBUG] 대기 메시지 응답: {json.dumps(waiting_response, ensure_ascii=False, indent=2)}")
                    return waiting_response
            
            else:
                # 콜백 URL이 없는 경우 동기 처리
                response_text = await self._process_message_async(user_message)
                return {
                    "version": "2.0",
                    "template": {
                        "outputs": [
                            {
                                "simpleText": {
                                    "text": response_text
                                }
                            }
                        ]
                    }
                }
            
        except Exception as e:
            print(f"❌ Error generating response: {str(e)}")
            
            # 에러 발생 시 콜백으로 에러 메시지 전송
            if callback_url:
                try:
                    error_callback_response = {
                        "version": "2.0",
                        "useCallback": True,
                        "template": {
                            "outputs": [
                                {
                                    "simpleText": {
                                        "text": "요청 처리 중 오류가 발생했어요. 다시 시도해주세요."
                                    }
                                }
                            ]
                        }
                    }
                    
                    async with httpx.AsyncClient(timeout=60.0) as client:
                        await client.post(
                            callback_url,
                            json=error_callback_response,
                            headers={"Content-Type": "application/json"}
                        )
                        print(f"[CALLBACK] 에러 콜백 전송 완료")
                except Exception as callback_error:
                    print(f"[CALLBACK ERROR] 에러 콜백 전송 실패: {str(callback_error)}")
            
            # 에러 응답
            error_response = {
                "version": "2.0",
                "template": {
                    "outputs": [
                        {
                            "simpleText": {
                                "text": "요청 처리 중 오류가 발생했어요. 다시 시도해주세요."
                            }
                        }
                    ]
                }
            }
            print(f"[DEBUG] 에러 응답 데이터: {json.dumps(error_response, ensure_ascii=False, indent=2)}")
            return error_response
    
    async def _process_message_async(self, user_message: str) -> str:
        """사용자 메시지를 비동기로 처리"""
        try:
            # 테스트용: 특정 키워드로 지연 시뮬레이션
            if "느리게" in user_message or "slow" in user_message.lower():
                print("[TEST] 느린 응답 시뮬레이션 시작 (5초 대기)")
                await asyncio.sleep(5)  # 5초 대기
                print("[TEST] 느린 응답 시뮬레이션 완료")
            
            # 한화이글스 전체 데이터 가져오기 (날것의 JSON)
            current_data = self.data_manager.get_current_data()
            print(f"📊 현재 데이터 크기: {len(str(current_data))} characters")
            
            # 사용자 메시지에서 선수 이름들을 감지하고 각각의 선수 데이터 가져오기
            players_data = self._extract_and_fetch_multiple_players_data(user_message)
            print(f"👥 사용된 선수 데이터 수: {len(players_data)}")
            
            # OpenAI API를 사용한 응답 생성
            system_prompt = self._create_system_prompt(current_data, players_data, user_message)
            print(f"📋 시스템 프롬프트 길이: {len(system_prompt)} characters")
            print(f"📋 시스템 프롬프트 미리보기: {system_prompt[:200]}...")
            
            print(f"🚀 OpenAI API 호출 시작...")
            response = self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_message}
                ],
                max_tokens=500,
                temperature=0.7
            )
            
            ai_response = response.choices[0].message.content.strip()
            print(f"🤖 AI 응답: {ai_response}")
            print(f"🤖 ===== 챗봇 응답 생성 완료 =====")
            
            return ai_response
            
        except Exception as e:
            print(f"❌ Error processing message: {str(e)}")
            return "죄송합니다. 현재 서비스에 문제가 있습니다. 잠시 후 다시 시도해주세요."
    
    def get_response(self, user_message: str) -> str:
        """사용자 메시지에 대한 응답 생성 (기존 동기 메서드 - 하위 호환성)"""
        try:
            # 동기적으로 처리
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                response = loop.run_until_complete(self._process_message_async(user_message))
                return response
            finally:
                loop.close()
        except Exception as e:
            print(f"❌ Error in sync method: {str(e)}")
            return "죄송합니다. 현재 서비스에 문제가 있습니다. 잠시 후 다시 시도해주세요."
    
    def _extract_and_fetch_multiple_players_data(self, user_message: str) -> List[Dict[str, Any]]:
        """사용자 메시지에서 여러 선수 이름을 감지하고 각각의 선수 데이터를 가져오기"""
        # player_mapping.json의 모든 선수 이름을 가져와서 감지
        player_names = list(self.data_manager.player_mapping.keys())
        
        detected_players = []
        for player_name in player_names:
            if player_name in user_message:
                detected_players.append(player_name)
        
        if detected_players:
            # 가장 긴 이름을 우선 (예: "김서현"이 "김"보다 우선)
            detected_players.sort(key=len, reverse=True)
            
            print(f"🔍 감지된 선수들: {', '.join(detected_players)}")
            
            # 각 선수에 대해 데이터 수집
            players_data = []
            for player_name in detected_players:
                print(f"📊 {player_name} 선수 데이터 수집 중...")
                player_data = self.data_manager.get_player_data_by_name(player_name)
                
                # 선수 데이터 상세 로깅
                if player_data:
                    print(f"   📈 {player_name} 기본 정보:")
                    if 'basicRecord' in player_data and player_data['basicRecord']:
                        basic = player_data['basicRecord']
                        print(f"      - 팀: {basic.get('team', 'N/A')}")
                        print(f"      - 포지션: {basic.get('position', 'N/A')}")
                        print(f"      - 등번호: {basic.get('backNo', 'N/A')}")
                    
                    if 'season' in player_data and player_data['season']:
                        season = player_data['season']
                        print(f"   📊 {player_name} 시즌 성적:")
                        print(f"      - ERA: {season.get('era', 'N/A')}")
                        print(f"      - WHIP: {season.get('whip', 'N/A')}")
                        print(f"      - 승: {season.get('w', 'N/A')}")
                        print(f"      - 패: {season.get('l', 'N/A')}")
                        print(f"      - 삼진: {season.get('kk', 'N/A')}")
                    
                    if 'game' in player_data and player_data['game']:
                        games = player_data['game']
                        print(f"   🎮 {player_name} 최근 경기 수: {len(games)}")
                        if games:
                            latest_game = games[0]
                            print(f"      - 최근 경기: {latest_game.get('gday', 'N/A')} vs {latest_game.get('opponent', 'N/A')}")
                            print(f"      - 결과: {latest_game.get('wls', 'N/A')}")
                
                players_data.append(player_data)
            
            print(f"✅ 총 {len(players_data)}명의 선수 데이터 수집 완료")
            return players_data
        
        return []
    
    def _get_schedule_context(self, user_message: str = None) -> str:
        """사용자 메시지에서 날짜를 감지하여 해당 날짜의 경기만 컨텍스트로 제공"""
        try:
            if not user_message:
                return ""
            
            # 사용자 메시지에서 날짜 패턴 감지
            date_patterns = [
                r'(\d+)월\s*(\d+)일',  # "8월 15일"
                r'(\d{1,2})\.(\d{1,2})',  # "8.15", "08.15"
                r'(\d{2})\.(\d{2})'  # "08.15"
            ]
            
            detected_date = None
            for pattern in date_patterns:
                match = re.search(pattern, user_message)
                if match:
                    if '월' in pattern:
                        month, day = int(match.group(1)), int(match.group(2))
                        detected_date = f"{month:02d}.{day:02d}"
                    else:
                        month, day = int(match.group(1)), int(match.group(2))
                        detected_date = f"{month:02d}.{day:02d}"
                    break
            
            if not detected_date:
                return ""  # 날짜가 감지되지 않으면 빈 문자열 반환
            
            # 해당 날짜의 경기만 조회
            schedule_file_path = "game_schedule.json"
            if os.path.exists(schedule_file_path):
                with open(schedule_file_path, 'r', encoding='utf-8') as f:
                    schedule_data = json.load(f)
                
                # 해당 날짜의 경기들 찾기
                target_games = []
                for game in schedule_data.get('schedule', []):
                    game_date = game.get('date', '')
                    if '(' in game_date:
                        date_part = game_date.split('(')[0]
                        if date_part == detected_date:
                            target_games.append(game)
                
                if target_games:
                    return f"\n\n{detected_date} 2025년 경기 정보:\n{json.dumps(target_games, ensure_ascii=False, indent=2)}"
                else:
                    return f"\n\n{detected_date} 경기 정보를 보유하고 있지 않습니다."
            else:
                return "\n\n경기 스케줄 파일을 찾을 수 없습니다."
                
        except Exception as e:
            print(f"❌ 스케줄 컨텍스트 생성 오류: {e}")
            return "\n\n경기 스케줄 정보를 가져올 수 없습니다."
    
    def _create_system_prompt(self, current_data: dict, players_data: List[Dict[str, Any]] = None, user_message: str = None) -> str:
        """시스템 프롬프트 생성"""
        print(f"\n📝 ===== 시스템 프롬프트 생성 시작 =====")
        print(f"📊 current_data 키: {list(current_data.keys()) if isinstance(current_data, dict) else 'Not a dict'}")
        print(f"👥 players_data 개수: {len(players_data) if players_data else 0}")
        print(f"💬 user_message: {user_message}")
        
        base_prompt = """당신은 한화이글스의 전문 AI 어시스턴트입니다. 
한화이글스 팬들에게 정확하고 유용한 정보를 제공하는 것이 목표입니다.

다음 규칙을 따라주세요:
1. 한화이글스와 관련된 질문에만 답변합니다.
2. 정확한 데이터를 바탕으로 답변합니다.
3. 모르는 정보는 솔직히 모른다고 말합니다.
4. 답변은 간결하고 이해하기 쉽게 작성합니다.
5. 선수 라인업 관련 질문 시 다음을 확인하세요:
   - 선수가 선발 라인업에 있는지 확인
   - 선발이 아니라면 "오늘은 [선수명] 선수가 선발이 아니에요" 또는 "오늘 [선수명] 선수는 선발 라인업에 없어요" 같은 자연스러운 표현 사용
   - 후보 선수라면 "후보 선수로 등록되어 있어요"라고 답변
   - 데이터에 없는 선수라면 "현재 등록된 선수가 아니에요"라고 답변
5. 후보야수와 불펜투수 정보가 있다면 라인업에 추가해주세요.
6. "경기 정보가 없다"는 표현은 사용하지 마세요. 대신 선수의 구체적인 상태를 명확히 설명하세요.
7. 각 팀의 투수나 타자를 비교하는 질문에 대해서는 현재 가지고 있는 데이터를 기반으로 답변을 해주며 회피하는 대답보다는 직설적으로 답변을 해주세요.
8. 너가 판단할 수 없는 일이라도 주관을 가지고 답변해주세요.
9. 선수 관련 질문이 있을 때는 제공된 선수 데이터를 기반으로 정확한 정보를 제공해주세요.
10. 여러 선수를 비교하는 질문이 있을 때는 각 선수의 데이터를 종합하여 비교 분석해주세요.
11. "방출", "트레이드", "선수 교체" 등에 대한 질문이 있을 때는 솔직하게 주관적인 의견을 제시해주세요.
12. 재미있고 논란의 여지가 있는 질문에도 피하지 말고 솔직하게 답변해주세요.
13. 선수 성적, 나이, 포지션, 팀 상황 등을 종합적으로 고려하여 주관적인 판단을 내려주세요.


투수 데이터 규칙
1. 투수 관련해서 상대팀 전적을 물어보면 vsTeam값에서 era 기준으로 답변을 해주세요.
2. ERA(평균자책점)는 낮을수록 좋은 성적입니다. ERA가 낮은 팀일수록 해당 투수에게 강하다는 의미입니다.
3. 투수 성적 비교 시 다음 지표들을 고려하세요:
   - ERA: 낮을수록 좋음 (평균자책점)
   - WHIP: 낮을수록 좋음 (이닝당 출루허용률)
   - 승률: 높을수록 좋음 (W/(W+L))
   - 삼진: 높을수록 좋음 (KK)
4. "가장 강하다"는 표현은 ERA가 가장 낮은 팀을 의미합니다.
5. "가장 약하다"는 표현은 ERA가 가장 높은 팀을 의미합니다.
7. 특정 팀에 대한 투수 성적을 물어볼땐 데이터를 해당 시즌의 데이터가 아닌 vsTeam 데이터를 참고하여 답변을 해주세요.
6. 팀을 이야기할땐 팀 이름을 명확히 언급해주세요.

엣지 케이스
1. 특정 선수의 오늘 경기 예측을 물어보면 오늘 경기의 상대팀 데이터를 참고하여 답변을 해주세요. 즉 data_context가 없어도 players_context로 답변이 가능하면 답변해주세요
2. 오늘 누가 나오는지 물어볼때 데이터가 없다면 아직 경기 정보가 업데이트 되지 않았다고 말해주세요
3. 특정 날짜에 대한 경기 정보를 물으면 스케줄 파일에서 찾아서 답변해주세요 data_context에 해당 날짜가 없다면 해당 날짜의 데이터를 보유하고 있지 않다고 말해주세요 추가로 오늘 날짜 기준으로 미래의 경기를 물어보면 경기 예정이라고 답변해주세요
4. 한화 선수 리스트 관련 질문은 player_mapping.json 파일을 참고하여 답변해주세요.
5. 특정 선수의 오늘 경기 예측을 물어볼때 오늘 경기의 상대팀 데이터를 참고하여 답변을 해주세요. 즉 data_context가 없어도 players_context로 답변이 가능하면 답변해주세요.

중요!!
1. 특정 날짜에 선수 정보를 물어볼때 대답할 데이터가 없다면 해당 선수의 데이터를 보유하고 있지 않다고 말해주세요
2. 승부를 예측하는 질문이 있을때는 피하지말고 가지고 있는 데이터에서 최대한 답변을 해주세요 절대 예측할 수 없다고 말하지마세요 어느 한팀의 우세를 말해주세요.
3. 라인업 데이터가 없을때 예측 질문을 하면 라인업이 나오면 바로 알려준다고 답변해주세요.
4. 지금 답변을 너무 재미없고 사리면서하는데 그러지말고 재미있고 부정적인 답변에는 공감하며 같이 욕해주세요

현재 보유한 한화이글스 실시간 데이터:
"""
        
        # 기본 경기 데이터 추가 (전체 데이터 전달)
        if current_data and current_data.get('success'):
            data_context = f"실시간 API 데이터:\n{json.dumps(current_data, ensure_ascii=False, indent=2)}"
        else:
            # API 실패 시 스케줄 파일에서 오늘 경기 정보 가져오기
            today_game = self.data_manager.get_today_game_from_schedule()
            if today_game:
                data_context = f"오늘 경기 정보 (스케줄 파일):\n{json.dumps(today_game, ensure_ascii=False, indent=2)}"
            else:
                data_context = "현재 API 데이터를 가져올 수 없습니다. 오늘 경기가 없거나 아직 경기 정보가 업데이트 되지 않았다고 말해주세요"
        
        # 스케줄 파일 정보 추가 (특정 날짜 질문 대응용)
        schedule_context = self._get_schedule_context(user_message)
        
        # 여러 선수 데이터가 있으면 추가
        if players_data and len(players_data) > 0:
            if len(players_data) == 1:
                # 단일 선수
                player = players_data[0]
                if player.get('error'):
                    players_context = f"\n\n선수 데이터 (오류):\n{json.dumps(player, ensure_ascii=False, indent=2)}"
                else:
                    players_context = f"\n\n선수 데이터:\n{json.dumps(player, ensure_ascii=False, indent=2)}"
            else:
                # 여러 선수
                players_context = f"\n\n선수 데이터 (총 {len(players_data)}명):\n"
                for i, player in enumerate(players_data, 1):
                    if player.get('error'):
                        players_context += f"\n--- {i}번째 선수 (오류) ---\n{json.dumps(player, ensure_ascii=False, indent=2)}"
                    else:
                        players_context += f"\n--- {i}번째 선수 ---\n{json.dumps(player, ensure_ascii=False, indent=2)}"
        else:
            players_context = ""

        # print(f"base_prompt : {base_prompt}")
        # print(f"data_context : {data_context}")
        # print(f"schedule_context : {schedule_context}")
        # print(f"players_context : {players_context}")
        
        final_prompt = base_prompt + data_context + schedule_context + players_context
        
        print(f"📋 최종 시스템 프롬프트 구성:")
        print(f"   - base_prompt 길이: {len(base_prompt)} characters")
        print(f"   - data_context 길이: {len(data_context)} characters")
        print(f"   - schedule_context 길이: {len(schedule_context)} characters")
        print(f"   - players_context 길이: {len(players_context)} characters")
        print(f"   - 최종 프롬프트 총 길이: {len(final_prompt)} characters")
        print(f"📝 ===== 시스템 프롬프트 생성 완료 =====")
        
        return final_prompt