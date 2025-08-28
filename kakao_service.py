"""
Kakao service for handling Kakao chatbot requests with Hanwha Eagles data.
"""

import json
import asyncio
import httpx
from typing import Dict, Any
from chatbot_service import HanwhaEaglesChatbot


class KakaoService:
    """Service for handling Kakao chatbot requests with Hanwha Eagles data."""
    
    def __init__(self):
        self.chatbot = HanwhaEaglesChatbot()
    
    async def process_kakao_request(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process Kakao request with background processing and callback support.
        
        Args:
            request_data: The parsed request data from Kakao
            
        Returns:
            Dict containing the response for Kakao
        """
        try:
            print(f"[REQUEST] /kakao 엔드포인트 호출됨")
            print(f"[DEBUG] 받은 요청 데이터: {json.dumps(request_data, ensure_ascii=False, indent=2)}")
            
            # 카카오톡 형식인지 확인
            if 'userRequest' in request_data:
                # 카카오톡 형식
                user_id = request_data['userRequest']['user']['id']
                utterance = request_data['userRequest']['utterance']
                callback_url = request_data['userRequest']['callbackUrl']
                
                print(f"[DEBUG] 카카오톡 형식 - 사용자 ID: {user_id}")
                print(f"[DEBUG] 카카오톡 형식 - 전체 발화문: {utterance}")
                print(f"[DEBUG] 카카오톡 형식 - 콜백 URL: {callback_url}")
                
                return await self._process_kakao_format(utterance, callback_url)
                
            elif 'message' in request_data:
                # 간단한 메시지 형식
                user_message = request_data['message']
                print(f"[DEBUG] 간단한 메시지 형식: {user_message}")
                
                return await self._process_simple_message(user_message)
                
            else:
                # 지원하지 않는 형식
                print(f"[ERROR] 지원하지 않는 요청 형식")
                return {
                    "version": "2.0",
                    "template": {
                        "outputs": [
                            {
                                "simpleText": {
                                    "text": "지원하지 않는 요청 형식입니다."
                                }
                            }
                        ]
                    }
                }
            
        except Exception as e:
            print(f"[ERROR] 예외 발생: {str(e)}")
            print(f"[ERROR] 예외 타입: {type(e).__name__}")
            
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
    
    async def _process_simple_message(self, user_message: str) -> Dict[str, Any]:
        """간단한 메시지 형식 처리"""
        try:
            # 챗봇 응답 생성
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(None, self.chatbot.get_response, user_message)
            
            print(f"[DEBUG] 챗봇 답변: {result}")
            
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
            
        except Exception as e:
            print(f"[ERROR] 간단한 메시지 처리 중 오류: {str(e)}")
            return {
                "version": "2.0",
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
    
    async def _process_kakao_format(self, utterance: str, callback_url: str) -> Dict[str, Any]:
        """카카오톡 형식 처리 (기존 로직)"""
        try:
            # 백그라운드에서 실제 챗봇 작업을 처리하는 함수
            async def process_chatbot_background():
                try:
                    print(f"[BACKGROUND] 백그라운드 챗봇 처리 시작 - 질문: {utterance}")
                    
                    # 챗봇 서비스 호출 (동기 함수를 비동기로 실행)
                    loop = asyncio.get_event_loop()
                    result = await loop.run_in_executor(None, self.chatbot.get_response, utterance)
                    
                    print(f"[BACKGROUND] 챗봇 답변 생성 완료: {result}")
                    response_text = result
                    
                    # 최종 결과를 콜백으로 전송
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
            
            # 백그라운드에서 챗봇 작업 시작
            background_task = asyncio.create_task(process_chatbot_background())
            
            # 4초 대기 (빠른 응답인지 확인)
            try:
                # 4초 동안 챗봇 작업이 완료되는지 기다림
                loop = asyncio.get_event_loop()
                result = await asyncio.wait_for(
                    loop.run_in_executor(None, self.chatbot.get_response, utterance),
                    timeout=4.0
                )
                
                # 4초 이내에 결과가 나온 경우
                print("[SUCCESS] 4초 이내에 결과 완료")
                background_task.cancel()  # 백그라운드 태스크 취소
                
                print(f"[DEBUG] 챗봇 답변: {result}")
                response_text = result
                
                # 즉시 응답
                immediate_response = {
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
                
                print(f"[DEBUG] 즉시 응답 데이터: {json.dumps(immediate_response, ensure_ascii=False, indent=2)}")
                return immediate_response
                
            except asyncio.TimeoutError:
                # 4초가 지나서 타임아웃된 경우
                print("[INFO] 4초 타임아웃 - 백그라운드 처리로 전환")
                
                # 즉시 "기다리는 메시지" 응답
                waiting_response = {
                    "version": "2.0",
                    "useCallback": True,
                    "template": {
                        "outputs": [
                            {
                                "simpleText": {
                                    "text": "답변을 입력중입니다 . . ."
                                }
                            }
                        ]
                    }
                }
                
                print(f"[DEBUG] 대기 메시지 응답: {json.dumps(waiting_response, ensure_ascii=False, indent=2)}")
                return waiting_response
                
        except Exception as e:
            print(f"[ERROR] 카카오톡 형식 처리 중 오류: {str(e)}")
            return {
                "version": "2.0",
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


# Create a singleton instance
kakao_service = KakaoService()
