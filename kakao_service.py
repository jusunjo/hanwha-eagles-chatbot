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
    
    async def process_kakao_request(self, request_data: Dict[str, Any]):
        """
        Process Kakao request with immediate response.
        
        Args:
            request_data: The parsed request data from Kakao
            
        Returns:
            Dict containing the response for Kakao
        """
        try:
            print(f"[KAKAO] 카카오톡 챗봇 요청 시작")
            
            # 사용자 정보 및 파라미터 추출
            if 'userRequest' not in request_data:
                raise KeyError("userRequest field not found")
            
            user_request = request_data['userRequest']
            
            # callbackUrl 확인
            callback_url = user_request.get('callbackUrl')
            
            if 'user' not in user_request or 'utterance' not in user_request:
                raise KeyError("Required fields not found")
            
            utterance = user_request['utterance']
            
            # action.params.message에서 실제 질문 추출
            question = utterance
            if 'action' in request_data and 'params' in request_data['action']:
                params = request_data['action']['params']
                if 'message' in params:
                    question = params['message']
            
            # 즉시 응답 처리
            response = await self._process_immediate_response(question, callback_url)
            
            print(f"[KAKAO] 카카오톡 챗봇 요청 완료")
            return response
            
        except Exception as e:
            print(f"[KAKAO-ERROR] 예외 발생: {str(e)}")
            
            # 에러 응답
            error_response = {
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
            return error_response
    
    async def _process_immediate_response(self, question: str, callback_url: str = None) -> Dict[str, Any]:
        """즉시 응답 처리"""
        try:
            print(f"[KAKAO-IMMEDIATE] 즉시 응답 처리 시작")
            
            # 챗봇 서비스 호출
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(None, self.chatbot.get_response, question)
            
            if result:
                response_text = result
            else:
                response_text = "AI 처리 중 오류가 발생했어요. 다시 시도해주세요."
            
            # 카카오톡 챗봇 응답 구조
            immediate_response = {
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
            
            # callback_url이 있으면 해당 URL로 API 요청 보내기
            if callback_url:
                print(f"[KAKAO-CALLBACK] callback URL로 API 요청 전송")
                await self._send_callback_request(callback_url, immediate_response)
            
            print(f"[KAKAO-IMMEDIATE] 즉시 응답 처리 완료")
            return immediate_response
            
        except Exception as e:
            print(f"[KAKAO-IMMEDIATE-ERROR] 즉시 응답 처리 오류: {str(e)}")
            
            # 에러 시에도 정확한 스키마로 응답
            error_response = {
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
            return error_response
    
    async def _send_callback_request(self, callback_url: str, response_data: Dict[str, Any]):
        """callback_url로 API 요청을 보내는 메서드"""
        try:
            print(f"[KAKAO-CALLBACK] callback API 요청 시작: {callback_url}")
            
            # httpx를 사용하여 비동기 HTTP POST 요청
            async with httpx.AsyncClient(timeout=30.0) as client:
                headers = {
                    "Content-Type": "application/json",
                }
                
                response = await client.post(
                    callback_url,
                    json=response_data,
                    headers=headers
                )
                
                if response.status_code == 200:
                    print(f"[KAKAO-CALLBACK] callback API 요청 성공")
                else:
                    print(f"[KAKAO-CALLBACK] callback API 요청 실패 - 상태 코드: {response.status_code}")
                    
        except httpx.TimeoutException:
            print(f"[KAKAO-CALLBACK-ERROR] callback API 요청 타임아웃")
        except httpx.RequestError as e:
            print(f"[KAKAO-CALLBACK-ERROR] callback API 요청 오류: {e}")
        except Exception as e:
            print(f"[KAKAO-CALLBACK-ERROR] callback API 요청 예외: {str(e)}")


# Create a singleton instance
kakao_service = KakaoService()
