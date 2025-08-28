"""
Kakao service for handling Kakao chatbot requests with Hanwha Eagles data.
"""

import json
import asyncio
from typing import Dict, Any
from chatbot_service import HanwhaEaglesChatbot


class KakaoService:
    """Service for handling Kakao chatbot requests with Hanwha Eagles data."""
    
    def __init__(self):
        self.chatbot = HanwhaEaglesChatbot()
    
    async def process_kakao_request(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process Kakao request with Hanwha Eagles chatbot.
        
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
                
                print(f"[DEBUG] 카카오톡 형식 - 사용자 ID: {user_id}")
                print(f"[DEBUG] 카카오톡 형식 - 전체 발화문: {utterance}")
                
                return await self._process_kakao_format(utterance)
                
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
    
    async def _process_kakao_format(self, utterance: str) -> Dict[str, Any]:
        """카카오톡 형식 처리 (콜백 없이 즉시 응답)"""
        try:
            print(f"[DEBUG] 카카오톡 형식 처리 시작 - 질문: {utterance}")
            
            # 챗봇 서비스 호출 (동기 함수를 비동기로 실행)
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(None, self.chatbot.get_response, utterance)
            
            print(f"[DEBUG] 챗봇 답변: {result}")
            
            # 즉시 응답 (카카오톡 형식)
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
