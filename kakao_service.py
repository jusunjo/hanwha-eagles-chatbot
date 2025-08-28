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
        Process Kakao request with immediate response.
        
        Args:
            request_data: The parsed request data from Kakao
            
        Returns:
            Dict containing the response for Kakao
        """
        try:
            print(f"[KAKAO] ===== 카카오톡 챗봇 요청 시작 =====")
            print(f"[KAKAO] 요청 시간: {asyncio.get_event_loop().time()}")
            
            # 사용자 정보 및 파라미터 추출
            user_id = request_data['userRequest']['user']['id']
            utterance = request_data['userRequest']['utterance']
            
            # action.params.message에서 실제 질문 추출
            if 'action' in request_data and 'params' in request_data['action']:
                question = request_data['action']['params'].get('message', utterance)
                print(f"[KAKAO] action.params.message에서 질문 추출: {question}")
            else:
                question = utterance
                print(f"[KAKAO] utterance에서 질문 추출: {question}")
            
            print(f"[KAKAO] 사용자 ID: {user_id}")
            print(f"[KAKAO] 전체 발화문: {utterance}")
            print(f"[KAKAO] 최종 질문: {question}")
            
            # 즉시 응답 처리
            print(f"[KAKAO] 즉시 응답 처리로 전환")
            response = await self._process_immediate_response(question)
            print(f"[KAKAO] 즉시 응답 완료: {response}")
            print(f"[KAKAO] ===== 카카오톡 챗봇 요청 완료 =====")
            return response
            
        except Exception as e:
            print(f"[KAKAO-ERROR] 예외 발생: {str(e)}")
            print(f"[KAKAO-ERROR] 예외 타입: {type(e).__name__}")
            print(f"[KAKAO-ERROR] 예외 상세: {e}")
            
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
            print(f"[KAKAO-ERROR] 에러 응답 생성 완료")
            print(f"[KAKAO] ===== 카카오톡 챗봇 요청 실패 =====")
            return error_response
    
    async def _process_immediate_response(self, question: str) -> Dict[str, Any]:
        """즉시 응답 처리"""
        try:
            print(f"[KAKAO-IMMEDIATE] 즉시 응답 처리 시작")
            print(f"[KAKAO-IMMEDIATE] 질문: {question}")
            
            # 챗봇 서비스 호출 (동기 함수를 비동기로 실행)
            loop = asyncio.get_event_loop()
            print(f"[KAKAO-IMMEDIATE] 챗봇 서비스 호출 시작")
            result = await loop.run_in_executor(None, self.chatbot.get_response, question)
            
            if result:
                print(f"[KAKAO-IMMEDIATE] 챗봇 답변 성공: {result[:100]}...")
                # 줄바꿈 문자를 공백으로 변경 (카카오톡 호환성)
                response_text = result.replace('\n', ' ')
            else:
                print(f"[KAKAO-IMMEDIATE] 챗봇 처리 실패 - 빈 응답")
                response_text = "AI 처리 중 오류가 발생했어요. 다시 시도해주세요."
            
            # 카카오톡 챗봇 정확한 스키마에 맞춘 응답
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
            
            # 응답 검증 및 로깅
            print(f"[KAKAO-IMMEDIATE] 응답 검증:")
            print(f"[KAKAO-IMMEDIATE] - version: {immediate_response.get('version')}")
            print(f"[KAKAO-IMMEDIATE] - template 존재: {'template' in immediate_response}")
            print(f"[KAKAO-IMMEDIATE] - outputs 존재: {'outputs' in immediate_response.get('template', {})}")
            print(f"[KAKAO-IMMEDIATE] - outputs 길이: {len(immediate_response.get('template', {}).get('outputs', []))}")
            print(f"[KAKAO-IMMEDIATE] - simpleText 존재: {'simpleText' in immediate_response.get('template', {}).get('outputs', [{}])[0]}")
            print(f"[KAKAO-IMMEDIATE] - text 길이: {len(immediate_response.get('template', {}).get('outputs', [{}])[0].get('simpleText', {}).get('text', ''))}")
            
            print(f"[KAKAO-IMMEDIATE] 즉시 응답 생성 완료")
            return immediate_response
            
        except Exception as e:
            print(f"[KAKAO-IMMEDIATE-ERROR] 즉시 응답 처리 오류: {str(e)}")
            print(f"[KAKAO-IMMEDIATE-ERROR] 오류 타입: {type(e).__name__}")
            
            # 에러 시에도 정확한 스키마로 응답
            error_response = {
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
            return error_response


# Create a singleton instance
kakao_service = KakaoService()
