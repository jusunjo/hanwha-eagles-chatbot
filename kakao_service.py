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
    
    async def process_kakao_request(self, request_data: Dict[str, Any]):
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
            print(f"[KAKAO] 요청 데이터 타입: {type(request_data)}")
            print(f"[KAKAO] 요청 데이터 키: {list(request_data.keys())}")
            
            # 사용자 정보 및 파라미터 추출
            if 'userRequest' not in request_data:
                print(f"[KAKAO-ERROR] userRequest 필드가 없음")
                raise KeyError("userRequest field not found")
            
            user_request = request_data['userRequest']
            print(f"[KAKAO] userRequest 키: {list(user_request.keys())}")
            
            if 'user' not in user_request:
                print(f"[KAKAO-ERROR] user 필드가 없음")
                raise KeyError("user field not found")
            
            if 'utterance' not in user_request:
                print(f"[KAKAO-ERROR] utterance 필드가 없음")
                raise KeyError("utterance field not found")
            
            user_id = user_request['user']['id']
            utterance = user_request['utterance']
            
            print(f"[KAKAO] 사용자 정보:")
            print(f"[KAKAO] - 사용자 ID: {user_id}")
            print(f"[KAKAO] - 사용자 ID 타입: {type(user_id)}")
            print(f"[KAKAO] - 전체 발화문: {utterance}")
            print(f"[KAKAO] - 발화문 타입: {type(utterance)}")
            print(f"[KAKAO] - 발화문 길이: {len(utterance) if utterance else 0}")
            
            # action.params.message에서 실제 질문 추출
            print(f"[KAKAO] action 파라미터 분석:")
            if 'action' in request_data:
                action = request_data['action']
                print(f"[KAKAO] - action 존재: True")
                print(f"[KAKAO] - action 키: {list(action.keys())}")
                
                if 'params' in action:
                    params = action['params']
                    print(f"[KAKAO] - params 존재: True")
                    print(f"[KAKAO] - params 키: {list(params.keys())}")
                    
                    if 'message' in params:
                        question = params['message']
                        print(f"[KAKAO] - message 파라미터 발견: {question}")
                        print(f"[KAKAO] - message 타입: {type(question)}")
                        print(f"[KAKAO] - message 길이: {len(question) if question else 0}")
                    else:
                        print(f"[KAKAO] - message 파라미터 없음, utterance 사용")
                        question = utterance
                else:
                    print(f"[KAKAO] - params 없음, utterance 사용")
                    question = utterance
            else:
                print(f"[KAKAO] - action 없음, utterance 사용")
                question = utterance
            
            print(f"[KAKAO] 최종 질문 결정:")
            print(f"[KAKAO] - 최종 질문: {question}")
            print(f"[KAKAO] - 질문 타입: {type(question)}")
            print(f"[KAKAO] - 질문 길이: {len(question) if question else 0}")
            
            # 즉시 응답 처리
            print(f"[KAKAO] 즉시 응답 처리로 전환")
            response = await self._process_immediate_response(question)
            
            print(f"[KAKAO] 응답 생성 완료:")
            print(f"[KAKAO] - 응답 타입: {type(response)}")
            print(f"[KAKAO] - 응답 키: {list(response.keys()) if isinstance(response, dict) else 'Not a dict'}")
            print(f"[KAKAO] - 응답 내용: {response}")
            
            print(f"[KAKAO] ===== 카카오톡 챗봇 요청 완료 =====")
            return response
            
        except Exception as e:
            print(f"[KAKAO-ERROR] 예외 발생: {str(e)}")
            print(f"[KAKAO-ERROR] 예외 타입: {type(e).__name__}")
            print(f"[KAKAO-ERROR] 예외 상세: {e}")
            print(f"[KAKAO-ERROR] 예외 추적:")
            import traceback
            traceback.print_exc()
            
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
            print(f"[KAKAO-IMMEDIATE] ===== 즉시 응답 처리 시작 =====")
            print(f"[KAKAO-IMMEDIATE] 입력 질문: {question}")
            print(f"[KAKAO-IMMEDIATE] 질문 타입: {type(question)}")
            print(f"[KAKAO-IMMEDIATE] 질문 길이: {len(question) if question else 0}")
            
            # 챗봇 서비스 호출 (동기 함수를 비동기로 실행)
            print(f"[KAKAO-IMMEDIATE] 챗봇 서비스 호출 시작")
            loop = asyncio.get_event_loop()
            print(f"[KAKAO-IMMEDIATE] 이벤트 루프: {loop}")
            
            print(f"[KAKAO-IMMEDIATE] run_in_executor 호출 시작")
            result = await loop.run_in_executor(None, self.chatbot.get_response, question)
            print(f"[KAKAO-IMMEDIATE] run_in_executor 호출 완료")
            
            print(f"[KAKAO-IMMEDIATE] 챗봇 응답 결과:")
            print(f"[KAKAO-IMMEDIATE] - 결과 타입: {type(result)}")
            print(f"[KAKAO-IMMEDIATE] - 결과 내용: {result}")
            print(f"[KAKAO-IMMEDIATE] - 결과 길이: {len(result) if result else 0}")
            
            if result:
                print(f"[KAKAO-IMMEDIATE] 챗봇 답변 성공")
                # 줄바꿈 문자를 공백으로 변경 (카카오톡 호환성)
                response_text = result.replace('\n', ' ')
                print(f"[KAKAO-IMMEDIATE] 줄바꿈 제거 후 텍스트: {response_text}")
            else:
                print(f"[KAKAO-IMMEDIATE] 챗봇 처리 실패 - 빈 응답")
                response_text = "AI 처리 중 오류가 발생했어요. 다시 시도해주세요."
            
            print(f"[KAKAO-IMMEDIATE] 최종 응답 텍스트:")
            print(f"[KAKAO-IMMEDIATE] - 텍스트: {response_text}")
            print(f"[KAKAO-IMMEDIATE] - 텍스트 타입: {type(response_text)}")
            print(f"[KAKAO-IMMEDIATE] - 텍스트 길이: {len(response_text)}")
            
            # 카카오톡 챗봇 정확한 스키마에 맞춘 응답
            print(f"[KAKAO-IMMEDIATE] 응답 구조 생성 시작")
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
            
            print(f"[KAKAO-IMMEDIATE] 응답 구조 생성 완료")
            print(f"[KAKAO-IMMEDIATE] 생성된 응답: {immediate_response}")
            
            # 응답 검증 및 로깅
            print(f"[KAKAO-IMMEDIATE] ===== 응답 검증 시작 =====")
            print(f"[KAKAO-IMMEDIATE] - version: {immediate_response.get('version')}")
            print(f"[KAKAO-IMMEDIATE] - version 타입: {type(immediate_response.get('version'))}")
            print(f"[KAKAO-IMMEDIATE] - template 존재: {'template' in immediate_response}")
            print(f"[KAKAO-IMMEDIATE] - template 타입: {type(immediate_response.get('template'))}")
            print(f"[KAKAO-IMMEDIATE] - outputs 존재: {'outputs' in immediate_response.get('template', {})}")
            print(f"[KAKAO-IMMEDIATE] - outputs 타입: {type(immediate_response.get('template', {}).get('outputs'))}")
            print(f"[KAKAO-IMMEDIATE] - outputs 길이: {len(immediate_response.get('template', {}).get('outputs', []))}")
            
            if immediate_response.get('template', {}).get('outputs'):
                first_output = immediate_response['template']['outputs'][0]
                print(f"[KAKAO-IMMEDIATE] - 첫 번째 output: {first_output}")
                print(f"[KAKAO-IMMEDIATE] - 첫 번째 output 타입: {type(first_output)}")
                print(f"[KAKAO-IMMEDIATE] - simpleText 존재: {'simpleText' in first_output}")
                print(f"[KAKAO-IMMEDIATE] - simpleText 타입: {type(first_output.get('simpleText'))}")
                
                if first_output.get('simpleText'):
                    simple_text = first_output['simpleText']
                    print(f"[KAKAO-IMMEDIATE] - text 존재: {'text' in simple_text}")
                    print(f"[KAKAO-IMMEDIATE] - text 값: {simple_text.get('text')}")
                    print(f"[KAKAO-IMMEDIATE] - text 타입: {type(simple_text.get('text'))}")
                    print(f"[KAKAO-IMMEDIATE] - text 길이: {len(simple_text.get('text', ''))}")
            
            print(f"[KAKAO-IMMEDIATE] ===== 응답 검증 완료 =====")
            print(f"[KAKAO-IMMEDIATE] 즉시 응답 생성 완료")
            print(f"[KAKAO-IMMEDIATE] ===== 즉시 응답 처리 완료 =====")
            return immediate_response
            
        except Exception as e:
            print(f"[KAKAO-IMMEDIATE-ERROR] 즉시 응답 처리 오류: {str(e)}")
            print(f"[KAKAO-IMMEDIATE-ERROR] 오류 타입: {type(e).__name__}")
            print(f"[KAKAO-IMMEDIATE-ERROR] 오류 상세: {e}")
            print(f"[KAKAO-IMMEDIATE-ERROR] 오류 추적:")
            import traceback
            traceback.print_exc()
            
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
            print(f"[KAKAO-IMMEDIATE-ERROR] 에러 응답 생성 완료")
            return error_response


# Create a singleton instance
kakao_service = KakaoService()
