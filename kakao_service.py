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
            
            # callbackUrl이 없는 경우 즉시 응답으로 처리
            if 'callbackUrl' not in request_data.get('userRequest', {}):
                print(f"[KAKAO] callbackUrl 없음 - 즉시 응답 처리로 전환")
                response = await self._process_immediate_response(question)
                print(f"[KAKAO] 즉시 응답 완료: {response}")
                return response
            
            callback_url = request_data['userRequest']['callbackUrl']
            print(f"[KAKAO] callbackUrl 발견: {callback_url}")
            print(f"[KAKAO] 백그라운드 처리 + 콜백 모드로 전환")
            
            # 백그라운드에서 실제 챗봇 작업을 처리하는 함수
            async def process_chatbot_background():
                try:
                    print(f"[KAKAO-BG] 백그라운드 챗봇 처리 시작")
                    print(f"[KAKAO-BG] 사용자: {user_id}, 질문: {question}")
                    
                    # 챗봇 서비스 호출 (동기 함수를 비동기로 실행)
                    loop = asyncio.get_event_loop()
                    print(f"[KAKAO-BG] 챗봇 서비스 호출 시작")
                    result = await loop.run_in_executor(None, self.chatbot.get_response, question)
                    
                    if result:
                        print(f"[KAKAO-BG] 챗봇 답변 생성 성공: {result[:100]}...")
                        response_text = result
                    else:
                        print(f"[KAKAO-BG] 챗봇 처리 실패 - 빈 응답")
                        response_text = "AI 처리 중 오류가 발생했어요. 다시 시도해주세요."
                    
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
                    
                    print(f"[KAKAO-BG] 콜백 응답 준비 완료")
                    print(f"[KAKAO-BG] 콜백 URL로 전송 시작: {callback_url}")
                    
                    async with httpx.AsyncClient(timeout=60.0) as client:
                        response = await client.post(
                            callback_url,
                            json=final_callback_response,
                            headers={"Content-Type": "application/json"}
                        )
                        print(f"[KAKAO-BG] 콜백 전송 완료 - 상태코드: {response.status_code}")
                        print(f"[KAKAO-BG] 콜백 응답 헤더: {dict(response.headers)}")
                        
                except Exception as e:
                    print(f"[KAKAO-BG-ERROR] 백그라운드 처리 중 오류: {str(e)}")
                    print(f"[KAKAO-BG-ERROR] 오류 타입: {type(e).__name__}")
                    
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
                        
                        print(f"[KAKAO-BG-ERROR] 에러 콜백 전송 시도")
                        async with httpx.AsyncClient(timeout=60.0) as client:
                            await client.post(
                                callback_url,
                                json=error_callback_response,
                                headers={"Content-Type": "application/json"}
                            )
                            print(f"[KAKAO-BG-ERROR] 에러 콜백 전송 성공")
                    except Exception as callback_error:
                        print(f"[KAKAO-BG-ERROR] 에러 콜백 전송 실패: {str(callback_error)}")
            
            # 백그라운드에서 챗봇 작업 시작
            print(f"[KAKAO] 백그라운드 태스크 생성 시작")
            background_task = asyncio.create_task(process_chatbot_background())
            print(f"[KAKAO] 백그라운드 태스크 생성 완료")
            
            # 4초 대기 (빠른 응답인지 확인)
            print(f"[KAKAO] 4초 타임아웃 대기 시작")
            try:
                # 4초 동안 챗봇 작업이 완료되는지 기다림
                loop = asyncio.get_event_loop()
                result = await asyncio.wait_for(
                    loop.run_in_executor(None, self.chatbot.get_response, question),
                    timeout=4.0
                )
                
                # 4초 이내에 결과가 나온 경우
                print(f"[KAKAO] 4초 이내에 결과 완료 - 백그라운드 태스크 취소")
                background_task.cancel()  # 백그라운드 태스크 취소
                
                if result:
                    print(f"[KAKAO] 챗봇 답변 성공: {result[:100]}...")
                    response_text = result
                else:
                    print(f"[KAKAO] 챗봇 처리 실패 - 빈 응답")
                    response_text = "AI 처리 중 오류가 발생했어요. 다시 시도해주세요."
                
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
                
                # 응답 검증 및 로깅
                print(f"[KAKAO] 즉시 응답 검증:")
                print(f"[KAKAO] - version: {immediate_response.get('version')}")
                print(f"[KAKAO] - template 존재: {'template' in immediate_response}")
                print(f"[KAKAO] - outputs 존재: {'outputs' in immediate_response.get('template', {})}")
                print(f"[KAKAO] - outputs 길이: {len(immediate_response.get('template', {}).get('outputs', []))}")
                print(f"[KAKAO] - simpleText 존재: {'simpleText' in immediate_response.get('template', {}).get('outputs', [{}])[0]}")
                print(f"[KAKAO] - text 길이: {len(immediate_response.get('template', {}).get('outputs', [{}])[0].get('simpleText', {}).get('text', ''))}")
                
                print(f"[KAKAO] 즉시 응답 생성 완료")
                print(f"[KAKAO] ===== 카카오톡 챗봇 요청 완료 (즉시 응답) =====")
                return immediate_response
                
            except asyncio.TimeoutError:
                # 4초가 지나서 타임아웃된 경우
                print(f"[KAKAO] 4초 타임아웃 발생 - 백그라운드 처리로 전환")
                
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
                
                print(f"[KAKAO] 대기 메시지 응답 생성 완료")
                print(f"[KAKAO] ===== 카카오톡 챗봇 요청 완료 (백그라운드 처리) =====")
                return waiting_response
            
        except Exception as e:
            print(f"[KAKAO-ERROR] 예외 발생: {str(e)}")
            print(f"[KAKAO-ERROR] 예외 타입: {type(e).__name__}")
            print(f"[KAKAO-ERROR] 예외 상세: {e}")
            
            # 에러 발생 시 콜백으로 에러 메시지 전송 (callbackUrl이 있는 경우만)
            try:
                callback_url = request_data.get('userRequest', {}).get('callbackUrl')
                if callback_url:
                    print(f"[KAKAO-ERROR] 에러 콜백 전송 시도: {callback_url}")
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
                        print(f"[KAKAO-ERROR] 에러 콜백 전송 성공")
            except Exception as callback_error:
                print(f"[KAKAO-ERROR] 에러 콜백 전송 실패: {str(callback_error)}")
            
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
        """즉시 응답 처리 (callbackUrl이 없는 경우)"""
        try:
            print(f"[KAKAO-IMMEDIATE] 즉시 응답 처리 시작")
            print(f"[KAKAO-IMMEDIATE] 질문: {question}")
            
            # 챗봇 서비스 호출 (동기 함수를 비동기로 실행)
            loop = asyncio.get_event_loop()
            print(f"[KAKAO-IMMEDIATE] 챗봇 서비스 호출 시작")
            result = await loop.run_in_executor(None, self.chatbot.get_response, question)
            
            if result:
                print(f"[KAKAO-IMMEDIATE] 챗봇 답변 성공: {result[:100]}...")
                response_text = result
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
