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
            
            # 사용자 정보 및 파라미터 추출
            user_id = request_data['userRequest']['user']['id']
            utterance = request_data['userRequest']['utterance']
            callback_url = request_data['userRequest'].get('callbackUrl')
            
            # action.params.message에서 실제 질문 추출
            question = utterance
            if 'action' in request_data and 'params' in request_data['action']:
                params = request_data['action']['params']
                if 'message' in params:
                    question = params['message']
            
            print(f"[DEBUG] 사용자 ID: {user_id}")
            print(f"[DEBUG] 전체 발화문: {utterance}")
            print(f"[DEBUG] 최종 질문: {question}")
            print(f"[DEBUG] 콜백 URL: {callback_url}")
            
            # 백그라운드에서 실제 챗봇 작업을 처리하는 함수
            async def process_chatbot_background():
                try:
                    print(f"[BACKGROUND] 백그라운드 챗봇 처리 시작 - 사용자: {user_id}, 질문: {question}")
                    
                    # 챗봇 서비스 호출
                    loop = asyncio.get_event_loop()
                    result = await loop.run_in_executor(None, self.chatbot.get_response, question)
                    
                    if result:
                        response_text = result
                        print(f"[BACKGROUND] 챗봇 답변 생성 완료: {response_text}")
                    else:
                        print(f"[BACKGROUND] 챗봇 처리 실패 - 빈 응답")
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
                    loop.run_in_executor(None, self.chatbot.get_response, question),
                    timeout=4.0
                )
                
                # 4초 이내에 결과가 나온 경우
                print("[SUCCESS] 4초 이내에 결과 완료")
                background_task.cancel()  # 백그라운드 태스크 취소
                
                if result:
                    response_text = result
                    print(f"[DEBUG] 챗봇 답변: {response_text}")
                else:
                    print(f"[ERROR] 챗봇 처리 실패 - 빈 응답")
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
            print(f"[ERROR] 예외 발생: {str(e)}")
            print(f"[ERROR] 예외 타입: {type(e).__name__}")
            
            # 에러 발생 시 콜백으로 에러 메시지 전송
            try:
                callback_url = request_data.get('userRequest', {}).get('callbackUrl')
                if callback_url:
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


# Create a singleton instance
kakao_service = KakaoService()
