"""
Kakao service for handling Kakao chatbot requests with Hanwha Eagles data.
"""

import json
import asyncio
import httpx
from typing import Dict, Any
from rag.rag_text_to_sql import RAGTextToSQL


class KakaoService:
    """Service for handling Kakao chatbot requests with Hanwha Eagles data."""
    
    def __init__(self):
        print("🔄 KakaoService 초기화 시작...")
        print("🔄 RAGTextToSQL 인스턴스 생성 중... (이 과정에서 텐서플로우 모델 훈련이 진행됩니다)")
        self.text_to_sql = RAGTextToSQL()
        print("✅ KakaoService 초기화 완료")
    
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
            
            # 요청 데이터 형식 확인 및 처리
            if 'userRequest' in request_data:
                # 카카오톡 형식
                print(f"[DEBUG] 카카오톡 형식 감지")
                user_id = request_data['userRequest']['user']['id']
                utterance = request_data['userRequest']['utterance']
                callback_url = request_data['userRequest']['callbackUrl']
                
                # action.params.message에서 실제 질문 추출
                question = utterance
                if 'action' in request_data and 'params' in request_data['action']:
                    params = request_data['action']['params']
                    if 'message' in params:
                        question = params['message']
                        
            elif 'message' in request_data:
                # 간단한 메시지 형식
                print(f"[DEBUG] 간단한 메시지 형식 감지")
                user_id = "simple_user"
                question = request_data['message']
                callback_url = request_data.get('callback_url') or request_data.get('callbackUrl')
                utterance = question
                
            else:
                # 지원하지 않는 형식
                print(f"[ERROR] 지원하지 않는 요청 형식")
                print(f"[ERROR] 요청 키: {list(request_data.keys())}")
                raise ValueError("지원하지 않는 요청 형식입니다.")
            
            print(f"[DEBUG] 사용자 ID: {user_id}")
            print(f"[DEBUG] 전체 발화문: {utterance}")
            print(f"[DEBUG] 파라미터로 받은 질문: {question}")
            print(f"[DEBUG] 콜백 URL: {callback_url}")
            
            # 백그라운드에서 실제 챗봇 작업을 처리하는 함수
            async def process_chatbot_background():
                print(f"[BACKGROUND] ===== 백그라운드 함수 진입 =====")
                print(f"[BACKGROUND] 함수 시작 시간: {asyncio.get_event_loop().time()}")
                
                try:
                    print(f"[BACKGROUND] 백그라운드 챗봇 처리 시작 - 사용자: {user_id}, 질문: {question}")
                    
                    # Text-to-SQL 서비스 호출
                    loop = asyncio.get_event_loop()
                    result = await loop.run_in_executor(None, self.text_to_sql.process_question, question)
                    
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
                    
                    print(f"[BACKGROUND] ===== 콜백 API 호출 시작 =====")
                    print(f"[BACKGROUND] 콜백 URL: {callback_url}")
                    print(f"[BACKGROUND] 콜백 데이터: {json.dumps(final_callback_response, ensure_ascii=False, indent=2)}")
                    
                    try:
                        async with httpx.AsyncClient(timeout=60.0) as client:
                            print(f"[BACKGROUND] HTTP 클라이언트 생성 완료")
                            print(f"[BACKGROUND] POST 요청 전송 중...")
                            
                            response = await client.post(
                                callback_url,
                                json=final_callback_response,
                                headers={"Content-Type": "application/json"}
                            )
                            
                            print(f"[BACKGROUND] ===== 콜백 API 호출 완료 =====")
                            print(f"[BACKGROUND] 상태코드: {response.status_code}")
                            print(f"[BACKGROUND] 응답 헤더: {dict(response.headers)}")
                            print(f"[BACKGROUND] 응답 내용: {response.text}")
                            
                            if response.status_code == 200:
                                print(f"[BACKGROUND] ✅ 콜백 API 호출 성공")
                            else:
                                print(f"[BACKGROUND] ❌ 콜백 API 호출 실패 - 상태코드: {response.status_code}")
                                
                    except httpx.TimeoutException:
                        print(f"[BACKGROUND] ❌ 콜백 API 호출 타임아웃 (60초)")
                    except httpx.RequestError as e:
                        print(f"[BACKGROUND] ❌ 콜백 API 호출 네트워크 오류: {e}")
                    except Exception as e:
                        print(f"[BACKGROUND] ❌ 콜백 API 호출 예외: {str(e)}")
                        print(f"[BACKGROUND] 예외 타입: {type(e).__name__}")
                        
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
                        
                        print(f"[BACKGROUND] ===== 에러 콜백 API 호출 시작 =====")
                        print(f"[BACKGROUND] 에러 콜백 URL: {callback_url}")
                        print(f"[BACKGROUND] 에러 콜백 데이터: {json.dumps(error_callback_response, ensure_ascii=False, indent=2)}")
                        
                        async with httpx.AsyncClient(timeout=60.0) as client:
                            print(f"[BACKGROUND] 에러 콜백 HTTP 클라이언트 생성")
                            print(f"[BACKGROUND] 에러 콜백 POST 요청 전송 중...")
                            
                            response = await client.post(
                                callback_url,
                                json=error_callback_response,
                                headers={"Content-Type": "application/json"}
                            )
                            
                            print(f"[BACKGROUND] ===== 에러 콜백 API 호출 완료 =====")
                            print(f"[BACKGROUND] 에러 콜백 상태코드: {response.status_code}")
                            print(f"[BACKGROUND] 에러 콜백 응답: {response.text}")
                            
                            if response.status_code == 200:
                                print(f"[BACKGROUND] ✅ 에러 콜백 API 호출 성공")
                            else:
                                print(f"[BACKGROUND] ❌ 에러 콜백 API 호출 실패 - 상태코드: {response.status_code}")
                                
                    except httpx.TimeoutException:
                        print(f"[BACKGROUND] ❌ 에러 콜백 API 호출 타임아웃 (60초)")
                    except httpx.RequestError as e:
                        print(f"[BACKGROUND] ❌ 에러 콜백 API 호출 네트워크 오류: {e}")
                    except Exception as callback_error:
                        print(f"[BACKGROUND] ❌ 에러 콜백 API 호출 예외: {str(callback_error)}")
                        print(f"[BACKGROUND] 에러 콜백 예외 타입: {type(callback_error).__name__}")
                
                print(f"[BACKGROUND] ===== 백그라운드 함수 종료 =====")
                print(f"[BACKGROUND] 함수 종료 시간: {asyncio.get_event_loop().time()}")
            
            # 백그라운드에서 챗봇 작업 시작
            print(f"[BACKGROUND] ===== 백그라운드 태스크 생성 시작 =====")
            background_task = asyncio.create_task(process_chatbot_background())
            print(f"[BACKGROUND] 백그라운드 태스크 생성 완료")
            print(f"[BACKGROUND] 태스크 객체: {background_task}")
            print(f"[BACKGROUND] 태스크 상태: {background_task.done()}")
            
            # 백그라운드 태스크가 실제로 실행되도록 보장
            print(f"[BACKGROUND] 백그라운드 태스크 실행 보장 시작")
            try:
                # 태스크가 시작되도록 약간의 지연
                await asyncio.sleep(0.1)
                print(f"[BACKGROUND] 백그라운드 태스크 실행 보장 완료")
                print(f"[BACKGROUND] 태스크 상태: {background_task.done()}")
            except Exception as e:
                print(f"[BACKGROUND ERROR] 태스크 실행 보장 중 오류: {str(e)}")
            
            # 4초 대기 (빠른 응답인지 확인)
            try:
                # 4초 동안 챗봇 작업이 완료되는지 기다림
                loop = asyncio.get_event_loop()
                result = await asyncio.wait_for(
                    loop.run_in_executor(None, self.text_to_sql.process_question, question),
                    timeout=3.0
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
                print(f"[TIMEOUT] 백그라운드 태스크 상태: {background_task.done()}")
                print(f"[TIMEOUT] 백그라운드 태스크 객체: {background_task}")
                
                # 백그라운드 태스크가 실제로 실행되고 있는지 확인
                if not background_task.done():
                    print(f"[TIMEOUT] 백그라운드 태스크가 실행 중입니다")
                    print(f"[TIMEOUT] 태스크가 완료될 때까지 기다리지 않고 즉시 응답 반환")
                else:
                    print(f"[TIMEOUT WARNING] 백그라운드 태스크가 이미 완료되었습니다!")
                    print(f"[TIMEOUT WARNING] 이는 예상되지 않은 상황입니다")
                
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
                    
                    print(f"[CALLBACK] ===== 메인 에러 콜백 API 호출 시작 =====")
                    print(f"[CALLBACK] 에러 콜백 URL: {callback_url}")
                    print(f"[CALLBACK] 에러 콜백 데이터: {json.dumps(error_callback_response, ensure_ascii=False, indent=2)}")
                    
                    async with httpx.AsyncClient(timeout=60.0) as client:
                        print(f"[CALLBACK] 메인 에러 콜백 HTTP 클라이언트 생성")
                        print(f"[CALLBACK] 메인 에러 콜백 POST 요청 전송 중...")
                        
                        response = await client.post(
                            callback_url,
                            json=error_callback_response,
                            headers={"Content-Type": "application/json"}
                        )
                        
                        print(f"[CALLBACK] ===== 메인 에러 콜백 API 호출 완료 =====")
                        print(f"[CALLBACK] 메인 에러 콜백 상태코드: {response.status_code}")
                        print(f"[CALLBACK] 메인 에러 콜백 응답: {response.text}")
                        
                        if response.status_code == 200:
                            print(f"[CALLBACK] ✅ 메인 에러 콜백 API 호출 성공")
                        else:
                            print(f"[CALLBACK] ❌ 메인 에러 콜백 API 호출 실패 - 상태코드: {response.status_code}")
                            
            except httpx.TimeoutException:
                print(f"[CALLBACK] ❌ 메인 에러 콜백 API 호출 타임아웃 (60초)")
            except httpx.RequestError as e:
                print(f"[CALLBACK] ❌ 메인 에러 콜백 API 호출 네트워크 오류: {e}")
            except Exception as callback_error:
                print(f"[CALLBACK] ❌ 메인 에러 콜백 API 호출 예외: {str(callback_error)}")
                print(f"[CALLBACK] 메인 에러 콜백 예외 타입: {type(callback_error).__name__}")
            
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
