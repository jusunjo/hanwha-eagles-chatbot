from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import os
import asyncio
from dotenv import load_dotenv
from chatbot_service import HanwhaEaglesChatbot
from kakao_service import kakao_service
import json
from typing import Dict, Any

# 환경 변수 로드
load_dotenv()

# FastAPI 앱 생성
app = FastAPI(
    title="한화이글스 챗봇 API",
    description="한화이글스 선수 정보 및 성적을 제공하는 AI 챗봇 API",
    version="1.0.0"
)

# CORS 미들웨어 추가
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 챗봇 인스턴스 생성
chatbot = HanwhaEaglesChatbot()

def _create_fallback_response(text: str) -> Dict[str, Any]:
    """카카오톡 챗봇 fallback 응답 생성"""
    return {
        "version": "2.0",
        "template": {
            "outputs": [
                {
                    "simpleText": {
                        "text": text
                    }
                }
            ]
        }
    }

@app.get("/health")
async def health_check():
    """서버 상태 확인"""
    return {"status": "healthy", "message": "한화이글스 챗봇 서버가 정상 작동 중입니다."}

@app.post("/chat")
async def handle_chat(request: Request):
    """AI 챗봇 메시지 처리"""
    try:
        print(f"\n[APP-CHAT] ===== /chat 엔드포인트 호출 시작 =====")
        
        # 요청 데이터 로깅
        data = await request.json()
        print(f"[APP-CHAT] 받은 요청 데이터:")
        print(f"   - 데이터 타입: {type(data)}")
        print(f"   - 데이터 내용: {json.dumps(data, ensure_ascii=False, indent=2) if data else 'None'}")
        
        if not data:
            print(f"[APP-CHAT] 요청 데이터가 비어있음")
            return JSONResponse({
                "error": "요청 데이터가 없습니다."
            })
        
        # 사용자 메시지 및 콜백 URL 추출
        user_message = data.get('message', '')
        callback_url = data.get('callback_url', None)
        
        print(f"[APP-CHAT] 추출된 정보:")
        print(f"   - 사용자 메시지: {user_message}")
        print(f"   - 콜백 URL: {callback_url}")
        
        # 콜백 URL 체크 및 로깅
        if callback_url:
            print(f"[APP-CHAT] ✅ 콜백 URL이 제공됨: {callback_url}")
        else:
            print(f"[APP-CHAT] ⚠️ 콜백 URL이 제공되지 않음 - 동기 처리로 진행")
        
        if not user_message:
            print(f"[APP-CHAT] 사용자 메시지가 비어있음")
            return JSONResponse({
                "error": "메시지를 입력해주세요."
            })
        
        # 비동기 챗봇 응답 생성
        print(f"[APP-CHAT] 챗봇 응답 생성 시작...")
        
        if callback_url:
            print(f"[APP-CHAT] 콜백 URL이 감지됨 - 비동기 처리 시작")
            # 콜백 URL이 있는 경우 비동기 처리
            response = await chatbot.get_response_async(user_message, callback_url)
            print(f"[APP-CHAT] 비동기 처리 완료")
            return JSONResponse(response)
        else:
            print(f"[APP-CHAT] 콜백 URL이 없음 - 동기 처리 시작")
            # 콜백 URL이 없는 경우 동기 처리
            response_text = await chatbot._process_message_async(user_message)
            response = {
                "version": "2.0",
                "useCallback": True,
                "user_message": user_message,
                "response": response_text,
                "timestamp": asyncio.get_event_loop().time()
            }
            print(f"[APP-CHAT] 동기 처리 완료")
            return JSONResponse(response)
            
    except Exception as e:
        print(f"[APP-CHAT-ERROR] 예외 발생: {str(e)}")
        print(f"[APP-CHAT-ERROR] 예외 타입: {type(e).__name__}")
        import traceback
        traceback.print_exc()
        
        return JSONResponse({
            "error": "요청 처리 중 오류가 발생했습니다.",
            "details": str(e)
        })

@app.post("/kakao")
async def kakao_handler(request: Request):
    """카카오톡 챗봇을 위한 엔드포인트 (콜백 방식)."""
    try:
        req = await request.json()
        response = await kakao_service.process_kakao_request(req)
        return JSONResponse(response)
    except Exception as e:
        print(f"[ERROR] Kakao handler error: {str(e)}")
        return JSONResponse({
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
        })

@app.post("/kakao-simple")
async def kakao_simple_handler(request: Request):
    """간단한 카카오톡 챗봇 메시지 처리"""
    try:
        print(f"[APP-KAKAO-SIMPLE] ===== 간단한 카카오톡 엔드포인트 호출 시작 =====")
        data = await request.json()
        
        if not data:
            print(f"[APP-KAKAO-SIMPLE] 요청 데이터 없음")
            return JSONResponse({
                "version": "2.0",
                "template": {
                    "outputs": [
                        {
                            "simpleText": {
                                "text": "요청 데이터가 없습니다."
                            }
                        }
                    ]
                }
            })
        
        print(f"[APP-KAKAO-SIMPLE] 요청 데이터 수신 완료")
        
        # 간단한 메시지 형식 처리
        if 'message' in data:
            print(f"[APP-KAKAO-SIMPLE] 간단한 메시지 형식 감지 - message 존재")
            user_message = data.get('message', '')
            
            if not user_message:
                print(f"[APP-KAKAO-SIMPLE] 메시지 내용 없음")
                return JSONResponse({
                    "version": "2.0",
                    "template": {
                        "outputs": [
                            {
                                "simpleText": {
                                    "text": "메시지가 필요합니다."
                                }
                            }
                        ]
                    }
                })
            
            print(f"[APP-KAKAO-SIMPLE] 간단한 메시지 처리 시작: {user_message}")
            
            # 챗봇 응답 생성
            response_text = await chatbot._process_message_async(user_message)
            print(f"[APP-KAKAO-SIMPLE] 챗봇 응답 생성 완료: {response_text[:100]}...")
            
            # 카카오톡 형식으로 응답
            response = {
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
            
            print(f"[APP-KAKAO-SIMPLE] 간단한 메시지 응답 생성 완료")
            return JSONResponse(response)
            
        else:
            print(f"[APP-KAKAO-SIMPLE-ERROR] 지원하지 않는 요청 형식")
            print(f"[APP-KAKAO-SIMPLE-ERROR] 요청 키: {list(data.keys())}")
            return JSONResponse({
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
            })
            
    except Exception as e:
        print(f"[APP-KAKAO-SIMPLE-ERROR] 간단한 카카오 메시지 처리 중 예외 발생: {str(e)}")
        print(f"[APP-KAKAO-SIMPLE-ERROR] 예외 타입: {type(e).__name__}")
        print(f"[APP-KAKAO-SIMPLE-ERROR] 예외 상세: {e}")
        return JSONResponse({
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
        })

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=5000) 