from flask import Flask, request, jsonify
import os
import asyncio
from dotenv import load_dotenv
from chatbot_service import HanwhaEaglesChatbot
from kakao_service import kakao_service
import json
from typing import Dict, Any

# 환경 변수 로드
load_dotenv()

app = Flask(__name__)
# CORS(app)  # 임시로 주석 처리

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

@app.route('/health', methods=['GET'])
def health_check():
    """서버 상태 확인"""
    return jsonify({"status": "healthy", "message": "한화이글스 챗봇 서버가 정상 작동 중입니다."})

@app.route('/chat', methods=['POST'])
def handle_chat():
    """AI 챗봇 메시지 처리"""
    try:
        print(f"\n[APP-CHAT] ===== /chat 엔드포인트 호출 시작 =====")
        
        # 요청 데이터 로깅
        data = request.get_json()
        print(f"[APP-CHAT] 받은 요청 데이터:")
        print(f"   - 데이터 타입: {type(data)}")
        print(f"   - 데이터 내용: {json.dumps(data, ensure_ascii=False, indent=2) if data else 'None'}")
        
        if not data:
            print(f"[APP-CHAT] 요청 데이터가 비어있음")
            return jsonify({
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
            return jsonify({
                "error": "메시지를 입력해주세요."
            })
        
        # 비동기 챗봇 응답 생성
        print(f"[APP-CHAT] 챗봇 응답 생성 시작...")
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            if callback_url:
                print(f"[APP-CHAT] 콜백 URL이 감지됨 - 비동기 처리 시작")
                # 콜백 URL이 있는 경우 비동기 처리
                response = loop.run_until_complete(
                    chatbot.get_response_async(user_message, callback_url)
                )
                print(f"[APP-CHAT] 비동기 처리 완료")
                return jsonify(response)
            else:
                print(f"[APP-CHAT] 콜백 URL이 없음 - 동기 처리 시작")
                # 콜백 URL이 없는 경우 동기 처리
                response_text = loop.run_until_complete(
                    chatbot._process_message_async(user_message)
                )
                response = {
                    "version": "2.0",
                    "useCallback": True,
                    "user_message": user_message,
                    "bot_response": response_text
                }
                print(f"[APP-CHAT] 동기 처리 완료")
                return jsonify(response)
        finally:
            loop.close()
            print(f"[APP-CHAT] 비동기 루프 정리 완료")
        
    except Exception as e:
        print(f"[APP-CHAT ERROR] 예외 발생: {str(e)}")
        print(f"[APP-CHAT ERROR] 예외 타입: {type(e).__name__}")
        import traceback
        print(f"[APP-CHAT ERROR] 스택 트레이스: {traceback.format_exc()}")
        
        return jsonify({
            "error": "죄송합니다. 처리 중 오류가 발생했습니다. 다시 시도해주세요."
        })

@app.route('/kakao', methods=['POST'])
def kakao_handler():
    """카카오톡 챗봇을 위한 엔드포인트 (콜백 방식)."""
    try:
        req = request.get_json()
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            response = loop.run_until_complete(
                kakao_service.process_kakao_request(req)
            )
            return jsonify(response)
        finally:
            loop.close()
            
    except Exception as e:
        print(f"[ERROR] Kakao handler error: {str(e)}")
        return jsonify({
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
    finally:
        print(f"[APP-KAKAO] ===== 카카오톡 엔드포인트 처리 완료 =====")

@app.route('/kakao-simple', methods=['POST'])
def handle_kakao_simple():
    """카카오톡 챗봇 간단한 메시지 처리 (테스트용)"""
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({
                "error": "요청 데이터가 없습니다."
            })
        
        # 일반적인 message 형식 처리
        user_message = data.get('message', '')
        
        if not user_message:
            return jsonify({
                "error": "메시지가 필요합니다."
            })
        
        # 챗봇 응답 생성
        response_text = chatbot.get_response(user_message)
        
        # 카카오톡 형식으로 응답
        return jsonify({
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
        })
        
    except Exception as e:
        print(f"Error processing Kakao simple message: {str(e)}")
        return jsonify({
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

@app.route('/test', methods=['POST'])
def test_message():
    """테스트용 엔드포인트 (간단한 형식)"""
    try:
        data = request.get_json()
        message = data.get('message', '')
        
        if not message:
            return jsonify({"error": "메시지가 필요합니다."})
        
        response = chatbot.get_response(message)
        
        return jsonify({
            "user_message": message,
            "bot_response": response
        })
        
    except Exception as e:
        return jsonify({"error": f"오류 발생: {str(e)}"})

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True) 