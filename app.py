from flask import Flask, request, jsonify
import os
import asyncio
from dotenv import load_dotenv
from chatbot_service import HanwhaEaglesChatbot
from kakao_service import kakao_service
import json

# 환경 변수 로드
load_dotenv()

app = Flask(__name__)
# CORS(app)  # 임시로 주석 처리

# 챗봇 인스턴스 생성
chatbot = HanwhaEaglesChatbot()

@app.route('/health', methods=['GET'])
def health_check():
    """서버 상태 확인"""
    return jsonify({"status": "healthy", "message": "한화이글스 챗봇 서버가 정상 작동 중입니다."})

@app.route('/chat', methods=['POST'])
def handle_chat():
    """AI 챗봇 메시지 처리"""
    try:
        data = request.get_json()
        
        # 사용자 메시지 추출
        user_message = data.get('message', '')
        
        if not user_message:
            return jsonify({
                "error": "메시지를 입력해주세요."
            })
        
        # 챗봇 응답 생성
        response_text = chatbot.get_response(user_message)
        
        # AI 응답만 반환
        return jsonify({
            "user_message": user_message,
            "bot_response": response_text
        })
        
    except Exception as e:
        print(f"Error processing message: {str(e)}")
        return jsonify({
            "error": "죄송합니다. 처리 중 오류가 발생했습니다. 다시 시도해주세요."
        })

@app.route('/kakao', methods=['POST'])
def handle_kakao():
    """카카오톡 챗봇 메시지 처리"""
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({
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
        
        # 비동기 함수를 동기적으로 실행
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            response = loop.run_until_complete(
                kakao_service.process_kakao_request(data)
            )
            
            # 응답 형식 검증
            print(f"[APP] 카카오 응답: {json.dumps(response, ensure_ascii=False, indent=2)}")
            
            # 필수 필드 확인
            if 'version' not in response or 'template' not in response:
                print(f"[ERROR] 응답에 필수 필드가 누락됨: {response}")
                # 기본 형식으로 재생성
                response = {
                    "version": "2.0",
                    "template": {
                        "outputs": [
                            {
                                "simpleText": {
                                    "text": response.get('template', {}).get('outputs', [{}])[0].get('simpleText', {}).get('text', '응답 형식 오류')
                                }
                            }
                        ]
                    }
                }
            
            return jsonify(response)
        finally:
            loop.close()
            
    except Exception as e:
        print(f"Error processing Kakao message: {str(e)}")
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
    app.run(host='0.0.0.0', port=port, debug=False) 