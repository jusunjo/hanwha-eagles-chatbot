from flask import Flask, request, jsonify
import os
from dotenv import load_dotenv
from chatbot_service import HanwhaEaglesChatbot

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