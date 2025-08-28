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
        
        # 요청 헤더 로깅
        print(f"[APP-CHAT] 요청 헤더:")
        for header, value in request.headers.items():
            print(f"   {header}: {value}")
        
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
        print(f"   - 콜백 URL 타입: {type(callback_url)}")
        
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
                print(f"[APP-CHAT] 비동기 처리 완료 - 응답: {json.dumps(response, ensure_ascii=False, indent=2)}")
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
                print(f"[APP-CHAT] 동기 처리 완료 - 응답: {json.dumps(response, ensure_ascii=False, indent=2)}")
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
def handle_kakao():
    """카카오톡 챗봇 메시지 처리"""
    try:
        print(f"[APP-KAKAO] ===== 카카오톡 엔드포인트 호출 시작 =====")
        data = request.get_json()
        
        if not data:
            print(f"[APP-KAKAO] 요청 데이터 없음")
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
        
        print(f"[APP-KAKAO] 요청 데이터 수신 완료")
        
        # 카카오톡 형식인지 확인
        if 'userRequest' in data:
            print(f"[APP-KAKAO] 카카오톡 형식 감지 - userRequest 존재")
            # 카카오톡 형식 - 기존 로직 사용
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            try:
                print(f"[APP-KAKAO] 비동기 처리 시작")
                response = loop.run_until_complete(
                    kakao_service.process_kakao_request(data)
                )
                
                print(f"[APP-KAKAO] 카카오 서비스 응답 수신 완료")
                
                # 응답 스키마 엄격 검증
                print(f"[APP-KAKAO] 응답 스키마 검증 시작")
                
                # 1단계: 기본 구조 검증
                if not isinstance(response, dict):
                    print(f"[APP-KAKAO-ERROR] 응답이 딕셔너리가 아님: {type(response)}")
                    response = _create_fallback_response("응답 형식 오류")
                elif 'version' not in response:
                    print(f"[APP-KAKAO-ERROR] version 필드 누락")
                    response = _create_fallback_response("응답 형식 오류")
                elif 'template' not in response:
                    print(f"[APP-KAKAO-ERROR] template 필드 누락")
                    response = _create_fallback_response("응답 형식 오류")
                elif not isinstance(response['template'], dict):
                    print(f"[APP-KAKAO-ERROR] template이 딕셔너리가 아님: {type(response['template'])}")
                    response = _create_fallback_response("응답 형식 오류")
                elif 'outputs' not in response['template']:
                    print(f"[APP-KAKAO-ERROR] outputs 필드 누락")
                    response = _create_fallback_response("응답 형식 오류")
                elif not isinstance(response['template']['outputs'], list):
                    print(f"[APP-KAKAO-ERROR] outputs가 리스트가 아님: {type(response['template']['outputs'])}")
                    response = _create_fallback_response("응답 형식 오류")
                elif len(response['template']['outputs']) == 0:
                    print(f"[APP-KAKAO-ERROR] outputs가 비어있음")
                    response = _create_fallback_response("응답 형식 오류")
                else:
                    # 2단계: outputs 내용 검증
                    first_output = response['template']['outputs'][0]
                    if not isinstance(first_output, dict):
                        print(f"[APP-KAKAO-ERROR] 첫 번째 output이 딕셔너리가 아님: {type(first_output)}")
                        response = _create_fallback_response("응답 형식 오류")
                    elif 'simpleText' not in first_output:
                        print(f"[APP-KAKAO-ERROR] simpleText 필드 누락")
                        response = _create_fallback_response("응답 형식 오류")
                    elif not isinstance(first_output['simpleText'], dict):
                        print(f"[APP-KAKAO-ERROR] simpleText가 딕셔너리가 아님: {type(first_output['simpleText'])}")
                        response = _create_fallback_response("응답 형식 오류")
                    elif 'text' not in first_output['simpleText']:
                        print(f"[APP-KAKAO-ERROR] text 필드 누락")
                        response = _create_fallback_response("응답 형식 오류")
                    elif not isinstance(first_output['simpleText']['text'], str):
                        print(f"[APP-KAKAO-ERROR] text가 문자열이 아님: {type(first_output['simpleText']['text'])}")
                        response = _create_fallback_response("응답 형식 오류")
                    else:
                        print(f"[APP-KAKAO] 응답 스키마 검증 성공")
                        print(f"[APP-KAKAO] - version: {response.get('version')}")
                        print(f"[APP-KAKAO] - text 길이: {len(first_output['simpleText']['text'])}")
                
                print(f"[APP-KAKAO] 최종 응답 반환 준비 완료")
                return jsonify(response)
            finally:
                loop.close()
                print(f"[APP-KAKAO] 비동기 루프 정리 완료")
                
        elif 'message' in data:
            print(f"[APP-KAKAO] 간단한 메시지 형식 감지 - message 존재")
            # 간단한 메시지 형식 - 즉시 처리
            user_message = data.get('message', '')
            
            if not user_message:
                print(f"[APP-KAKAO] 메시지 내용 없음")
                return jsonify({
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
            
            print(f"[APP-KAKAO] 간단한 메시지 처리 시작: {user_message}")
            
            # 챗봇 응답 생성
            response_text = chatbot.get_response(user_message)
            print(f"[APP-KAKAO] 챗봇 응답 생성 완료: {response_text[:100]}...")
            
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
            
            print(f"[APP-KAKAO] 간단한 메시지 응답 생성 완료")
            return jsonify(response)
            
        else:
            print(f"[APP-KAKAO-ERROR] 지원하지 않는 요청 형식")
            print(f"[APP-KAKAO-ERROR] 요청 키: {list(data.keys())}")
            # 지원하지 않는 형식
            return jsonify({
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
        print(f"[APP-KAKAO-ERROR] 카카오 메시지 처리 중 예외 발생: {str(e)}")
        print(f"[APP-KAKAO-ERROR] 예외 타입: {type(e).__name__}")
        print(f"[APP-KAKAO-ERROR] 예외 상세: {e}")
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