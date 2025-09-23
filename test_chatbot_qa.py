#!/usr/bin/env python3
"""
KBO 챗봇 테스트 스크립트
실제 챗봇에 질문을 보내고 답변을 받아서 JSON으로 저장합니다.
"""

import json
import asyncio
import httpx
import time
from datetime import datetime
from typing import Dict, List, Any

class ChatbotTester:
    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url
        self.results = []
        
    async def send_question(self, question: str, question_id: int) -> Dict[str, Any]:
        """챗봇에 질문을 보내고 답변을 받습니다."""
        try:
            # 카카오톡 형식으로 요청 데이터 구성
            request_data = {
                "userRequest": {
                    "user": {
                        "id": "test_user"
                    },
                    "utterance": question,
                    "callbackUrl": "http://localhost:8000/kakao"
                },
                "action": {
                    "params": {
                        "message": question
                    }
                }
            }
            
            print(f"🔍 질문 {question_id}: {question}")
            
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(
                    f"{self.base_url}/kakao",
                    json=request_data
                )
                
                if response.status_code == 200:
                    result = response.json()
                    
                    # 카카오톡 응답 형식에서 텍스트 추출
                    answer = "답변을 받지 못했습니다."
                    try:
                        if "template" in result and "outputs" in result["template"]:
                            outputs = result["template"]["outputs"]
                            if outputs and len(outputs) > 0:
                                first_output = outputs[0]
                                if "simpleText" in first_output and "text" in first_output["simpleText"]:
                                    answer = first_output["simpleText"]["text"]
                    except Exception as e:
                        print(f"⚠️ 응답 파싱 오류 {question_id}: {e}")
                        answer = f"응답 파싱 오류: {str(e)}"
                    
                    print(f"✅ 답변 {question_id}: {answer[:100]}...")
                    
                    return {
                        "question_id": question_id,
                        "question": question,
                        "answer": answer,
                        "status": "success",
                        "timestamp": datetime.now().isoformat(),
                        "raw_response": result
                    }
                else:
                    print(f"❌ 오류 {question_id}: HTTP {response.status_code}")
                    return {
                        "question_id": question_id,
                        "question": question,
                        "answer": f"오류 발생: HTTP {response.status_code}",
                        "status": "error",
                        "timestamp": datetime.now().isoformat()
                    }
                    
        except Exception as e:
            print(f"❌ 예외 {question_id}: {str(e)}")
            return {
                "question_id": question_id,
                "question": question,
                "answer": f"예외 발생: {str(e)}",
                "status": "error",
                "timestamp": datetime.now().isoformat()
            }
    
    async def test_all_questions(self, questions: List[str]) -> List[Dict[str, Any]]:
        """모든 질문을 순차적으로 테스트합니다."""
        results = []
        
        for i, question in enumerate(questions, 1):
            result = await self.send_question(question, i)
            results.append(result)
            
            # 요청 간 간격 (서버 부하 방지)
            if i < len(questions):
                print(f"⏳ 2초 대기 중... ({i}/{len(questions)})")
                await asyncio.sleep(2)
        
        return results
    
    def save_results(self, results: List[Dict[str, Any]], filename: str = "chatbot_test_results.json"):
        """결과를 JSON 파일로 저장합니다."""
        dataset = {
            "metadata": {
                "title": "KBO 챗봇 테스트 결과",
                "description": "실제 챗봇에 질문을 보내고 받은 답변들",
                "test_date": datetime.now().isoformat(),
                "total_questions": len(results),
                "successful_questions": len([r for r in results if r["status"] == "success"]),
                "failed_questions": len([r for r in results if r["status"] == "error"])
            },
            "test_results": results
        }
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(dataset, f, ensure_ascii=False, indent=2)
            print(f"✅ 결과가 '{filename}' 파일로 저장되었습니다.")
            return True
        except Exception as e:
            print(f"❌ 파일 저장 중 오류 발생: {e}")
            return False

def get_test_questions() -> List[str]:
    """테스트할 질문 리스트를 반환합니다."""
    return [
        "오늘 경기 일정이 뭐야?",
        "어제 경기 결과 알려줘",
        "내일 경기 언제야?",
        "한화 이글스 경기 언제야?",
        "지금 경기하고 있어?",
        "노시환 선수 기록 어때?",
        "이번 시즌 홈런왕이 누구야?",
        "한화 이글스 타자들 성적은?",
        "투수 방어율 순위 알려줘",
        "최근 활약하는 신인 선수 있어?",
        "한화 이글스 순위가 몇 위야?",
        "KBO 전체 순위표 보여줘",
        "한화 이글스 감독이 누구야?",
        "한화 이글스 홈구장이 어디야?",
        "이번 시즌 최다승 투수는?",
        "타율 1위가 누구야?",
        "한화 이글스 최근 10경기 성적은?",
        "KBO 규칙이 뭐야?",
        "야구 용어 설명해줘",
        "KBO 챗봇이 뭘 할 수 있어?"
    ]

async def main():
    """메인 실행 함수"""
    print("🚀 KBO 챗봇 테스트 시작...")
    
    # 테스트 질문 로드
    questions = get_test_questions()
    print(f"📋 총 {len(questions)}개 질문을 테스트합니다.")
    
    # 챗봇 테스터 초기화
    tester = ChatbotTester()
    
    # 서버 연결 확인
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.get(f"{tester.base_url}/")
            if response.status_code == 200:
                print("✅ 챗봇 서버 연결 확인됨")
            else:
                print(f"⚠️ 서버 응답 이상: {response.status_code}")
    except Exception as e:
        print(f"❌ 서버 연결 실패: {e}")
        print("💡 서버가 실행 중인지 확인해주세요: python app.py")
        return
    
    # 모든 질문 테스트
    print("\n" + "="*50)
    print("📝 질문-답변 테스트 시작")
    print("="*50)
    
    results = await tester.test_all_questions(questions)
    
    # 결과 저장
    print("\n" + "="*50)
    print("💾 결과 저장 중...")
    print("="*50)
    
    if tester.save_results(results):
        # 결과 요약 출력
        successful = len([r for r in results if r["status"] == "success"])
        failed = len([r for r in results if r["status"] == "error"])
        
        print(f"\n📊 테스트 결과 요약:")
        print(f"  ✅ 성공: {successful}개")
        print(f"  ❌ 실패: {failed}개")
        print(f"  📈 성공률: {successful/len(results)*100:.1f}%")
        
        # 실패한 질문들 출력
        if failed > 0:
            print(f"\n❌ 실패한 질문들:")
            for result in results:
                if result["status"] == "error":
                    print(f"  • {result['question_id']}: {result['question']}")
        
        print(f"\n🎉 테스트 완료! 결과가 저장되었습니다.")
    else:
        print("❌ 결과 저장에 실패했습니다.")

if __name__ == "__main__":
    asyncio.run(main())
