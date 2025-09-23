#!/usr/bin/env python3
"""
간단한 KBO 챗봇 테스트 스크립트
질문과 답변을 한글 파일로 정리합니다.
"""

import requests
import time
from datetime import datetime

def test_chatbot():
    """챗봇에 질문을 보내고 답변을 받습니다."""
    
    # 테스트할 질문들
    questions = [
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
    
    results = []
    
    print("🚀 KBO 챗봇 테스트 시작...")
    print(f"📋 총 {len(questions)}개 질문을 테스트합니다.\n")
    
    for i, question in enumerate(questions, 1):
        print(f"🔍 질문 {i}: {question}")
        
        try:
            # 챗봇에 요청 보내기
            response = requests.post(
                "http://localhost:8000/rag-test",
                json={"message": question},
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                
                # 답변 추출
                answer = "답변을 받지 못했습니다."
                try:
                    if "answer" in data:
                        answer = data["answer"]
                except:
                    pass
                
                results.append({
                    "질문": question,
                    "답변": answer
                })
                
                print(f"✅ 답변 {i}: {answer[:50]}...")
                
            else:
                results.append({
                    "질문": question,
                    "답변": f"오류 발생: HTTP {response.status_code}"
                })
                print(f"❌ 오류 {i}: HTTP {response.status_code}")
                
        except Exception as e:
            results.append({
                "질문": question,
                "답변": f"예외 발생: {str(e)}"
            })
            print(f"❌ 예외 {i}: {str(e)}")
        
        # 요청 간 간격
        if i < len(questions):
            time.sleep(2)
    
    return results

def save_to_korean_file(results):
    """결과를 한글 파일로 저장합니다."""
    
    filename = f"KBO_챗봇_질문답변_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    
    with open(filename, 'w', encoding='utf-8') as f:
        f.write("=" * 60 + "\n")
        f.write("KBO 챗봇 질문과 답변 모음\n")
        f.write("=" * 60 + "\n")
        f.write(f"생성일: {datetime.now().strftime('%Y년 %m월 %d일 %H시 %M분')}\n")
        f.write(f"총 질문 수: {len(results)}개\n")
        f.write("=" * 60 + "\n\n")
        
        for i, result in enumerate(results, 1):
            f.write(f"【질문 {i}】\n")
            f.write(f"Q: {result['질문']}\n\n")
            f.write(f"【답변 {i}】\n")
            f.write(f"A: {result['답변']}\n")
            f.write("-" * 60 + "\n\n")
    
    print(f"\n✅ 결과가 '{filename}' 파일로 저장되었습니다.")
    return filename

def main():
    """메인 실행 함수"""
    try:
        # 챗봇 테스트
        results = test_chatbot()
        
        # 한글 파일로 저장
        filename = save_to_korean_file(results)
        
        print(f"\n🎉 테스트 완료!")
        print(f"📁 저장된 파일: {filename}")
        
        # 간단한 통계
        success_count = len([r for r in results if "오류" not in r["답변"] and "예외" not in r["답변"]])
        print(f"📊 성공한 질문: {success_count}/{len(results)}개")
        
    except Exception as e:
        print(f"❌ 프로그램 실행 중 오류: {e}")

if __name__ == "__main__":
    main()
