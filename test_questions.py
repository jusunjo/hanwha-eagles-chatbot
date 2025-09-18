#!/usr/bin/env python3
"""
모든 질문 유형에 대한 답변 테스트 및 JSON 저장
"""

import json
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from rag.text_to_sql import TextToSQL
from datetime import datetime

def test_all_questions():
    """모든 질문 유형에 대한 답변 생성 및 JSON 저장"""
    
    # 질문 리스트 (제공해주신 모든 질문들)
    questions = [
        # 경기 일정 관련 (10개)
        "오늘 경기 일정이 뭐야?",
        "내일 한화 경기 일정 알려줘",
        "한화 앞으로 남은 경기 일정",
        "이번 주 경기 일정",
        "9월 경기 일정",
        "한화 홈 경기 일정",
        "주말 경기 일정",
        "다음주 토요일 경기",
        "잠실 경기 일정",
        "한화 vs 두산 경기 언제야?",
        
        # 경기 결과 관련 (8개)
        "어제 경기 결과",
        "한화 vs 두산 경기 결과",
        "최근 한화 경기 결과",
        "3월 8일 경기 결과",
        "한화 이번 시즌 전적",
        "한화 승률이 어때?",
        "한화 몇승 몇패야?",
        "한화 순위가 몇 위야?",
        
        # 선수 성적 관련 (7개)
        "문동주 선수 성적이 어때?",
        "한화 타자 중에 가장 잘하는 선수가 누구야?",
        "KBO 타율 1위는 누구야?",
        "한화 투수 중에 가장 잘하는 투수가 누구야?",
        "이정후 선수 요즘 어때?",
        "한화 홈런 1위는 누구야?",
        "한화 ERA 1위 투수는 누구야?",
        
        # 팀 통계 관련 (5개)
        "한화 팀 타율이 어때?",
        "한화 팀 홈런 개수",
        "한화 팀 ERA",
        "한화 팀 순위",
        "한화 팀 승률",
        
        # 더 세부적인 질문들
        "한화 다음 경기 상대는 누구야?",
        "한화 원정 경기 일정",
        "한화 홈 경기만 보여줘",
        "이번 달 한화 경기 몇 개야?",
        "한화 vs 특정팀 경기 결과",
        "한화 선발투수는 누구야?",
        "한화 마무리투수는 누구야?",
        "한화 4번타자는 누구야?",
        "한화 감독은 누구야?",
        "한화 구단주는 누구야?",
        
        # 시즌 통계 관련
        "한화 이번 시즌 홈런 개수",
        "한화 이번 시즌 타점",
        "한화 이번 시즌 도루",
        "한화 이번 시즌 완봉",
        "한화 이번 시즌 세이브",
        
        # 경기장/구장 관련
        "고척 경기 일정",
        "잠실 경기 일정",
        "한화 홈구장은 어디야?",
        "각 팀 홈구장 알려줘"
    ]
    
    # TextToSQL 인스턴스 생성
    try:
        text_to_sql = TextToSQL()
        print("✅ TextToSQL 초기화 완료")
    except Exception as e:
        print(f"❌ TextToSQL 초기화 실패: {e}")
        return
    
    # 결과 저장용 리스트
    results = []
    
    print(f"\n🚀 총 {len(questions)}개 질문에 대한 답변 생성 시작...")
    
    for i, question in enumerate(questions, 1):
        print(f"\n{'='*60}")
        print(f"[{i}/{len(questions)}] 질문: {question}")
        
        try:
            # 질문 처리
            answer = text_to_sql.process_question(question)
            
            # 결과 저장
            result = {
                "question_id": i,
                "question": question,
                "answer": answer,
                "timestamp": datetime.now().isoformat(),
                "status": "success"
            }
            
            print(f"✅ 답변 완료")
            print(f"답변: {answer[:100]}{'...' if len(answer) > 100 else ''}")
            
        except Exception as e:
            print(f"❌ 오류 발생: {e}")
            result = {
                "question_id": i,
                "question": question,
                "answer": f"오류 발생: {str(e)}",
                "timestamp": datetime.now().isoformat(),
                "status": "error"
            }
        
        results.append(result)
    
    # JSON 파일로 저장
    output_file = "question_answers.json"
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        
        print(f"\n🎉 모든 결과가 {output_file}에 저장되었습니다!")
        
        # 통계 출력
        success_count = sum(1 for r in results if r["status"] == "success")
        error_count = sum(1 for r in results if r["status"] == "error")
        
        print(f"\n📊 처리 결과 통계:")
        print(f"  - 총 질문 수: {len(questions)}")
        print(f"  - 성공: {success_count}")
        print(f"  - 실패: {error_count}")
        print(f"  - 성공률: {success_count/len(questions)*100:.1f}%")
        
    except Exception as e:
        print(f"❌ JSON 저장 실패: {e}")

if __name__ == "__main__":
    test_all_questions()
