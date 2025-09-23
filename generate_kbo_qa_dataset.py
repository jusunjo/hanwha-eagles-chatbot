#!/usr/bin/env python3
"""
KBO 챗봇 예상 질문-답변 데이터셋 생성 스크립트
KBO 팬들이 챗봇에 할 수 있는 질문들을 수집하고 답변을 매핑합니다.
"""

import json
import os
from datetime import datetime

def generate_kbo_qa_dataset():
    """KBO 챗봇 예상 질문-답변 데이터셋 생성"""
    
    qa_dataset = {
        "metadata": {
            "title": "KBO 챗봇 예상 질문-답변 데이터셋",
            "description": "KBO 팬들이 챗봇에 할 수 있는 예상 질문들과 답변",
            "version": "1.0",
            "created_at": datetime.now().isoformat(),
            "total_questions": 20,
            "categories": [
                "경기 일정 및 결과",
                "선수 정보",
                "팀 정보",
                "순위 및 기록",
                "일반 정보"
            ]
        },
        "questions": [
            {
                "id": 1,
                "category": "경기 일정 및 결과",
                "question": "오늘 경기 일정이 뭐야?",
                "answer": "오늘 KBO 경기 일정을 확인해드리겠습니다. 현재 경기 일정을 조회 중입니다...",
                "keywords": ["오늘", "경기", "일정", "스케줄"],
                "expected_response_type": "game_schedule"
            },
            {
                "id": 2,
                "category": "경기 일정 및 결과",
                "question": "어제 경기 결과 알려줘",
                "answer": "어제 KBO 경기 결과를 확인해드리겠습니다. 경기 분석을 진행 중입니다...",
                "keywords": ["어제", "경기", "결과", "승부"],
                "expected_response_type": "game_result"
            },
            {
                "id": 3,
                "category": "경기 일정 및 결과",
                "question": "내일 경기 언제야?",
                "answer": "내일 KBO 경기 일정을 확인해드리겠습니다. 경기 시간과 장소를 조회 중입니다...",
                "keywords": ["내일", "경기", "시간", "언제"],
                "expected_response_type": "game_schedule"
            },
            {
                "id": 4,
                "category": "경기 일정 및 결과",
                "question": "한화 이글스 경기 언제야?",
                "answer": "한화 이글스 경기 일정을 확인해드리겠습니다. 다음 경기 정보를 조회 중입니다...",
                "keywords": ["한화", "이글스", "경기", "일정"],
                "expected_response_type": "team_schedule"
            },
            {
                "id": 5,
                "category": "경기 일정 및 결과",
                "question": "지금 경기하고 있어?",
                "answer": "현재 진행 중인 KBO 경기가 있는지 확인해드리겠습니다. 실시간 경기 상황을 조회 중입니다...",
                "keywords": ["지금", "현재", "경기", "진행중"],
                "expected_response_type": "live_game"
            },
            {
                "id": 6,
                "category": "선수 정보",
                "question": "노시환 선수 기록 어때?",
                "answer": "노시환 선수의 최근 기록을 확인해드리겠습니다. 타율, 홈런, 타점 등 상세 기록을 조회 중입니다...",
                "keywords": ["노시환", "선수", "기록", "성적"],
                "expected_response_type": "player_stats"
            },
            {
                "id": 7,
                "category": "선수 정보",
                "question": "이번 시즌 홈런왕이 누구야?",
                "answer": "이번 시즌 KBO 홈런 리더를 확인해드리겠습니다. 홈런 순위와 기록을 조회 중입니다...",
                "keywords": ["홈런왕", "홈런", "리더", "순위"],
                "expected_response_type": "league_leader"
            },
            {
                "id": 8,
                "category": "선수 정보",
                "question": "한화 이글스 타자들 성적은?",
                "answer": "한화 이글스 타자들의 성적을 확인해드리겠습니다. 팀 타격 통계를 조회 중입니다...",
                "keywords": ["한화", "이글스", "타자", "성적"],
                "expected_response_type": "team_batting"
            },
            {
                "id": 9,
                "category": "선수 정보",
                "question": "투수 방어율 순위 알려줘",
                "answer": "KBO 투수 방어율 순위를 확인해드리겠습니다. ERA 리더보드를 조회 중입니다...",
                "keywords": ["투수", "방어율", "ERA", "순위"],
                "expected_response_type": "pitching_leader"
            },
            {
                "id": 10,
                "category": "선수 정보",
                "question": "최근 활약하는 신인 선수 있어?",
                "answer": "최근 활약하는 신인 선수들을 확인해드리겠습니다. 신인들의 성적과 활약상을 조회 중입니다...",
                "keywords": ["신인", "신예", "활약", "성적"],
                "expected_response_type": "rookie_performance"
            },
            {
                "id": 11,
                "category": "팀 정보",
                "question": "한화 이글스 순위가 몇 위야?",
                "answer": "한화 이글스의 현재 순위를 확인해드리겠습니다. 팀 순위와 승률을 조회 중입니다...",
                "keywords": ["한화", "이글스", "순위", "위치"],
                "expected_response_type": "team_ranking"
            },
            {
                "id": 12,
                "category": "팀 정보",
                "question": "KBO 전체 순위표 보여줘",
                "answer": "KBO 전체 순위표를 확인해드리겠습니다. 10개 팀의 순위와 승률을 조회 중입니다...",
                "keywords": ["KBO", "순위표", "전체", "리그"],
                "expected_response_type": "league_standings"
            },
            {
                "id": 13,
                "category": "팀 정보",
                "question": "한화 이글스 감독이 누구야?",
                "answer": "한화 이글스 감독 정보를 확인해드리겠습니다. 감독과 코칭스태프 정보를 조회 중입니다...",
                "keywords": ["한화", "이글스", "감독", "코치"],
                "expected_response_type": "team_staff"
            },
            {
                "id": 14,
                "category": "팀 정보",
                "question": "한화 이글스 홈구장이 어디야?",
                "answer": "한화 이글스 홈구장 정보를 확인해드리겠습니다. 구장 위치와 시설 정보를 조회 중입니다...",
                "keywords": ["한화", "이글스", "홈구장", "구장"],
                "expected_response_type": "stadium_info"
            },
            {
                "id": 15,
                "category": "순위 및 기록",
                "question": "이번 시즌 최다승 투수는?",
                "answer": "이번 시즌 최다승 투수를 확인해드리겠습니다. 승수 리더보드를 조회 중입니다...",
                "keywords": ["최다승", "투수", "승수", "리더"],
                "expected_response_type": "pitching_leader"
            },
            {
                "id": 16,
                "category": "순위 및 기록",
                "question": "타율 1위가 누구야?",
                "answer": "현재 타율 1위 선수를 확인해드리겠습니다. 타율 리더보드를 조회 중입니다...",
                "keywords": ["타율", "1위", "리더", "순위"],
                "expected_response_type": "batting_leader"
            },
            {
                "id": 17,
                "category": "순위 및 기록",
                "question": "한화 이글스 최근 10경기 성적은?",
                "answer": "한화 이글스의 최근 10경기 성적을 확인해드리겠습니다. 최근 경기 결과와 통계를 조회 중입니다...",
                "keywords": ["한화", "이글스", "최근", "10경기", "성적"],
                "expected_response_type": "recent_games"
            },
            {
                "id": 18,
                "category": "일반 정보",
                "question": "KBO 규칙이 뭐야?",
                "answer": "KBO 규칙에 대해 설명해드리겠습니다. 야구 규칙과 KBO 특별 규정을 안내해드립니다...",
                "keywords": ["KBO", "규칙", "야구", "규정"],
                "expected_response_type": "rules_info"
            },
            {
                "id": 19,
                "category": "일반 정보",
                "question": "야구 용어 설명해줘",
                "answer": "야구 용어를 설명해드리겠습니다. 자주 사용되는 야구 용어들의 의미를 안내해드립니다...",
                "keywords": ["야구", "용어", "설명", "의미"],
                "expected_response_type": "baseball_terms"
            },
            {
                "id": 20,
                "category": "일반 정보",
                "question": "KBO 챗봇이 뭘 할 수 있어?",
                "answer": "KBO 챗봇이 제공하는 기능을 안내해드리겠습니다. 경기 일정, 선수 정보, 팀 순위 등 다양한 정보를 제공합니다...",
                "keywords": ["KBO", "챗봇", "기능", "도움"],
                "expected_response_type": "bot_capabilities"
            }
        ]
    }
    
    return qa_dataset

def save_dataset_to_file(dataset, filename="kbo_qa_dataset.json"):
    """데이터셋을 JSON 파일로 저장"""
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(dataset, f, ensure_ascii=False, indent=2)
        print(f"✅ 데이터셋이 '{filename}' 파일로 저장되었습니다.")
        return True
    except Exception as e:
        print(f"❌ 파일 저장 중 오류 발생: {e}")
        return False

def print_dataset_summary(dataset):
    """데이터셋 요약 정보 출력"""
    print("\n" + "="*50)
    print("📊 KBO 챗봇 예상 질문-답변 데이터셋 요약")
    print("="*50)
    
    metadata = dataset["metadata"]
    print(f"📝 제목: {metadata['title']}")
    print(f"📅 생성일: {metadata['created_at']}")
    print(f"📊 총 질문 수: {metadata['total_questions']}개")
    
    print(f"\n📂 카테고리별 분류:")
    categories = {}
    for q in dataset["questions"]:
        cat = q["category"]
        if cat not in categories:
            categories[cat] = 0
        categories[cat] += 1
    
    for category, count in categories.items():
        print(f"  • {category}: {count}개")
    
    print(f"\n🔍 예상 질문 유형:")
    response_types = set()
    for q in dataset["questions"]:
        response_types.add(q["expected_response_type"])
    
    for response_type in sorted(response_types):
        print(f"  • {response_type}")
    
    print("\n" + "="*50)

def main():
    """메인 실행 함수"""
    print("🚀 KBO 챗봇 예상 질문-답변 데이터셋 생성 시작...")
    
    # 데이터셋 생성
    dataset = generate_kbo_qa_dataset()
    
    # 요약 정보 출력
    print_dataset_summary(dataset)
    
    # 파일로 저장
    if save_dataset_to_file(dataset):
        print(f"\n🎉 데이터셋 생성 완료!")
        print(f"📁 저장 위치: {os.path.abspath('kbo_qa_dataset.json')}")
        
        # 샘플 질문 출력
        print(f"\n📋 샘플 질문들:")
        for i, q in enumerate(dataset["questions"][:5], 1):
            print(f"  {i}. {q['question']}")
        print(f"  ... 총 {len(dataset['questions'])}개 질문")
    else:
        print("❌ 데이터셋 저장에 실패했습니다.")

if __name__ == "__main__":
    main()
