# KBO 챗봇 API

KBO(한국야구연맹) 데이터를 활용한 AI 챗봇 서버입니다.

## 🚀 배포 상태
- Railway 자동 배포 설정 완료
- Text-to-SQL 기반 KBO 데이터 조회 지원

## 주요 기능

- **선수 성적 조회**: 투수/타자 성적 순위 조회
- **팀별 선수 조회**: 특정 팀의 선수 정보 조회
- **경기 일정 조회**: 네이버 스포츠 API를 통한 실시간 경기 일정 조회
- **자연어 질의**: 한국어 자연어로 야구 관련 질문 가능

## API 엔드포인트

### POST /test
일반적인 챗봇 질문 처리
```json
{
  "message": "한화 타자 중에 타율 높은 순으로 5명 나열해줘"
}
```

### POST /pitcher-ranking
투수 순위 조회
```json
{
  "criteria": "era"  // era, wins, strikeouts
}
```

### GET /health
서버 상태 확인

## 설치 및 실행

1. 의존성 설치
```bash
pip install -r requirements.txt
```

2. 환경변수 설정 (.env 파일)
```
OPENAI_API_KEY=your_openai_api_key
SUPABASE_URL=your_supabase_url
SUPABASE_KEY=your_supabase_key
```

3. 서버 실행
```bash
python app.py
```

서버는 `http://localhost:5000`에서 실행됩니다.

## 프로젝트 구조

```
├── app.py                    # 메인 애플리케이션
├── requirements.txt          # 의존성 목록
├── data/                     # 데이터 관련 모듈
│   ├── supabase_client.py   # Supabase 클라이언트
│   └── player_data_scheduler.py # 선수 데이터 수집 스케줄러
└── rag/                      # RAG 관련 모듈
    └── text_to_sql.py       # Text-to-SQL 처리
```

## 지원하는 질문 유형

- **선수 성적**: "한화 타자 중 타율 높은 순으로 5명"
- **팀별 조회**: "삼성 투수들 보여줘"
- **경기 일정**: "오늘 경기 일정 알려줘"
- **순위 조회**: "KBO 홈런 순위"
