# 한화이글스 AI 챗봇 🦅

한화이글스 전용 AI 어시스턴트입니다.

## 기능

- 📋 **라인업 정보**: 오늘의 선발 라인업과 선발투수 정보
- 👨‍⚾ **선수 정보**: 개별 선수의 상세 정보 (성적, 포지션 등)
- 📅 **경기 일정**: 다음 경기 일정 및 최근 경기 결과
- 🏟️ **팀 정보**: 한화이글스 기본 정보 (홈구장, 창단년도 등)

## 설치 및 실행

### 1. 환경 설정

```bash
# 가상환경 생성 (권장)
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# 패키지 설치
pip install -r requirements.txt
```

### 2. 환경 변수 설정

`env_example.txt` 파일을 참고하여 `.env` 파일을 생성하세요:

```bash
# .env 파일 생성
OPENAI_API_KEY=your_openai_api_key_here
FLASK_ENV=development
FLASK_DEBUG=True
```

### 3. 서버 실행

```bash
python app.py
```

서버가 `http://localhost:5000`에서 실행됩니다.

## API 엔드포인트

### 1. 건강 상태 확인
```
GET /health
```

### 2. AI 챗봇 (메인)
```
POST /chat
Content-Type: application/json

{
  "message": "오늘 라인업 알려줘",
  "user_id": "user_id"
}
```

### 3. 테스트용 (간단한 형식)
```
POST /test
Content-Type: application/json

{
  "message": "오늘 라인업 알려줘"
}
```

## 테스트

```bash
# 테스트 클라이언트 실행 (서버가 실행 중일 때)
python test_client.py
```

## 질문 예시

- "오늘 라인업 알려줘"
- "이진영 선발로 나와?"
- "문동주 선수 정보 알려줘"
- "다음 경기 언제야?"
- "한화이글스 홈구장이 어디야?"
- "최근 경기 결과 알려줘"

## 데이터 관리

`hanwha_eagles_data.json` 파일에서 선수 정보, 라인업, 경기 일정 등을 관리할 수 있습니다.

### 데이터 업데이트 방법

1. `hanwha_eagles_data.json` 파일을 직접 수정
2. `data_manager.py`의 `update_lineup()`, `add_game_result()` 메소드 사용

## 배포

### Heroku 배포 예시

```bash
# Procfile 생성
echo "web: gunicorn app:app" > Procfile

# Heroku CLI 설치 후
heroku create your-app-name
heroku config:set OPENAI_API_KEY=your_api_key
git push heroku main
```

## 라이선스

MIT License # hhbot
# hhbot
# hhbot
# hanwha-eagles-chatbot
