#!/usr/bin/env python3
"""
스키마 정보를 관리하고 RAG 방식으로 제공하는 모듈
"""

import json
import os
from typing import Dict, List, Any, Optional
from langchain_openai import ChatOpenAI
from langchain.prompts import ChatPromptTemplate
from langchain_openai import OpenAIEmbeddings
from langchain_community.vectorstores import FAISS
from langchain.schema import Document

class SchemaManager:
    def __init__(self):
        """스키마 매니저 초기화"""
        try:
            self.llm = ChatOpenAI(
                model="gpt-4o-mini",
                temperature=0.1,
                api_key=os.getenv("OPENAI_API_KEY")
            )
            self.embeddings = OpenAIEmbeddings(api_key=os.getenv("OPENAI_API_KEY"))
            self.vectorstore = None
            self.schema_info = self._load_schema_info()
            self._build_vectorstore()
            print("✅ 스키마 매니저 초기화 완료")
            
        except Exception as e:
            print(f"❌ 스키마 매니저 초기화 실패: {e}")
            raise e
    
    def _load_schema_info(self) -> Dict[str, Any]:
        """스키마 정보 로드"""
        return {
            "tables": {
                "player_season_stats": {
                    "description": "선수 시즌별 통계를 저장하는 테이블",
                    "columns": {
                        "player_id": {"type": "INTEGER", "description": "선수 ID"},
                        "player_name": {"type": "VARCHAR(100)", "nullable": False, "description": "선수 이름 (player name) - 선수의 실제 이름", "aliases": ["선수명", "player name", "name"]},
                        "gyear": {"type": "VARCHAR(10)", "description": "시즌 연도 (예: 2025) - 현재 시즌 데이터를 조회할 때는 gyear = '2025' 조건을 반드시 포함해야 함", "aliases": ["시즌", "연도", "year", "season"]},
                        "team": {"type": "VARCHAR(10)", "description": "소속 팀명 (team name) - 선수가 소속된 팀의 이름 (예: 한화, 두산, KIA, 키움, 롯데, 삼성, SSG, KT, NC, LG)", "aliases": ["팀", "team", "팀명", "team_name"]},
                        "hra": {"type": "DECIMAL(4,3)", "description": "타율 (hitting average) - 안타수/타석수", "aliases": ["타율", "batting average", "avg"]},
                        "hr": {"type": "INTEGER", "description": "홈런 개수 (home runs) - 홈런을 친 횟수", "aliases": ["홈런", "home runs", "home_runs", "홈런개수"]},
                        "rbi": {"type": "INTEGER", "description": "타점 (runs batted in) - 타자가 친 안타로 인한 득점", "aliases": ["타점", "RBI", "runs batted in"]},
                        "era": {"type": "DECIMAL(4,2)", "description": "평균자책점 (earned run average) - 9이닝당 평균 자책점", "aliases": ["평균자책점", "ERA", "earned run average"]},
                        "w": {"type": "INTEGER", "description": "승수 (wins) - 투수가 기록한 승리 횟수", "aliases": ["승수", "wins", "승", "W"]},
                        "l": {"type": "INTEGER", "description": "패수 (losses) - 투수가 기록한 패배 횟수", "aliases": ["패수", "losses", "패", "L"]},
                        "sv": {"type": "INTEGER", "description": "세이브 (saves) - 마무리 투수가 기록한 세이브 횟수", "aliases": ["세이브", "saves", "SV"]},
                        "hold": {"type": "INTEGER", "description": "홀드 (holds) - 중간 계투 투수가 기록한 홀드 횟수", "aliases": ["홀드", "holds", "HOLD"]}
                    }
                },
                "game_schedule": {
                    "description": "경기 일정 및 결과를 저장하는 테이블",
                    "columns": {
                        "game_id": {"type": "VARCHAR(50)", "primary_key": True, "description": "경기 고유 ID"},
                        "game_date": {"type": "DATE", "description": "경기 날짜"},
                        "game_date_time": {"type": "TIMESTAMP", "description": "경기 시작 시간"},
                        "stadium": {"type": "VARCHAR(100)", "description": "경기장"},
                        "home_team_code": {"type": "VARCHAR(10)", "description": "홈팀 코드"},
                        "home_team_name": {"type": "VARCHAR(50)", "description": "홈팀 이름"},
                        "away_team_code": {"type": "VARCHAR(10)", "description": "원정팀 코드"},
                        "away_team_name": {"type": "VARCHAR(50)", "description": "원정팀 이름"},
                        "home_team_score": {"type": "INTEGER", "description": "홈팀 점수"},
                        "away_team_score": {"type": "INTEGER", "description": "원정팀 점수"},
                        "winner": {"type": "VARCHAR(10)", "description": "승리팀"},
                        "status_code": {"type": "VARCHAR(20)", "description": "경기 상태 (BEFORE, RESULT 등)"},
                        "status_info": {"type": "VARCHAR(50)", "description": "경기 상태 정보"}
                    }
                },
                "game_result": {
                    "description": "팀 순위 및 통계를 저장하는 테이블",
                    "columns": {
                        "team_id": {"type": "VARCHAR(10)", "description": "팀 코드"},
                        "team_name": {"type": "VARCHAR(50)", "description": "팀 이름"},
                        "year": {"type": "VARCHAR(10)", "description": "시즌 연도"},
                        "ranking": {"type": "INTEGER", "description": "순위"},
                        "wra": {"type": "DECIMAL(4,3)", "description": "승률"},
                        "win_game_count": {"type": "INTEGER", "description": "승수"},
                        "lose_game_count": {"type": "INTEGER", "description": "패수"},
                        "offense_hra": {"type": "DECIMAL(4,3)", "description": "팀 타율"},
                        "offense_hr": {"type": "INTEGER", "description": "팀 홈런"},
                        "defense_era": {"type": "DECIMAL(4,2)", "description": "팀 평균자책점"}
                    }
                }
            },
            "relationships": [
                "player_season_stats.team = game_result.team_id"
            ],
            "team_mappings": {
                "한화": "HH", "두산": "OB", "KIA": "HT", "키움": "WO",
                "롯데": "LT", "삼성": "SS", "SSG": "SK", "KT": "KT",
                "NC": "NC", "LG": "LG"
            },
            "team_stadiums": {
                "한화": "대전", "두산": "잠실", "KIA": "광주", "키움": "고척",
                "롯데": "사직", "삼성": "대구", "SSG": "문학", "KT": "수원",
                "NC": "창원", "LG": "잠실"
            },
            "question_types": {
                "schedule": {
                    "keywords": ["일정", "경기", "오늘", "내일", "어제", "다음", "이번 주", "스케줄"],
                    "table": "game_schedule",
                    "description": "경기 일정 관련 질문"
                },
                "game_prediction": {
                    "keywords": ["이길", "질 것", "예상", "승부", "누가", "어떤 팀", "결과", "예측"],
                    "table": "game_schedule",
                    "description": "경기 결과 예측 관련 질문 - 팀별 최근 성적과 상대 전적을 고려한 예측 제공"
                },
                "player_stats": {
                    "keywords": ["성적", "어때", "어떻게", "통계", "타율", "홈런", "홈런개수", "home runs", "home_runs", "ERA", "승수", "타점", "RBI", "세이브", "홀드"],
                    "table": "player_season_stats",
                    "description": "선수 성적 관련 질문"
                },
                "home_run_leader": {
                    "keywords": ["홈런", "홈런개수", "home runs", "home_runs", "홈런왕", "홈런 1위", "홈런 최다", "가장 많이", "제일 많이"],
                    "table": "player_season_stats",
                    "description": "홈런 관련 질문 (hr 컬럼 사용)"
                },
                "team_ranking": {
                    "keywords": ["순위", "1위", "최고", "가장", "상위", "하위", "올해", "이번 시즌"],
                    "table": "game_result",
                    "description": "팀 순위 관련 질문"
                },
                "historical": {
                    "keywords": ["우승년도", "마지막 우승", "과거 성적", "역사", "최근 경기 결과"],
                    "table": None,
                    "description": "과거 기록 관련 질문 (DB에 없음)"
                }
            }
        }
    
    def _build_vectorstore(self):
        """벡터 스토어 구축"""
        try:
            documents = []
            
            # 테이블 정보를 문서로 변환
            for table_name, table_info in self.schema_info["tables"].items():
                doc_content = f"테이블: {table_name}\n"
                doc_content += f"설명: {table_info['description']}\n"
                doc_content += "컬럼:\n"
                
                for col_name, col_info in table_info["columns"].items():
                    doc_content += f"- {col_name}: {col_info['type']}"
                    if col_info.get('primary_key'):
                        doc_content += " (PRIMARY KEY)"
                    if col_info.get('foreign_key'):
                        doc_content += f" (FOREIGN KEY: {col_info['foreign_key']})"
                    if col_info.get('description'):
                        doc_content += f" - {col_info['description']}"
                    if col_info.get('aliases'):
                        doc_content += f" [별칭: {', '.join(col_info['aliases'])}]"
                    doc_content += "\n"
                
                documents.append(Document(page_content=doc_content, metadata={"table": table_name}))
            
            # 관계 정보 추가
            rel_doc = "테이블 관계:\n" + "\n".join(self.schema_info["relationships"])
            documents.append(Document(page_content=rel_doc, metadata={"type": "relationships"}))
            
            # 팀 매핑 정보 추가
            team_doc = "팀 코드 매핑:\n" + json.dumps(self.schema_info["team_mappings"], ensure_ascii=False, indent=2)
            documents.append(Document(page_content=team_doc, metadata={"type": "team_mappings"}))
            
            # 팀 홈구장 정보 추가
            stadium_doc = "팀 홈구장 매핑:\n" + json.dumps(self.schema_info["team_stadiums"], ensure_ascii=False, indent=2)
            documents.append(Document(page_content=stadium_doc, metadata={"type": "team_stadiums"}))
            
            # 질문 유형 정보 추가
            for qtype, qinfo in self.schema_info["question_types"].items():
                qtype_doc = f"질문 유형: {qtype}\n"
                qtype_doc += f"키워드: {', '.join(qinfo['keywords'])}\n"
                qtype_doc += f"테이블: {qinfo['table']}\n"
                qtype_doc += f"설명: {qinfo['description']}"
                documents.append(Document(page_content=qtype_doc, metadata={"type": "question_type", "qtype": qtype}))
            
            # 벡터 스토어 생성
            self.vectorstore = FAISS.from_documents(documents, self.embeddings)
            print(f"✅ 벡터 스토어 구축 완료 - {len(documents)}개 문서")
            
        except Exception as e:
            print(f"❌ 벡터 스토어 구축 실패: {e}")
            raise e
    
    def get_relevant_schema(self, question: str, top_k: int = 5) -> Dict[str, Any]:
        """질문에 관련된 스키마 정보 검색"""
        try:
            if not self.vectorstore:
                raise Exception("벡터 스토어가 초기화되지 않았습니다")
            
            # 관련 문서 검색
            docs = self.vectorstore.similarity_search(question, k=top_k)
            
            # 검색된 정보 정리
            relevant_tables = set()
            relationships = []
            team_mappings = {}
            question_types = []
            
            for doc in docs:
                metadata = doc.metadata
                
                if metadata.get("table"):
                    relevant_tables.add(metadata["table"])
                elif metadata.get("type") == "relationships":
                    relationships.append(doc.page_content)
                elif metadata.get("type") == "team_mappings":
                    team_mappings = json.loads(doc.page_content.split("팀 코드 매핑:\n")[1])
                elif metadata.get("type") == "question_type":
                    question_types.append({
                        "type": metadata["qtype"],
                        "content": doc.page_content
                    })
            
            # 관련 테이블 정보만 추출
            relevant_schema = {
                "tables": {table: self.schema_info["tables"][table] for table in relevant_tables},
                "relationships": relationships,
                "team_mappings": team_mappings,
                "question_types": question_types
            }
            
            print(f"🔍 관련 스키마 검색 완료 - {len(relevant_tables)}개 테이블")
            return relevant_schema
            
        except Exception as e:
            print(f"❌ 관련 스키마 검색 실패: {e}")
            return {}
    
    def generate_dynamic_prompt(self, question: str) -> str:
        """동적 프롬프트 생성"""
        try:
            # 관련 스키마 정보 검색
            relevant_schema = self.get_relevant_schema(question)
            
            if not relevant_schema:
                return self._get_fallback_prompt(question)
            
            # 동적 프롬프트 생성
            prompt = f"""당신은 KBO 데이터베이스 전문가입니다. 사용자의 질문을 SQL 쿼리로 변환해주세요.

⚠️ 중요한 규칙 ⚠️
1. 팀명은 그대로 사용하세요 (팀 코드로 변환하지 마세요):
   - "한화" → team = '한화'
   - "두산" → team = '두산'
   - "KIA" → team = 'KIA'
   - "키움" → team = '키움'
   - "롯데" → team = '롯데'
   - "삼성" → team = '삼성'
   - "SSG" → team = 'SSG'
   - "KT" → team = 'KT'
   - "NC" → team = 'NC'
   - "LG" → team = 'LG'

2. 질문에 언급된 팀명을 그대로 사용하세요
3. 선수명은 그대로 사용하세요 (팀 코드로 변환하지 마세요)
4. **중요**: 현재 시즌 데이터를 조회할 때는 반드시 gyear = '2025' 조건을 포함하세요
5. **경기 예측 질문의 경우**: 팀별 최근 성적과 상대 전적을 고려하여 구체적인 예측을 제공하세요
6. **홈구장 정보**: 롯데는 사직, 한화는 대전, 삼성은 대구, SSG는 문학, KT는 수원, NC는 창원, KIA는 광주, 키움은 고척, 두산/LG는 잠실
7. 컬럼명 매핑 규칙:
   - "선수명", "player name", "name" → player_name 컬럼 사용
   - "홈런", "홈런개수", "home runs", "home_runs" → hr 컬럼 사용
   - "타율", "batting average", "avg" → hra 컬럼 사용
   - "타점", "RBI", "runs batted in" → rbi 컬럼 사용
   - "승수", "wins" → w 컬럼 사용
   - "패수", "losses" → l 컬럼 사용
   - "세이브", "saves" → sv 컬럼 사용
   - "홀드", "holds" → hold 컬럼 사용
   - "평균자책점", "ERA" → era 컬럼 사용

사용 가능한 테이블:
"""
            
            # 테이블 정보 추가
            for table_name, table_info in relevant_schema.get("tables", {}).items():
                prompt += f"\n{table_name} 테이블:\n"
                prompt += f"설명: {table_info['description']}\n"
                prompt += "컬럼:\n"
                
                for col_name, col_info in table_info["columns"].items():
                    prompt += f"- {col_name}: {col_info['type']}"
                    if col_info.get('primary_key'):
                        prompt += " (PRIMARY KEY)"
                    if col_info.get('foreign_key'):
                        prompt += f" (FOREIGN KEY: {col_info['foreign_key']})"
                    if col_info.get('description'):
                        prompt += f" - {col_info['description']}"
                    if col_info.get('aliases'):
                        prompt += f" [별칭: {', '.join(col_info['aliases'])}]"
                    prompt += "\n"
            
            # 관계 정보 추가
            if relevant_schema.get("relationships"):
                prompt += "\n테이블 관계:\n"
                for rel in relevant_schema["relationships"]:
                    prompt += f"- {rel}\n"
            
            # 질문 유형별 처리 규칙 추가
            prompt += "\n질문 유형별 처리 규칙:\n"
            for qtype_info in relevant_schema.get("question_types", []):
                prompt += f"- {qtype_info['content']}\n"
            
            prompt += f"\n질문: {question}\n\nSQL:"
            
            return prompt
            
        except Exception as e:
            print(f"❌ 동적 프롬프트 생성 실패: {e}")
            return self._get_fallback_prompt(question)
    
    def _get_fallback_prompt(self, question: str) -> str:
        """Fallback 프롬프트 (기본 프롬프트)"""
        return f"""당신은 KBO 데이터베이스 전문가입니다. 사용자의 질문을 SQL 쿼리로 변환해주세요.

⚠️ 중요한 규칙 ⚠️
1. 팀명을 팀 코드로 변환하세요
2. 질문에 언급된 팀에 맞는 팀 코드를 사용하세요
3. 선수명은 그대로 사용하세요
4. 타율은 "hra" 필드만 사용하세요

질문: {question}

SQL:"""
