#!/usr/bin/env python3
"""
스키마 정보를 관리하고 RAG 방식으로 제공하는 모듈
"""

import json
import os
import numpy as np
import tensorflow as tf
from typing import Dict, List, Any, Optional
from langchain_openai import ChatOpenAI
from langchain.prompts import ChatPromptTemplate
from langchain_openai import OpenAIEmbeddings
from langchain_community.vectorstores import FAISS
from langchain.schema import Document
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split

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
            self.question_classifier = None
            self.label_encoder = LabelEncoder()
            self._build_question_classifier()
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
                        "id": {"type": "SERIAL", "primary_key": True, "description": "테이블 기본 키"},
                        "game_id": {"type": "VARCHAR(50)", "description": "경기 고유 ID"},
                        "date": {"type": "VARCHAR(20)", "description": "경기 날짜 (YYYYMMDD 형식)"},
                        "home_team": {"type": "VARCHAR(50)", "description": "홈팀 이름"},
                        "away_team": {"type": "VARCHAR(50)", "description": "원정팀 이름"},
                        "home_team_code": {"type": "VARCHAR(10)", "description": "홈팀 코드"},
                        "away_team_code": {"type": "VARCHAR(10)", "description": "원정팀 코드"},
                        "stadium": {"type": "VARCHAR(100)", "description": "경기장"},
                        "time": {"type": "VARCHAR(20)", "description": "경기 시간"},
                        "game_data": {"type": "JSONB", "description": "경기 상세 데이터"}
                    }
                },
                "game_result": {
                    "description": "팀별 시즌 통계 및 순위를 저장하는 테이블",
                    "columns": {
                        "idx": {"type": "INTEGER", "description": "인덱스"},
                        "team_id": {"type": "VARCHAR(10)", "description": "팀 코드 (HH, OB, HT, WO, LT, SS, SK, KT, NC, LG)"},
                        "team_name": {"type": "VARCHAR(50)", "description": "팀 이름"},
                        "season_id": {"type": "INTEGER", "description": "시즌 ID"},
                        "year": {"type": "INTEGER", "description": "시즌 연도"},
                        "ranking": {"type": "INTEGER", "description": "순위"},
                        "order_no": {"type": "INTEGER", "description": "정렬 순서"},
                        "wra": {"type": "DECIMAL(4,3)", "description": "승률"},
                        "game_count": {"type": "INTEGER", "description": "총 경기 수"},
                        "win_game_count": {"type": "INTEGER", "description": "승수"},
                        "drawn_game_count": {"type": "INTEGER", "description": "무승부 수"},
                        "lose_game_count": {"type": "INTEGER", "description": "패수"},
                        "game_behind": {"type": "VARCHAR(10)", "description": "게임차"},
                        "continuous_game_result": {"type": "VARCHAR(20)", "description": "연속 경기 결과"},
                        "last_five_games": {"type": "VARCHAR(10)", "description": "최근 5경기 결과 (W/L)"},
                        "offense_hra": {"type": "DECIMAL(6,5)", "description": "팀 타율"},
                        "offense_run": {"type": "INTEGER", "description": "팀 득점"},
                        "offense_rbi": {"type": "INTEGER", "description": "팀 타점"},
                        "offense_ab": {"type": "INTEGER", "description": "팀 타석"},
                        "offense_hr": {"type": "INTEGER", "description": "팀 홈런"},
                        "offense_hit": {"type": "INTEGER", "description": "팀 안타"},
                        "offense_h2": {"type": "INTEGER", "description": "팀 2루타"},
                        "offense_h3": {"type": "INTEGER", "description": "팀 3루타"},
                        "offense_sb": {"type": "INTEGER", "description": "팀 도루"},
                        "offense_bbhp": {"type": "INTEGER", "description": "팀 볼넷+사구"},
                        "offense_kk": {"type": "INTEGER", "description": "팀 삼진"},
                        "offense_gd": {"type": "INTEGER", "description": "팀 병살"},
                        "offense_obp": {"type": "DECIMAL(6,5)", "description": "팀 출루율"},
                        "offense_slg": {"type": "DECIMAL(6,5)", "description": "팀 장타율"},
                        "offense_ops": {"type": "DECIMAL(6,5)", "description": "팀 OPS"},
                        "defense_era": {"type": "DECIMAL(6,5)", "description": "팀 평균자책점"},
                        "defense_r": {"type": "INTEGER", "description": "팀 실점"},
                        "defense_er": {"type": "INTEGER", "description": "팀 자책점"},
                        "defense_inning": {"type": "DECIMAL(6,1)", "description": "팀 이닝"},
                        "defense_hit": {"type": "INTEGER", "description": "팀 피안타"},
                        "defense_hr": {"type": "INTEGER", "description": "팀 피홈런"},
                        "defense_kk": {"type": "INTEGER", "description": "팀 탈삼진"},
                        "defense_bbhp": {"type": "INTEGER", "description": "팀 볼넷+사구"},
                        "defense_err": {"type": "INTEGER", "description": "팀 실책"},
                        "defense_whip": {"type": "DECIMAL(6,5)", "description": "팀 WHIP"},
                        "defense_qs": {"type": "INTEGER", "description": "팀 퀄리티 스타트"},
                        "defense_save": {"type": "INTEGER", "description": "팀 세이브"},
                        "defense_hold": {"type": "INTEGER", "description": "팀 홀드"},
                        "defense_wp": {"type": "INTEGER", "description": "팀 승리"},
                        "has_my_team": {"type": "VARCHAR(1)", "description": "내 팀 여부"},
                        "my_team_category_id": {"type": "VARCHAR(10)", "description": "내 팀 카테고리 ID"},
                        "next_schedule_game_id": {"type": "VARCHAR(50)", "description": "다음 경기 ID"},
                        "opposing_team_name": {"type": "VARCHAR(50)", "description": "상대팀 이름"}
                    }
                }
            },
            "relationships": [
                "player_season_stats.team = game_result.team_id",
                "game_schedule.home_team_name = game_result.team_name",
                "game_schedule.away_team_name = game_result.team_name"
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
                    "keywords": ["이길", "질 것", "예상", "승부", "누가", "어떤 팀", "결과", "예측", "이길거같", "질거같", "승리", "패배", "누가 이길", "어떤 팀이", "승부 예상", "경기 예상", "이길까", "질까", "승부는", "결과는", "이길 것 같", "질 것 같", "승부 예상", "경기 결과 예상", "누가 이길까", "어떤 팀이 이길까", "경기 승부 예상", "경기 결과 예측"],
                    "table": "game_schedule",
                    "description": "경기 결과 예측 및 승부 예상 질문 - 팀별 최근 성적과 상대 전적을 고려하여 경기 결과를 예측하고 승부를 예상하는 질문"
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
                },
                "game_analysis": {
                    "keywords": ["경기 분석", "경기 결과", "경기 요약", "경기 리뷰", "경기 상세", "경기 기록", "경기 통계", "경기 성과", "경기 평가", "어떻게", "어땠어", "어땠는지", "어땠나", "경기 내용", "경기 상황", "경기 흐름", "경기 정리", "경기 후기", "경기 소감", "승부는", "결과는", "스코어는", "점수는"],
                    "table": "game_schedule",
                    "description": "경기 분석 및 상세 정보 관련 질문 - 특정 날짜의 경기 기록을 분석하여 요약 제공"
                },
        "game_schedule_relative": {
            "keywords": ["어제 경기", "오늘 경기", "내일 경기", "최근 경기", "지난 경기", "이번 경기", "저번 경기"],
            "table": "game_schedule", 
            "description": "상대적 날짜를 포함한 경기 일정 관련 질문"
        },
        "future_game_info": {
            "keywords": ["선발투수", "선발", "투수", "라인업", "출전", "선수", "누구", "어디서", "언제", "몇시", "경기장", "상대팀", "내일", "모레", "다음", "이번 주", "다음 주", "앞으로", "예정", "경기 정보", "경기 상세", "경기 세부사항"],
            "table": "game_schedule",
            "description": "미래 경기의 상세 정보 질문 - 선발투수, 라인업, 경기장, 시간 등 구체적인 경기 정보"
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
    
    def _build_question_classifier(self):
        """텐서플로우 기반 질문 분류 모델 구축"""
        try:
            print("🔄 [1/6] 텐서플로우 질문 분류 모델 구축 시작...")
            
            # 훈련 데이터 생성
            print("🔄 [2/6] 훈련 데이터 생성 중...")
            training_data = self._generate_training_data()
            print(f"📊 생성된 훈련 데이터: {len(training_data)}개")
            
            if not training_data:
                print("⚠️ 훈련 데이터가 없어서 기본 분류기 사용")
                return
            
            # 데이터 준비
            print("🔄 [3/6] 데이터 준비 중...")
            questions = [item['question'] for item in training_data]
            labels = [item['label'] for item in training_data]
            print(f"📊 질문 수: {len(questions)}개, 라벨 수: {len(set(labels))}개")
            
            # 임베딩 캐시 확인 및 생성
            print("🔄 [4/6] 임베딩 캐시 확인 중...")
            cache_file = "embeddings_cache.pkl"
            question_embeddings = self._load_or_generate_embeddings(questions, cache_file)
            
            print("🔄 [5/6] 신경망 모델 구축 및 훈련 중...")
            X = np.array(question_embeddings)
            y = self.label_encoder.fit_transform(labels)
            
            # 훈련/테스트 분할
            X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
            print(f"📊 훈련 데이터: {len(X_train)}개, 테스트 데이터: {len(X_test)}개")
            
            # 모델 구축
            print("   🏗️ 신경망 구조 생성 중...")
            model = tf.keras.Sequential([
                tf.keras.layers.Dense(128, activation='relu', input_shape=(X.shape[1],)),
                tf.keras.layers.Dropout(0.3),
                tf.keras.layers.Dense(64, activation='relu'),
                tf.keras.layers.Dropout(0.3),
                tf.keras.layers.Dense(len(np.unique(y)), activation='softmax')
            ])
            
            model.compile(
                optimizer='adam',
                loss='sparse_categorical_crossentropy',
                metrics=['accuracy']
            )
            
            # 모델 훈련
            print("   🎯 모델 훈련 시작 (50 에포크, 배치 크기 32)...")
            model.fit(X_train, y_train, epochs=50, batch_size=32, validation_data=(X_test, y_test), verbose=0)
            print("   ✅ 모델 훈련 완료")
            
            print("🔄 [6/6] 모델 평가 중...")
            self.question_classifier = model
            accuracy = model.evaluate(X_test, y_test, verbose=0)[1]
            print(f"✅ 질문 분류 모델 훈련 완료 - 정확도: {accuracy:.3f}")
            
        except Exception as e:
            print(f"❌ 질문 분류 모델 구축 실패: {e}")
            self.question_classifier = None
    
    def _load_or_generate_embeddings(self, questions, cache_file):
        """임베딩 캐시를 로드하거나 새로 생성"""
        import pickle
        import hashlib
        
        # 질문들의 해시 생성 (질문이 변경되었는지 확인)
        questions_hash = hashlib.md5(str(sorted(questions)).encode()).hexdigest()
        
        try:
            # 캐시 파일이 존재하는지 확인
            if os.path.exists(cache_file):
                with open(cache_file, 'rb') as f:
                    cache_data = pickle.load(f)
                
                # 해시가 일치하면 캐시된 임베딩 사용
                if cache_data.get('hash') == questions_hash:
                    print(f"✅ 임베딩 캐시 로드 완료: {len(cache_data['embeddings'])}개")
                    return cache_data['embeddings']
                else:
                    print("⚠️ 질문이 변경되어 캐시 무효화, 새로 생성합니다")
            else:
                print("📁 캐시 파일이 없어서 새로 생성합니다")
        except Exception as e:
            print(f"⚠️ 캐시 로드 실패: {e}, 새로 생성합니다")
        
        # 새로 임베딩 생성
        print("🔄 OpenAI 임베딩 API 호출 중... (시간이 오래 걸릴 수 있습니다)")
        question_embeddings = []
        total_questions = len(questions)
        for i, question in enumerate(questions):
            if i % 10 == 0:  # 10개마다 진행상황 출력
                print(f"   📡 임베딩 진행률: {i+1}/{total_questions} ({(i+1)/total_questions*100:.1f}%)")
            embedding = self.embeddings.embed_query(question)
            question_embeddings.append(embedding)
        
        # 캐시에 저장
        try:
            cache_data = {
                'hash': questions_hash,
                'embeddings': question_embeddings,
                'questions': questions
            }
            with open(cache_file, 'wb') as f:
                pickle.dump(cache_data, f)
            print(f"💾 임베딩 캐시 저장 완료: {cache_file}")
        except Exception as e:
            print(f"⚠️ 캐시 저장 실패: {e}")
        
        print(f"✅ 임베딩 완료: {len(question_embeddings)}개")
        return question_embeddings
    
    def _generate_training_data(self):
        """질문 분류를 위한 훈련 데이터 생성"""
        training_data = []
        
        # 각 질문 유형별로 훈련 데이터 생성
        question_types = self.schema_info["question_types"]
        print(f"   📋 질문 유형 수: {len(question_types)}개")
        
        for qtype, qinfo in question_types.items():
            print(f"   🔍 '{qtype}' 유형 처리 중... (키워드: {len(qinfo['keywords'])}개)")
            # 기본 키워드로 질문 생성
            for keyword in qinfo["keywords"][:5]:  # 상위 5개 키워드만 사용
                # 다양한 패턴의 질문 생성
                patterns = [
                    f"{keyword}",
                    f"한화 {keyword}",
                    f"두산 {keyword}",
                    f"문동주 {keyword}",
                    f"오늘 {keyword}",
                    f"내일 {keyword}",
                    f"모레 {keyword}",
                    f"글피 {keyword}",
                    f"어제 {keyword}",
                    f"다음 {keyword}",
                    f"다음 주 {keyword}",
                    f"이번 주 {keyword}",
                    f"앞으로 {keyword}",
                    f"앞으로 남은 {keyword}",
                    f"이번 시즌 {keyword}",
                    f"올해 {keyword}",
                    f"9월 25일 {keyword}",
                    f"9/25 {keyword}",
                    f"25일 {keyword}"
                ]
                
                for pattern in patterns:
                    training_data.append({
                        'question': pattern,
                        'label': qtype
                    })
        
        print(f"   ✅ 총 {len(training_data)}개의 훈련 데이터 생성 완료")
        return training_data
    
    def _classify_question_with_tensorflow(self, question: str):
        """텐서플로우 모델을 사용한 질문 분류"""
        try:
            # 질문을 임베딩으로 변환
            question_embedding = self.embeddings.embed_query(question)
            X = np.array([question_embedding])
            
            # 예측
            predictions = self.question_classifier.predict(X, verbose=0)
            predicted_class_idx = np.argmax(predictions[0])
            confidence = predictions[0][predicted_class_idx]
            
            # 클래스 이름으로 변환
            predicted_class = self.label_encoder.inverse_transform([predicted_class_idx])[0]
            
            print(f"🤖 텐서플로우 분류 결과: {predicted_class} (신뢰도: {confidence:.3f})")
            
            # 해당 질문 유형 정보 반환
            if predicted_class in self.schema_info["question_types"]:
                qinfo = self.schema_info["question_types"][predicted_class]
                return [{
                    "type": predicted_class,
                    "score": confidence * 100,
                    "confidence": confidence,
                    "content": f"질문 유형: {predicted_class}\n키워드: {', '.join(qinfo['keywords'])}\n테이블: {qinfo['table']}\n설명: {qinfo['description']}"
                }]
            else:
                return []
                
        except Exception as e:
            print(f"❌ 텐서플로우 분류 오류: {e}")
            return self._classify_question_with_vectors(question)
    
    def _classify_question_with_vectors(self, question: str):
        """벡터 기반 질문 분류 (폴백)"""
        scored_question_types = []
        
        for qtype, qinfo in self.schema_info["question_types"].items():
            # 질문 유형의 설명과 키워드를 하나의 텍스트로 결합
            qtype_text = f"{qinfo['description']} {' '.join(qinfo['keywords'])}"
            
            # 질문과 질문 유형 텍스트의 유사도 계산
            question_embedding = self.embeddings.embed_query(question)
            qtype_embedding = self.embeddings.embed_query(qtype_text)
            
            # 코사인 유사도 계산
            similarity = np.dot(question_embedding, qtype_embedding) / (
                np.linalg.norm(question_embedding) * np.linalg.norm(qtype_embedding)
            )
            
            # 유사도를 0-100 점수로 변환
            score = similarity * 100
            
            if score > 30:  # 임계값 설정 (30점 이상만 고려)
                scored_question_types.append({
                    "type": qtype,
                    "score": score,
                    "similarity": similarity,
                    "content": f"질문 유형: {qtype}\n키워드: {', '.join(qinfo['keywords'])}\n테이블: {qinfo['table']}\n설명: {qinfo['description']}"
                })
        
        # 점수순으로 정렬하여 가장 높은 점수의 질문 유형 선택
        scored_question_types.sort(key=lambda x: x["score"], reverse=True)
        
        print(f"🎯 벡터 기반 질문 유형 매칭 점수:")
        for qt in scored_question_types[:3]:  # 상위 3개만 출력
            print(f"  - {qt['type']}: {qt['score']:.1f}점 (유사도: {qt['similarity']:.3f})")
        
        # 가장 높은 점수의 질문 유형을 선택
        if scored_question_types:
            best_match = scored_question_types[0]
            print(f"🏆 선택된 질문 유형: {best_match['type']} ({best_match['score']:.1f}점)")
            return [best_match]
        else:
            print(f"⚠️ 매칭되는 질문 유형 없음 - 기본 처리")
            return []
    
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
            
            # 텐서플로우 기반 질문 분류
            if self.question_classifier is not None:
                question_types = self._classify_question_with_tensorflow(question)
            else:
                # 폴백: 벡터 기반 매칭
                question_types = self._classify_question_with_vectors(question)
            
            print(f"📊 관련 테이블: {list(relevant_tables)}")
            
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
1. 팀명은 home_team_name 또는 away_team_name 컬럼에서 사용하세요:
   - "한화" → home_team_name = '한화' OR away_team_name = '한화'
   - "두산" → home_team_name = '두산' OR away_team_name = '두산'
   - "KIA" → home_team_name = 'KIA' OR away_team_name = 'KIA'
   - "키움" → home_team_name = '키움' OR away_team_name = '키움'
   - "롯데" → home_team_name = '롯데' OR away_team_name = '롯데'
   - "삼성" → home_team_name = '삼성' OR away_team_name = '삼성'
   - "SSG" → home_team_name = 'SSG' OR away_team_name = 'SSG'
   - "KT" → home_team_name = 'KT' OR away_team_name = 'KT'
   - "NC" → home_team_name = 'NC' OR away_team_name = 'NC'
   - "LG" → home_team_name = 'LG' OR away_team_name = 'LG'

2. **중요**: "다음 경기" 질문의 경우 반드시 game_date::date >= CURRENT_DATE 조건을 사용하세요
3. **중요**: team 컬럼은 존재하지 않습니다. home_team_name, away_team_name을 사용하세요
4. **중요**: gyear 컬럼은 존재하지 않습니다. game_date를 사용하세요
5. 선수명은 그대로 사용하세요 (팀 코드로 변환하지 마세요)
6. **경기 예측 질문의 경우**: 팀별 최근 성적과 상대 전적을 고려하여 구체적인 예측을 제공하세요
7. **홈구장 정보**: 롯데는 사직, 한화는 대전, 삼성은 대구, SSG는 문학, KT는 수원, NC는 창원, KIA는 광주, 키움은 고척, 두산/LG는 잠실
8. 컬럼명 매핑 규칙:
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
            
            # 경기 일정 관련 질문에 대한 가이드라인 추가
            if "game_schedule" in relevant_schema.get("tables", {}):
                prompt += f"""

경기 일정 관련 SQL 작성 가이드라인:
- 날짜 비교 시 game_date::date를 사용하여 타입 캐스팅
- 다음 경기 조회 시 반드시 game_date::date >= CURRENT_DATE 조건 사용
- 특정 날짜 조회 시 game_date = 'YYYY-MM-DD' 형식 사용
- 정렬 시 game_date::date, game_date_time 순서로 정렬
- 팀별 경기 조회 시 home_team_name = '팀명' OR away_team_name = '팀명' 조건 사용
- "다음 경기" 질문의 경우 반드시 CURRENT_DATE 이상의 날짜만 조회
- LIMIT 1을 사용하여 가장 가까운 경기 1개만 조회

다음 경기 조회 예시:
SELECT game_date, home_team_name, away_team_name, stadium, game_date_time, status_info
FROM game_schedule 
WHERE (home_team_name = '한화' OR away_team_name = '한화')
AND game_date::date >= CURRENT_DATE
ORDER BY game_date::date, game_date_time
LIMIT 1;
"""
            
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
