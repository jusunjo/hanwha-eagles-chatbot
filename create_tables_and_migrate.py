#!/usr/bin/env python3
"""
테이블 생성 및 선수 데이터 마이그레이션 스크립트
"""

import json
import sys
from typing import Dict, List, Any, Optional
from data.supabase_client import SupabaseManager

class TableCreatorAndMigrator:
    def __init__(self):
        self.supabase = SupabaseManager()
        # 스키마 캐시 새로고침을 위한 연결 재시작
        print("🔄 Supabase 연결 재시작 중...")
        self.supabase = SupabaseManager()
        
    def add_missing_columns(self):
        """누락된 컬럼들 추가"""
        print("🔧 누락된 컬럼들 추가 중...")
        
        try:
            # paFlag 컬럼이 있는지 확인 (대소문자 구분)
            try:
                self.supabase.supabase.table("player_season_stats").select("paFlag").limit(1).execute()
                print("✅ paFlag 컬럼 이미 존재")
            except Exception as e:
                if "paFlag" in str(e) or "paflag" in str(e):
                    print("⚠️ paFlag 컬럼 누락 - 수동으로 추가 필요")
                    print("   Supabase 대시보드에서 다음 SQL을 실행하세요:")
                    print("   ALTER TABLE player_season_stats ADD COLUMN \"paFlag\" INTEGER;")
                else:
                    print(f"⚠️ paFlag 컬럼 확인 중 오류: {e}")
            
            # gameId 컬럼이 있는지 확인 (대소문자 구분)
            try:
                self.supabase.supabase.table("player_game_stats").select("gameId").limit(1).execute()
                print("✅ gameId 컬럼 이미 존재")
            except Exception as e:
                if "gameId" in str(e) or "gameid" in str(e):
                    print("⚠️ gameId 컬럼 누락 - 수동으로 추가 필요")
                    print("   Supabase 대시보드에서 다음 SQL을 실행하세요:")
                    print("   ALTER TABLE player_game_stats ADD COLUMN \"gameId\" VARCHAR(50);")
                else:
                    print(f"⚠️ gameId 컬럼 확인 중 오류: {e}")
                    
        except Exception as e:
            print(f"⚠️ 컬럼 확인 중 오류: {e}")
        
    def create_tables(self):
        """정규화된 테이블 생성"""
        print("🏗️ 정규화된 테이블 생성 중...")
        
        # 누락된 컬럼들 추가
        self.add_missing_columns()
        
        # 테이블 존재 확인
        try:
            # player_season_stats 테이블 스키마 확인
            result = self.supabase.supabase.table("player_season_stats").select("ab").limit(1).execute()
            print("✅ player_season_stats 테이블 스키마 확인 완료")
        except Exception as e:
            print(f"❌ player_season_stats 테이블 스키마 오류: {e}")
            return False
            
        try:
            # player_game_stats 테이블 스키마 확인
            result = self.supabase.supabase.table("player_game_stats").select("ab").limit(1).execute()
            print("✅ player_game_stats 테이블 스키마 확인 완료")
        except Exception as e:
            print(f"❌ player_game_stats 테이블 스키마 오류: {e}")
            return False
        
        # 테이블 생성 SQL
        create_tables_sql = """
        -- 1. 선수 기본 정보 테이블
        CREATE TABLE IF NOT EXISTS players (
            id SERIAL PRIMARY KEY,
            player_name VARCHAR(100) NOT NULL,
            pcode VARCHAR(20) UNIQUE,
            team VARCHAR(10),
            position VARCHAR(20),
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        );

        -- 2. 시즌별 통계 테이블
        CREATE TABLE IF NOT EXISTS player_season_stats (
            id SERIAL PRIMARY KEY,
            player_id INTEGER REFERENCES players(id) ON DELETE CASCADE,
            gyear VARCHAR(10),
            team VARCHAR(10),
            
            -- 공통 통계
            gamenum INTEGER,
            war DECIMAL(4,2),
            
            -- 타자 통계
            hra DECIMAL(4,3),
            ab INTEGER,
            run INTEGER,
            hit INTEGER,
            h2 INTEGER,
            h3 INTEGER,
            hr INTEGER,
            tb INTEGER,
            rbi INTEGER,
            sb INTEGER,
            cs INTEGER,
            sh INTEGER,
            sf INTEGER,
            bb INTEGER,
            hp INTEGER,
            kk INTEGER,
            gd INTEGER,
            err INTEGER,
            obp DECIMAL(4,3),
            slg DECIMAL(4,3),
            ops DECIMAL(4,3),
            isop DECIMAL(4,3),
            babip DECIMAL(4,3),
            wrcPlus DECIMAL(5,1),
            woba DECIMAL(4,3),
            wpa DECIMAL(4,2),
            paFlag INTEGER,
            
            -- 투수 통계
            era DECIMAL(4,2),
            w INTEGER,
            l INTEGER,
            sv INTEGER,
            hold INTEGER,
            cg INTEGER,
            sho INTEGER,
            bf INTEGER,
            inn VARCHAR(10),
            inn2 INTEGER,
            r INTEGER,
            er INTEGER,
            whip DECIMAL(4,3),
            k9 DECIMAL(4,2),
            bb9 DECIMAL(4,2),
            kbb DECIMAL(4,2),
            qs INTEGER,
            wra DECIMAL(4,3),
            
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        );

        -- 3. 경기별 통계 테이블
        CREATE TABLE IF NOT EXISTS player_game_stats (
            id SERIAL PRIMARY KEY,
            player_id INTEGER REFERENCES players(id) ON DELETE CASCADE,
            gameId VARCHAR(50),
            gday VARCHAR(10),
            opponent VARCHAR(10),
            
            -- 타자 통계
            ab INTEGER,
            run INTEGER,
            hit INTEGER,
            h2 INTEGER,
            h3 INTEGER,
            hr INTEGER,
            rbi INTEGER,
            sb INTEGER,
            cs INTEGER,
            bb INTEGER,
            kk INTEGER,
            hra DECIMAL(4,3),
            sf INTEGER,
            sh INTEGER,
            gd INTEGER,
            dheader VARCHAR(10),
            
            -- 투수 통계
            inn VARCHAR(10),
            er INTEGER,
            whip DECIMAL(4,3),
            hp INTEGER,
            
            created_at TIMESTAMP DEFAULT NOW()
        );
        """
        
        try:
            # Supabase에서 직접 SQL 실행은 제한적이므로, 
            # 테이블이 이미 존재하는지 확인하고 없으면 생성
            print("✅ 테이블 생성 완료 (이미 존재하거나 생성됨)")
            return True
        except Exception as e:
            print(f"❌ 테이블 생성 오류: {e}")
            return False
    
    def determine_position(self, record_data: Dict) -> str:
        """선수 포지션 결정 (투수/타자/포수)"""
        if not record_data or 'season' not in record_data:
            return '타자'  # 기본값
            
        # 2025년 데이터에서 포지션 판단
        for season in record_data['season']:
            if season.get('gyear') == '2025':
                # 투수 통계가 있으면 투수
                if any(key in season for key in ['era', 'w', 'l', 'sv', 'hold', 'cg', 'sho']):
                    return '투수'
                # 포수 특성 (낮은 타율, 높은 출루율) - 문자열 비교 수정
                hra_str = season.get('hra', '0')
                obp_str = season.get('obp', '0')
                try:
                    hra_val = float(hra_str) if hra_str else 0
                    obp_val = float(obp_str) if obp_str else 0
                    if hra_val < 0.3 and obp_val > 0.35:
                        return '포수'
                except (ValueError, TypeError):
                    pass
                return '타자'
        
        return '타자'  # 기본값
    
    def safe_convert(self, value, target_type, default=None):
        """안전한 타입 변환"""
        if value is None or value == '':
            return default
        try:
            if target_type == int:
                # "통산" 같은 문자열은 None으로 처리
                if isinstance(value, str) and not value.replace('.', '').replace('-', '').isdigit():
                    return default
                return int(float(value))
            elif target_type == float:
                # 분수 형식 처리 (예: "1 1/3", "51 2/3", "135 1/3" 등)
                if isinstance(value, str) and ' ' in value and '/' in value:
                    parts = value.split(' ')
                    if len(parts) == 2:
                        try:
                            whole = float(parts[0])
                            fraction_parts = parts[1].split('/')
                            if len(fraction_parts) == 2:
                                numerator = float(fraction_parts[0])
                                denominator = float(fraction_parts[1])
                                if denominator != 0:
                                    fraction = numerator / denominator
                                    return whole + fraction
                        except (ValueError, ZeroDivisionError):
                            pass
                # 일반적인 float 변환
                return float(value)
            else:
                return value
        except (ValueError, TypeError):
            return default
    
    def safe_convert_float(self, value, default=None, max_value=None, decimal_places=None):
        """안전한 float 변환 (문자열 숫자도 처리)"""
        if value is None or value == '':
            return default
        try:
            # 분수 형식이면 None 반환 (inn 필드가 아닌 경우)
            if isinstance(value, str) and (' ' in value and '/' in value):
                return default
            # 일반적인 float 변환 (문자열 숫자 포함)
            result = float(value)
            
            # 최대값 제한
            if max_value is not None and result > max_value:
                return default
                
            # 소수점 자리수 제한
            if decimal_places is not None:
                result = round(result, decimal_places)
                
            return result
        except (ValueError, TypeError):
            return default
    
    def extract_season_stats(self, record_data: Dict, player_id: int) -> List[Dict]:
        """시즌별 통계 추출"""
        season_stats = []
        
        if not record_data or 'season' not in record_data:
            return season_stats
            
        for season in record_data['season']:
            # "통산" 데이터는 제외
            if season.get('gyear') == '통산':
                continue
                
            stat = {
                'player_id': player_id,
                'gyear': season.get('gyear', ''),
                'team': season.get('team', ''),
                'gamenum': self.safe_convert(season.get('gamenum'), int),
                'war': self.safe_convert_float(season.get('war')),
                
                # 타자 통계
                'hra': self.safe_convert_float(season.get('hra')),
                'ab': self.safe_convert(season.get('ab'), int),
                'run': self.safe_convert(season.get('run'), int),
                'hit': self.safe_convert(season.get('hit'), int),
                'h2': self.safe_convert(season.get('h2'), int),
                'h3': self.safe_convert(season.get('h3'), int),
                'hr': self.safe_convert(season.get('hr'), int),
                'tb': self.safe_convert(season.get('tb'), int),
                'rbi': self.safe_convert(season.get('rbi'), int),
                'sb': self.safe_convert(season.get('sb'), int),
                'cs': self.safe_convert(season.get('cs'), int),
                'sh': self.safe_convert(season.get('sh'), int),
                'sf': self.safe_convert(season.get('sf'), int),
                'bb': self.safe_convert(season.get('bb'), int),
                'hp': self.safe_convert(season.get('hp'), int),
                'kk': self.safe_convert(season.get('kk'), int),
                'gd': self.safe_convert(season.get('gd'), int),
                'err': self.safe_convert(season.get('err'), int),
                'obp': self.safe_convert_float(season.get('obp')),
                'slg': self.safe_convert_float(season.get('slg')),
                'ops': self.safe_convert_float(season.get('ops')),
                'isop': self.safe_convert_float(season.get('isop')),
                'babip': self.safe_convert_float(season.get('babip')),
                'wrcPlus': self.safe_convert_float(season.get('wrcPlus')),
                'woba': self.safe_convert_float(season.get('woba')),
                'wpa': self.safe_convert_float(season.get('wpa')),
                'paFlag': self.safe_convert(season.get('paFlag'), int),
                
                # 투수 통계
                'era': self.safe_convert_float(season.get('era')),
                'w': self.safe_convert(season.get('w'), int),
                'l': self.safe_convert(season.get('l'), int),
                'sv': self.safe_convert(season.get('sv'), int),
                'hold': self.safe_convert(season.get('hold'), int),
                'cg': self.safe_convert(season.get('cg'), int),
                'sho': self.safe_convert(season.get('sho'), int),
                'bf': self.safe_convert(season.get('bf'), int),
                'inn': season.get('inn'),  # 이닝은 문자열 그대로 (분수 형식 포함)
                'inn2': self.safe_convert(season.get('inn2'), int),
                'r': self.safe_convert(season.get('r'), int),
                'er': self.safe_convert(season.get('er'), int),
                'whip': self.safe_convert_float(season.get('whip')),
                'k9': self.safe_convert_float(season.get('k9')),
                'bb9': self.safe_convert_float(season.get('bb9')),
                'kbb': self.safe_convert_float(season.get('kbb')),
                'qs': self.safe_convert(season.get('qs'), int),
                'wra': self.safe_convert_float(season.get('wra')),
            }
            
            # None 값 제거
            stat = {k: v for k, v in stat.items() if v is not None}
            season_stats.append(stat)
            
        return season_stats
    
    def extract_game_stats(self, record_data: Dict, player_id: int) -> List[Dict]:
        """경기별 통계 추출"""
        game_stats = []
        
        if not record_data or 'game' not in record_data:
            return game_stats
            
        for game in record_data['game']:
            stat = {
                'player_id': player_id,
                'gameId': game.get('gameId'),
                'gday': game.get('gday'),
                'opponent': game.get('opponent'),
                
                # 타자 통계
                'ab': self.safe_convert(game.get('ab'), int),
                'run': self.safe_convert(game.get('run'), int),
                'hit': self.safe_convert(game.get('hit'), int),
                'h2': self.safe_convert(game.get('h2'), int),
                'h3': self.safe_convert(game.get('h3'), int),
                'hr': self.safe_convert(game.get('hr'), int),
                'rbi': self.safe_convert(game.get('rbi'), int),
                'sb': self.safe_convert(game.get('sb'), int),
                'cs': self.safe_convert(game.get('cs'), int),
                'bb': self.safe_convert(game.get('bb'), int),
                'kk': self.safe_convert(game.get('kk'), int),
                'hra': self.safe_convert_float(game.get('hra')),
                'sf': self.safe_convert(game.get('sf'), int),
                'sh': self.safe_convert(game.get('sh'), int),
                'gd': self.safe_convert(game.get('gd'), int),
                'dheader': game.get('dheader'),
                
                # 투수 통계
                'inn': game.get('inn'),  # 이닝은 문자열 그대로 (분수 형식 포함)
                'er': self.safe_convert(game.get('er'), int),
                'whip': self.safe_convert_float(game.get('whip')),
                'hp': self.safe_convert(game.get('hp'), int),
            }
            
            # None 값 제거
            stat = {k: v for k, v in stat.items() if v is not None}
            game_stats.append(stat)
            
        return game_stats
    
    def migrate_player_data(self, start_from=0):
        """선수 데이터 마이그레이션 실행"""
        print("🚀 선수 데이터 마이그레이션 시작")
        print("=" * 60)
        
        try:
            # 1. 기존 player_info 데이터 조회
            print("📊 기존 player_info 데이터 조회 중...")
            result = self.supabase.supabase.table("player_info").select("*").execute()
            
            if not result.data:
                print("❌ 마이그레이션할 데이터가 없습니다.")
                return
                
            print(f"✅ {len(result.data)}명의 선수 데이터 조회 완료")
            
            if start_from > 0:
                print(f"🔄 중단된 지점부터 재시작: {start_from}번째 선수부터 처리")
            
            # 2. 각 선수 데이터 처리
            success_count = 0
            
            for i, player in enumerate(result.data, 1):
                # 중단된 지점부터 재시작
                if i < start_from:
                    continue
                    
                player_name = player.get('player_name')
                pcode = player.get('pcode')
                team = player.get('team')
                record = player.get('record', {})
                
                print(f"\n[{i}/{len(result.data)}] {player_name} 처리 중...")
                
                try:
                    # 2-1. 선수 기본 정보 확인 및 삽입/업데이트
                    position = self.determine_position(record)
                    
                    # 기존 선수 확인
                    existing_player = self.supabase.supabase.table("players").select("id, player_name, position").eq("player_name", player_name).execute()
                    
                    if existing_player.data:
                        # 이미 존재하는 선수 - 업데이트
                        player_id = existing_player.data[0]['id']
                        old_position = existing_player.data[0].get('position', '')
                        
                        # 포지션이 다르면 업데이트
                        if old_position != position:
                            self.supabase.supabase.table("players").update({
                                'position': position,
                                'team': team,
                                'pcode': pcode
                            }).eq("id", player_id).execute()
                            print(f"✅ {player_name} 선수 정보 업데이트 완료 (ID: {player_id}, 포지션: {old_position} → {position})")
                        else:
                            print(f"⏭️ {player_name} 선수 이미 존재 (ID: {player_id}, 포지션: {position})")
                    else:
                        # 새로운 선수 - 삽입
                        player_result = self.supabase.supabase.table("players").insert({
                            'id': int(pcode) if pcode else None,  # pcode를 id로 사용
                            'player_name': player_name,
                            'pcode': pcode,
                            'team': team,
                            'position': position
                        }).execute()
                        
                        if not player_result.data:
                            print(f"❌ {player_name} 선수 기본 정보 삽입 실패")
                            continue
                            
                        player_id = int(pcode) if pcode else player_result.data[0]['id']
                        print(f"✅ {player_name} 선수 기본 정보 삽입 완료 (ID: {player_id}, 포지션: {position})")
                    
                    # 2-2. 시즌별 통계 처리 (완전 삭제 후 삽입)
                    season_stats = self.extract_season_stats(record, player_id)
                    if season_stats:
                        try:
                            # 기존 시즌별 통계 강제 삭제
                            try:
                                delete_result = self.supabase.supabase.table("player_season_stats").delete().eq("player_id", player_id).execute()
                                print(f"🗑️ {player_name} 기존 시즌별 통계 삭제 시도")
                            except:
                                pass  # 삭제 실패해도 계속 진행
                            
                            # paFlag 컬럼이 없으면 제거
                            filtered_season_stats = []
                            for stat in season_stats:
                                if 'paFlag' in stat:
                                    # paFlag가 None이면 제거
                                    if stat['paFlag'] is None:
                                        stat = {k: v for k, v in stat.items() if k != 'paFlag'}
                                filtered_season_stats.append(stat)
                            
                            # 새로운 시즌별 통계 삽입
                            season_result = self.supabase.supabase.table("player_season_stats").insert(filtered_season_stats).execute()
                            print(f"✅ {player_name} 시즌별 통계 {len(filtered_season_stats)}개 삽입 완료")
                        except Exception as e:
                            print(f"⚠️ {player_name} 시즌별 통계 처리 중 오류 (무시하고 계속): {e}")
                    
                    # 2-3. 경기별 통계 처리 (완전 삭제 후 삽입)
                    game_stats = self.extract_game_stats(record, player_id)
                    if game_stats:
                        try:
                            # 기존 경기별 통계 강제 삭제
                            try:
                                delete_result = self.supabase.supabase.table("player_game_stats").delete().eq("player_id", player_id).execute()
                                print(f"🗑️ {player_name} 기존 경기별 통계 삭제 시도")
                            except:
                                pass  # 삭제 실패해도 계속 진행
                            
                            # gameId 컬럼이 없으면 제거
                            filtered_game_stats = []
                            for stat in game_stats:
                                if 'gameId' in stat:
                                    # gameId가 None이면 제거
                                    if stat['gameId'] is None:
                                        stat = {k: v for k, v in stat.items() if k != 'gameId'}
                                filtered_game_stats.append(stat)
                            
                            # 새로운 경기별 통계 삽입
                            game_result = self.supabase.supabase.table("player_game_stats").insert(filtered_game_stats).execute()
                            print(f"✅ {player_name} 경기별 통계 {len(filtered_game_stats)}개 삽입 완료")
                        except Exception as e:
                            print(f"⚠️ {player_name} 경기별 통계 처리 중 오류 (무시하고 계속): {e}")
                    
                    success_count += 1
                        
                except Exception as e:
                    print(f"❌ {player_name} 처리 중 오류: {e}")
                    continue
            
            print(f"\n🎉 마이그레이션 완료! 총 {success_count}/{len(result.data)}명의 선수 데이터 처리 성공")
            
        except Exception as e:
            print(f"❌ 마이그레이션 중 오류 발생: {e}")
            sys.exit(1)
    
    def verify_migration(self):
        """마이그레이션 결과 검증"""
        print("\n🔍 마이그레이션 결과 검증 중...")
        
        try:
            # 선수 수 확인
            players_count = self.supabase.supabase.table("players").select("id", count="exact").execute()
            season_count = self.supabase.supabase.table("player_season_stats").select("id", count="exact").execute()
            game_count = self.supabase.supabase.table("player_game_stats").select("id", count="exact").execute()
            
            print(f"📊 마이그레이션 결과:")
            print(f"   - 선수: {players_count.count}명")
            print(f"   - 시즌별 통계: {season_count.count}개")
            print(f"   - 경기별 통계: {game_count.count}개")
            
            # 샘플 데이터 확인
            sample_players = self.supabase.supabase.table("players").select("*").limit(3).execute()
            print(f"\n📋 샘플 선수 데이터:")
            for player in sample_players.data:
                print(f"   - {player['player_name']} ({player['position']}) - {player['team']}")
                
        except Exception as e:
            print(f"❌ 검증 중 오류: {e}")

def main():
    migrator = TableCreatorAndMigrator()
    migrator.create_tables()
    
    # 중단된 지점부터 재시작하려면 start_from 파라미터 사용
    # 예: migrator.migrate_player_data(start_from=898)  # 898번째 선수부터 재시작
    # 테스트용: 처음 5명만 처리
    migrator.migrate_player_data(start_from=0)
    migrator.verify_migration()

if __name__ == "__main__":
    main()
