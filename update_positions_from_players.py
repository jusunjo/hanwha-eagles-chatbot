#!/usr/bin/env python3
"""
players 테이블의 포지션 정보를 player_season_stats, player_game_stats에 업데이트하는 스크립트
"""

import sys
from data.supabase_client import SupabaseManager

class PositionUpdaterFromPlayers:
    def __init__(self):
        self.supabase = SupabaseManager()
        
    def get_players_position_mapping(self):
        """players 테이블에서 pcode와 position 매핑 정보 조회"""
        try:
            result = self.supabase.supabase.table("players").select("pcode, position, player_name").execute()
            
            if not result.data:
                print("❌ players 테이블에 데이터가 없습니다.")
                return {}
            
            # pcode -> position 매핑 생성
            position_mapping = {}
            for player in result.data:
                pcode = player.get('pcode')
                position = player.get('position')
                player_name = player.get('player_name')
                
                if pcode and position:
                    position_mapping[str(pcode)] = {
                        'position': position,
                        'player_name': player_name
                    }
            
            print(f"📊 players 테이블에서 {len(position_mapping)}명의 선수 포지션 정보 조회 완료")
            return position_mapping
            
        except Exception as e:
            print(f"❌ players 테이블 조회 중 오류: {e}")
            return {}
    
    def update_player_season_positions(self, position_mapping):
        """player_season_stats 테이블의 포지션 업데이트"""
        print("\n🔄 player_season_stats 테이블 포지션 업데이트 시작...")
        
        try:
            # 모든 시즌별 데이터 조회
            result = self.supabase.supabase.table("player_season_stats").select("id, player_id, player_name, position").execute()
            
            if not result.data:
                print("❌ 업데이트할 데이터가 없습니다.")
                return
            
            print(f"📊 총 {len(result.data)}개의 레코드 처리 중...")
            
            updated_count = 0
            not_found_count = 0
            position_counts = {}
            
            for record in result.data:
                player_id = str(record.get('player_id', ''))
                current_position = record.get('position')
                player_name = record.get('player_name', '')
                
                # players 테이블에서 포지션 조회
                if player_id in position_mapping:
                    new_position = position_mapping[player_id]['position']
                    expected_name = position_mapping[player_id]['player_name']
                    
                    # 포지션이 다르면 업데이트
                    if current_position != new_position:
                        try:
                            self.supabase.supabase.table("player_season_stats").update({
                                'position': new_position
                            }).eq("id", record['id']).execute()
                            
                            print(f"✅ {player_name} (ID: {player_id}): {current_position or 'NULL'} → {new_position}")
                            updated_count += 1
                        except Exception as e:
                            print(f"❌ {player_name} 업데이트 실패: {e}")
                    
                    # 포지션 카운트
                    position_counts[new_position] = position_counts.get(new_position, 0) + 1
                else:
                    print(f"⚠️ {player_name} (ID: {player_id}): players 테이블에서 찾을 수 없음")
                    not_found_count += 1
                    # 기존 포지션 유지
                    if current_position:
                        position_counts[current_position] = position_counts.get(current_position, 0) + 1
                    else:
                        position_counts['NULL'] = position_counts.get('NULL', 0) + 1
            
            print(f"\n📊 player_season_stats 업데이트 완료:")
            print(f"   - 업데이트된 레코드: {updated_count}개")
            print(f"   - 찾을 수 없는 선수: {not_found_count}개")
            print(f"   - 포지션 분포:")
            for pos, count in sorted(position_counts.items()):
                print(f"     {pos or 'NULL'}: {count}명")
                
        except Exception as e:
            print(f"❌ player_season_stats 업데이트 중 오류: {e}")
    
    def update_player_game_positions(self, position_mapping):
        """player_game_stats 테이블의 포지션 업데이트"""
        print("\n🔄 player_game_stats 테이블 포지션 업데이트 시작...")
        
        try:
            # 모든 경기별 데이터 조회
            result = self.supabase.supabase.table("player_game_stats").select("id, player_id, player_name, position").execute()
            
            if not result.data:
                print("❌ 업데이트할 데이터가 없습니다.")
                return
            
            print(f"📊 총 {len(result.data)}개의 레코드 처리 중...")
            
            updated_count = 0
            not_found_count = 0
            position_counts = {}
            
            for record in result.data:
                player_id = str(record.get('player_id', ''))
                current_position = record.get('position')
                player_name = record.get('player_name', '')
                
                # players 테이블에서 포지션 조회
                if player_id in position_mapping:
                    new_position = position_mapping[player_id]['position']
                    expected_name = position_mapping[player_id]['player_name']
                    
                    # 포지션이 다르면 업데이트
                    if current_position != new_position:
                        try:
                            self.supabase.supabase.table("player_game_stats").update({
                                'position': new_position
                            }).eq("id", record['id']).execute()
                            
                            print(f"✅ {player_name} (ID: {player_id}): {current_position or 'NULL'} → {new_position}")
                            updated_count += 1
                        except Exception as e:
                            print(f"❌ {player_name} 업데이트 실패: {e}")
                    
                    # 포지션 카운트
                    position_counts[new_position] = position_counts.get(new_position, 0) + 1
                else:
                    print(f"⚠️ {player_name} (ID: {player_id}): players 테이블에서 찾을 수 없음")
                    not_found_count += 1
                    # 기존 포지션 유지
                    if current_position:
                        position_counts[current_position] = position_counts.get(current_position, 0) + 1
                    else:
                        position_counts['NULL'] = position_counts.get('NULL', 0) + 1
            
            print(f"\n📊 player_game_stats 업데이트 완료:")
            print(f"   - 업데이트된 레코드: {updated_count}개")
            print(f"   - 찾을 수 없는 선수: {not_found_count}개")
            print(f"   - 포지션 분포:")
            for pos, count in sorted(position_counts.items()):
                print(f"     {pos or 'NULL'}: {count}명")
                
        except Exception as e:
            print(f"❌ player_game_stats 업데이트 중 오류: {e}")
    
    def verify_positions(self):
        """포지션 업데이트 결과 검증"""
        print("\n🔍 포지션 업데이트 결과 검증...")
        
        try:
            # player_season_stats 포지션 분포 확인
            season_result = self.supabase.supabase.table("player_season_stats").select("position").execute()
            season_positions = {}
            for record in season_result.data:
                pos = record.get('position', 'NULL')
                season_positions[pos] = season_positions.get(pos, 0) + 1
            
            print("📊 player_season_stats 포지션 분포:")
            for pos, count in sorted(season_positions.items(), key=lambda x: (x[0] is None, x[0])):
                print(f"   {pos or 'NULL'}: {count}명")
            
            # player_game_stats 포지션 분포 확인
            game_result = self.supabase.supabase.table("player_game_stats").select("position").execute()
            game_positions = {}
            for record in game_result.data:
                pos = record.get('position', 'NULL')
                game_positions[pos] = game_positions.get(pos, 0) + 1
            
            print("\n📊 player_game_stats 포지션 분포:")
            for pos, count in sorted(game_positions.items(), key=lambda x: (x[0] is None, x[0])):
                print(f"   {pos or 'NULL'}: {count}명")
                
        except Exception as e:
            print(f"❌ 검증 중 오류: {e}")
    
    def show_sample_mappings(self, position_mapping, limit=10):
        """샘플 매핑 정보 표시"""
        print(f"\n📋 샘플 포지션 매핑 (상위 {limit}개):")
        count = 0
        for pcode, info in position_mapping.items():
            if count >= limit:
                break
            print(f"   pcode {pcode}: {info['player_name']} → {info['position']}")
            count += 1

def main():
    updater = PositionUpdaterFromPlayers()
    
    print("🚀 players 테이블 기반 포지션 업데이트 시작")
    print("=" * 60)
    
    # players 테이블에서 포지션 매핑 정보 조회
    position_mapping = updater.get_players_position_mapping()
    
    if not position_mapping:
        print("❌ 포지션 매핑 정보를 가져올 수 없습니다.")
        return
    
    # 샘플 매핑 정보 표시
    updater.show_sample_mappings(position_mapping)
    
    # player_season_stats 업데이트
    updater.update_player_season_positions(position_mapping)
    
    # player_game_stats 업데이트
    updater.update_player_game_positions(position_mapping)
    
    # 결과 검증
    updater.verify_positions()
    
    print("\n🎉 포지션 업데이트 완료!")

if __name__ == "__main__":
    main()
