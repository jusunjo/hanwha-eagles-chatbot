#!/usr/bin/env python3
"""
선수 포지션 자동 분류 및 업데이트 스크립트
"""

import sys
from data.supabase_client import SupabaseManager

class PositionUpdater:
    def __init__(self):
        self.supabase = SupabaseManager()
        
    def determine_position(self, player_data):
        """선수 데이터를 바탕으로 포지션 결정"""
        # 투수 통계가 있으면 투수
        if any(key in player_data for key in ['era', 'w', 'l', 'sv', 'hold', 'cg', 'sho', 'bf', 'inn', 'er', 'whip', 'k9', 'bb9', 'kbb', 'qs', 'wra']):
            # 투수 통계 값이 NULL이 아닌 경우만 투수로 분류
            if (player_data.get('era') is not None or 
                player_data.get('w') is not None or 
                player_data.get('l') is not None or 
                player_data.get('sv') is not None or 
                player_data.get('hold') is not None):
                return '투수'
        
        # 타자 통계가 있으면 타자
        if any(key in player_data for key in ['hra', 'hr', 'h2', 'h3', 'rbi', 'ab', 'obp', 'slg', 'ops', 'isop', 'babip', 'wrcplus', 'woba', 'wpa']):
            # 타자 통계 값이 NULL이 아닌 경우만 타자로 분류
            if (player_data.get('hra') is not None or 
                player_data.get('hr') is not None or 
                player_data.get('rbi') is not None or 
                player_data.get('ab') is not None):
                return '타자'
        
        # 포수 특성 판단 (낮은 타율, 높은 출루율)
        hra = player_data.get('hra')
        obp = player_data.get('obp')
        if hra is not None and obp is not None:
            try:
                hra_val = float(hra) if hra else 0
                obp_val = float(obp) if obp else 0
                if hra_val < 0.3 and obp_val > 0.35:
                    return '포수'
            except (ValueError, TypeError):
                pass
        
        # 기본값은 타자
        return '타자'
    
    def update_player_season_positions(self):
        """player_season_stats 테이블의 포지션 업데이트"""
        print("🔄 player_season_stats 테이블 포지션 업데이트 시작...")
        
        try:
            # 모든 선수 데이터 조회
            result = self.supabase.supabase.table("player_season_stats").select("*").execute()
            
            if not result.data:
                print("❌ 업데이트할 데이터가 없습니다.")
                return
            
            print(f"📊 총 {len(result.data)}개의 레코드 처리 중...")
            
            updated_count = 0
            position_counts = {'투수': 0, '타자': 0, '포수': 0}
            
            for record in result.data:
                player_name = record.get('player_name', '')
                current_position = record.get('position')
                
                # 포지션 결정
                new_position = self.determine_position(record)
                
                # 포지션이 다르면 업데이트
                if current_position != new_position:
                    try:
                        self.supabase.supabase.table("player_season_stats").update({
                            'position': new_position
                        }).eq("id", record['id']).execute()
                        
                        print(f"✅ {player_name}: {current_position or 'NULL'} → {new_position}")
                        updated_count += 1
                    except Exception as e:
                        print(f"❌ {player_name} 업데이트 실패: {e}")
                
                position_counts[new_position] += 1
            
            print(f"\n📊 업데이트 완료:")
            print(f"   - 업데이트된 레코드: {updated_count}개")
            print(f"   - 포지션 분포:")
            for pos, count in position_counts.items():
                print(f"     {pos}: {count}명")
                
        except Exception as e:
            print(f"❌ player_season_stats 업데이트 중 오류: {e}")
    
    def update_player_game_positions(self):
        """player_game_stats 테이블의 포지션 업데이트"""
        print("\n🔄 player_game_stats 테이블 포지션 업데이트 시작...")
        
        try:
            # 모든 경기별 데이터 조회
            result = self.supabase.supabase.table("player_game_stats").select("*").execute()
            
            if not result.data:
                print("❌ 업데이트할 데이터가 없습니다.")
                return
            
            print(f"📊 총 {len(result.data)}개의 레코드 처리 중...")
            
            updated_count = 0
            position_counts = {'투수': 0, '타자': 0, '포수': 0}
            
            for record in result.data:
                player_name = record.get('player_name', '')
                current_position = record.get('position')
                
                # 포지션 결정
                new_position = self.determine_position(record)
                
                # 포지션이 다르면 업데이트
                if current_position != new_position:
                    try:
                        self.supabase.supabase.table("player_game_stats").update({
                            'position': new_position
                        }).eq("id", record['id']).execute()
                        
                        print(f"✅ {player_name}: {current_position or 'NULL'} → {new_position}")
                        updated_count += 1
                    except Exception as e:
                        print(f"❌ {player_name} 업데이트 실패: {e}")
                
                position_counts[new_position] += 1
            
            print(f"\n📊 업데이트 완료:")
            print(f"   - 업데이트된 레코드: {updated_count}개")
            print(f"   - 포지션 분포:")
            for pos, count in position_counts.items():
                print(f"     {pos}: {count}명")
                
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

def main():
    updater = PositionUpdater()
    
    print("🚀 선수 포지션 자동 분류 및 업데이트 시작")
    print("=" * 60)
    
    # player_season_stats 업데이트
    updater.update_player_season_positions()
    
    # player_game_stats 업데이트
    updater.update_player_game_positions()
    
    # 결과 검증
    updater.verify_positions()
    
    print("\n🎉 포지션 업데이트 완료!")

if __name__ == "__main__":
    main()
