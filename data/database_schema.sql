-- Supabase 대시보드의 SQL Editor에서 실행하세요

-- 1. 선수 데이터 테이블
CREATE TABLE IF NOT EXISTS players (
    id SERIAL PRIMARY KEY,
    player_name VARCHAR(100) NOT NULL UNIQUE,
    record JSONB,
    chart JSONB,
    vs_team JSONB,
    basic_record JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 2. 경기 일정 테이블
CREATE TABLE IF NOT EXISTS game_schedule (
    id SERIAL PRIMARY KEY,
    game_id VARCHAR(50) UNIQUE,
    date VARCHAR(20) NOT NULL,
    home_team VARCHAR(50) NOT NULL,
    away_team VARCHAR(50) NOT NULL,
    home_team_code VARCHAR(10) NOT NULL,
    away_team_code VARCHAR(10) NOT NULL,
    stadium VARCHAR(100),
    time VARCHAR(20),
    game_data JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);


-- 3. 경기 결과 테이블 (상대전적 분석용)
CREATE TABLE IF NOT EXISTS game_result (
    id SERIAL PRIMARY KEY,
    game_id VARCHAR(50) UNIQUE,
    game_date DATE NOT NULL,
    home_team VARCHAR(50) NOT NULL,
    away_team VARCHAR(50) NOT NULL,
    home_team_code VARCHAR(10) NOT NULL,
    away_team_code VARCHAR(10) NOT NULL,
    home_score INTEGER NOT NULL,
    away_score INTEGER NOT NULL,
    winner VARCHAR(50) NOT NULL, -- 'HOME' 또는 'AWAY'
    stadium VARCHAR(100),
    season VARCHAR(10) NOT NULL, -- '2025'
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 4. 선수 매핑 테이블 (기존 player_mapping.json 대체)
CREATE TABLE IF NOT EXISTS player_mapping (
    id SERIAL PRIMARY KEY,
    player_name VARCHAR(100) NOT NULL UNIQUE,
    player_id VARCHAR(50) NOT NULL,
    position VARCHAR(20),
    team VARCHAR(50),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 5. 인덱스 생성
CREATE INDEX IF NOT EXISTS idx_players_name ON players(player_name);
CREATE INDEX IF NOT EXISTS idx_schedule_date ON game_schedule(date);
CREATE INDEX IF NOT EXISTS idx_schedule_game_id ON game_schedule(game_id);
CREATE INDEX IF NOT EXISTS idx_game_result_date ON game_result(game_date);
CREATE INDEX IF NOT EXISTS idx_game_result_teams ON game_result(home_team, away_team);
CREATE INDEX IF NOT EXISTS idx_game_result_season ON game_result(season);
CREATE INDEX IF NOT EXISTS idx_player_mapping_name ON player_mapping(player_name);
CREATE INDEX IF NOT EXISTS idx_player_mapping_id ON player_mapping(player_id);

-- 6. RLS (Row Level Security) 정책 설정
ALTER TABLE players ENABLE ROW LEVEL SECURITY;
ALTER TABLE game_schedule ENABLE ROW LEVEL SECURITY;
ALTER TABLE game_result ENABLE ROW LEVEL SECURITY;
ALTER TABLE player_mapping ENABLE ROW LEVEL SECURITY;

-- 모든 사용자가 읽기 가능하도록 설정
CREATE POLICY "Allow read access for all users" ON players FOR SELECT USING (true);
CREATE POLICY "Allow read access for all users" ON game_schedule FOR SELECT USING (true);
CREATE POLICY "Allow read access for all users" ON game_result FOR SELECT USING (true);
CREATE POLICY "Allow read access for all users" ON player_mapping FOR SELECT USING (true);

-- 모든 사용자가 쓰기 가능하도록 설정 (개발 환경용)
CREATE POLICY "Allow insert access for all users" ON players FOR INSERT WITH CHECK (true);
CREATE POLICY "Allow update access for all users" ON players FOR UPDATE USING (true);
CREATE POLICY "Allow insert access for all users" ON game_schedule FOR INSERT WITH CHECK (true);
CREATE POLICY "Allow update access for all users" ON game_schedule FOR UPDATE USING (true);
CREATE POLICY "Allow insert access for all users" ON game_result FOR INSERT WITH CHECK (true);
CREATE POLICY "Allow update access for all users" ON game_result FOR UPDATE USING (true);
CREATE POLICY "Allow insert access for all users" ON player_mapping FOR INSERT WITH CHECK (true);
CREATE POLICY "Allow update access for all users" ON player_mapping FOR UPDATE USING (true);

-- 7. 업데이트 시간 자동 갱신을 위한 함수
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- 8. 트리거 생성
CREATE TRIGGER update_players_updated_at BEFORE UPDATE ON players
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_game_schedule_updated_at BEFORE UPDATE ON game_schedule
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_game_result_updated_at BEFORE UPDATE ON game_result
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_player_mapping_updated_at BEFORE UPDATE ON player_mapping
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
