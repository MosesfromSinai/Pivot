-- Shared Part 2 tables. Run with:
-- mysql -h 127.0.0.1 -P 3307 -u root < part2_spark/schema.sql
CREATE DATABASE IF NOT EXISTS pivot;
USE pivot;

CREATE TABLE IF NOT EXISTS injury_rates (
    group_type        VARCHAR(32) NOT NULL,
    group_value       VARCHAR(64) NOT NULL,
    field_type        VARCHAR(16) NOT NULL,
    plays             INT         NOT NULL,
    injuries          INT         NOT NULL,
    injuries_per_1000 DOUBLE,
    PRIMARY KEY (group_type, group_value, field_type)
);

CREATE TABLE IF NOT EXISTS play_features (
    play_key        VARCHAR(32) NOT NULL PRIMARY KEY,
    player_key      INT         NOT NULL,
    game_id         VARCHAR(16) NOT NULL,
    field_type      VARCHAR(16) NOT NULL,
    roster_position VARCHAR(32),
    duration_s      DOUBLE,
    distance_yd     DOUBLE,
    max_speed       DOUBLE,
    avg_speed       DOUBLE,
    max_accel       DOUBLE,
    sharp_cuts      INT,
    is_injury       BOOLEAN     NOT NULL DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS movement_summary (
    field_type      VARCHAR(16) NOT NULL,
    roster_position VARCHAR(32) NOT NULL,
    plays           INT         NOT NULL,
    avg_max_speed   DOUBLE,
    avg_max_accel   DOUBLE,
    avg_sharp_cuts  DOUBLE,
    PRIMARY KEY (field_type, roster_position)
);
