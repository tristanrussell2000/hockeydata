DROP VIEW IF EXISTS GoalieSavePct;
CREATE VIEW GoalieSavePct AS
WITH goalie_avgs AS (SELECT 
`details.goalieInNetId` AS goalieId,
SUM(
    CASE WHEN typeDescKey='goal' THEN 1 ELSE 0 END
  ) AS goals_against,
 COUNT(*) AS shots_against
 FROM event_shots 
 WHERE isSOG = 1 AND situationCode=1551 AND gameType=2 AND goalieId  IS NOT NULL
GROUP BY goalieId)

SELECT goalie_avgs.*,
1 - ((80 + CAST(goals_against AS REAL)) / (1000 + CAST(shots_against AS REAL))) AS diluted_save_pct,
1 - (CAST(goals_against AS REAL) / CAST(shots_against AS REAL)) AS raw_save_pct
FROM goalie_avgs