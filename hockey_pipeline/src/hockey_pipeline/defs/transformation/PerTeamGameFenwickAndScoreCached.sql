CREATE TABLE IF NOT EXISTS PerTeamGameAdjustedFenwick(
    TeamId INTEGER,
    GameId INTEGER,
    GameType INTEGER,
    Season INTEGER,
    TeamFullName TEXT,
    IsHomeTeam INTEGER,
    AdjustedFenwick REAL,
    RawFenwick INTEGER,
    HomeFinal INTEGER,
    AwayFinal INTEGER,
    PRIMARY KEY (GameId, TeamId) 
) WITHOUT ROWID ;

WITH FenwickCounts AS (SELECT
eventOwnerTeamId as TeamId,
gameId,
season,
(eventOwnerTeamId = homeTeamId) AS isHomeTeam,
MIN(MAX(homeScoreAdj - awayScoreAdj, -3), 3) as HomeLead,
((coeff.HomeCoeff * isHomeTeam) + (coeff.AwayCoeff * (1- isHomeTeam))) AS shot_value,
homeScoreAdj,
awayScoreAdj,
gameType
 FROM shot_events 
 LEFT JOIN main.AdjustedFenwick as coeff ON HomeLead = coeff.HomeLeadBinned
WHERE  typeDescKey != 'blocked-shot'  AND situationCode='1551'
AND gameId > COALESCE((SELECT MAX(GameId) FROM PerTeamGameAdjustedFenwick), 0))

INSERT INTO PerTeamGameAdjustedFenwick (TeamId, GameId, GameType, Season, TeamFullName, IsHomeTeam, AdjustedFenwick, RawFenwick, HomeFinal, AwayFinal)
SELECT TeamId, gameId, gameType, season, t.fullName,
         MAX(isHomeTeam) AS isHomeTeam, 
         SUM(shot_value) AS AdjustedFenwick, 
         COUNT() AS RawFenwick, 
         MAX(homeScoreAdj) as HomeFinal, 
         MAX(awayScoreAdj) AS AwayFinal
FROM FenwickCounts
LEFT JOIN teams as t ON TeamId=t.id
GROUP BY TeamId, gameId
ORDER BY gameId ASC;