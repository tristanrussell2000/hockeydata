ANALYZE;
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
) WITHOUT ROWID;
DELETE FROM PerTeamGameAdjustedFenwick;
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
 LEFT JOIN AdjustedFenwick as coeff ON HomeLead = coeff.HomeLeadBinned
WHERE  typeDescKey != 'blocked-shot'  AND situationCode='1551')

, PerTeamGameAdjustedFenwickView AS (SELECT TeamId, gameId, gameType, season, t.fullName,
         MAX(isHomeTeam) AS isHomeTeam, 
         SUM(shot_value) AS AdjustedFenwick, 
         COUNT() AS RawFenwick, 
         MAX(homeScoreAdj) as HomeFinal, 
         MAX(awayScoreAdj) AS AwayFinal
FROM FenwickCounts
LEFT JOIN teams as t ON TeamId=t.id
GROUP BY TeamId, gameId
ORDER BY gameId ASC)

INSERT INTO PerTeamGameAdjustedFenwick (TeamId, GameId, GameType, Season, TeamFullName, IsHomeTeam, AdjustedFenwick, RawFenwick, HomeFinal, AwayFinal)
SELECT
    TeamGames.teamId,
    TeamGames.id,
    TeamGames.gameType,
    TeamGames.season,
    PerTeamGameAdjustedFenwickView.fullName,
    TeamGames.isHomeTeam,
    PerTeamGameAdjustedFenwickView.AdjustedFenwick,
    PerTeamGameAdjustedFenwickView.RawFenwick,
    PerTeamGameAdjustedFenwickView.HomeFinal,
    PerTeamGameAdjustedFenwickView.AwayFinal
FROM TeamGames
LEFT JOIN PerTeamGameAdjustedFenwickView ON TeamGames.id = PerTeamGameAdjustedFenwickView.gameId AND TeamGames.teamId = PerTeamGameAdjustedFenwickView.TeamId
WHERE TeamGames.id >= 2010020000