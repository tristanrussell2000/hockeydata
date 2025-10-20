DROP VIEW IF EXISTS TeamGames;
CREATE VIEW TeamGames AS
SELECT id,
       homeTeamId as teamId,
       visitingTeamId as oppTeamId,
       1 as isHomeTeam,
       season,
       gameDate,
       gameNumber,
       gameScheduleStateId,
       gameStateId, gameType,
       homeScore, visitingScore,
       period
FROM games
WHERE gameType IN (2, 3)
UNION ALL
SELECT id,
       visitingTeamId as teamId,
       homeTeamId as oppTeamId,
       0 as isHomeTeam,
       season,
       gameDate,
       gameNumber,
       gameScheduleStateId,
       gameStateId, gameType,
       homeScore, visitingScore,
       period
FROM games
WHERE gameType IN (2, 3)
