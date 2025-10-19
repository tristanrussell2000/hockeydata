DROP VIEW IF EXISTS TeamGames;
CREATE VIEW TeamGames AS
SELECT id,
       homeTeamId as teamId,
       1 as isHomeTeam,
       season,
       gameDate,
       gameNumber,
       gameScheduleStateId,
       gameStateId, gameType,
       homeScore, visitingScore,
       period
FROM games
UNION ALL
SELECT id,
       visitingTeamId as teamId,
       0 as isHomeTeam,
       season,
       gameDate,
       gameNumber,
       gameScheduleStateId,
       gameStateId, gameType,
       homeScore, visitingScore,
       period
FROM games