DROP VIEW IF EXISTS ShotGenSup5v4;
CREATE VIEW ShotGenSup5v4 AS
WITH PowerPlayShots AS (SELECT
eventOwnerTeamId as TeamId,
gameId,
season,
(eventOwnerTeamId = homeTeamId) AS isHomeTeam,
homeScoreAdj,
awayScoreAdj
FROM shot_events 
WHERE  ((situationCode='1451' AND isHomeTeam = 1) OR (situationCode='1541' AND isHomeTeam = 0)))

,TeamGamePowerPlayShotCounts AS
 (SELECT TeamId, gameId, season, t.fullName, 
         MAX(isHomeTeam) AS isHomeTeam, 
         COUNT() AS TotalPowerPlayShots, 
         MAX(homeScoreAdj) as HomeFinal, 
         MAX(awayScoreAdj) AS AwayFinal
FROM PowerPlayShots
LEFT JOIN teams as t ON TeamId=t.id
GROUP BY TeamId, gameId
ORDER BY gameId DESC)

SELECT
g.id,
g.season,
g.teamId,
g.oppTeamId,
g.isHomeTeam,
team.TotalPowerPlayShots AS TotalPowerPlayShotsFor,
opp.TotalPowerPlayShots AS TotalPenaltyKillShotsAgainst,
team.TotalPowerPlayShots * 1.0 / (toi.timeOnIce5v4 / 60.0 / 60.0) AS PowerPlayShotsForPerHour,
opp.TotalPowerPlayShots * 1.0 / (toi.timeOnIce4v5 / 60.0 / 60.0) AS PenaltyKillShotsAgainstPerHour,
toi.timeOnIce5v4 AS timeOnIce5v4,
toi.timeOnIce4v5 AS timeOnIce4v5
FROM TeamGames AS g
LEFT JOIN pk_pp_toi AS toi ON g.id = toi.gameId AND g.teamId = toi.teamId
LEFT JOIN TeamGamePowerPlayShotCounts AS team ON g.id = team.gameId AND g.teamId = team.teamId
LEFT JOIN TeamGamePowerPlayShotCounts AS opp ON g.id = opp.gameId AND g.oppTeamId = opp.teamId
WHERE g.id >= 2010020000
ORDER BY g.id DESC
