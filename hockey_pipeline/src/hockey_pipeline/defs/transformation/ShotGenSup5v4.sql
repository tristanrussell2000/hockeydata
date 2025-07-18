DROP VIEW IF EXISTS ShotGenSup5v4;
CREATE VIEW ShotGenSup5v4 AS
WITH PowerPlayShots AS (SELECT
eventOwnerTeamId as TeamId,
gameId,
season,
(eventOwnerTeamId = homeTeamId) AS isHomeTeam,
homeScoreAdj,
awayScoreAdj,
gameType
FROM shot_events 
WHERE  ((situationCode='1451' AND isHomeTeam = 1) OR (situationCode='1541' AND isHomeTeam = 0)) AND gameType=2)

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
toi.gameId,
g.season,
toi.teamId,
toi.homeRoad,
CASE WHEN toi.homeRoad = 'H'
THEN g.visitingTeamId
ELSE g.homeTeamId
END AS opponentId,
team.TotalPowerPlayShots AS TotalPowerPlayShotsFor,
opp.TotalPowerPlayShots AS TotalPenaltyKillShotsAgainst,
team.TotalPowerPlayShots * 1.0 / (toi.timeOnIce5v4 / 60.0 / 60.0) AS PowerPlayShotsForPerHour,
opp.TotalPowerPlayShots * 1.0 / (toi.timeOnIce4v5 / 60.0 / 60.0) AS PenaltyKillShotsAgainstPerHour,
toi.timeOnIce5v4 AS timeOnIce5v4,
toi.timeOnIce4v5 AS timeOnIce4v5
FROM pk_pp_toi  AS toi
LEFT JOIN games AS g ON toi.gameId = g.id
LEFT JOIN TeamGamePowerPlayShotCounts AS team ON toi.gameId = team.gameId AND toi.teamId = team.teamId
LEFT JOIN TeamGamePowerPlayShotCounts AS opp ON toi.gameId = opp.gameId AND opponentId = opp.teamId
WHERE g.gameType = 2
ORDER BY toi.gameId DESC
