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
ORDER BY gameId ASC)

,TeamGameShotCountsWithOpponent AS 
(SELECT 
Team.gameId, 
Team.season, 
Team.TeamId AS TeamId, 
Team.fullName AS TeamName, 
Team.TotalPowerPlayShots AS TotalPowerPlayShotsFor,
 Team.isHomeTeam, 
Opp.TeamId AS OpponentId, 
Opp.fullName AS OpponentName, 
Opp.TotalPowerPlayShots AS TotalPenaltyKillShotsAgainst, 
(Team.isHomeTeam * Team.HomeFinal + (1- Team.isHomeTeam) * Team.AwayFinal) AS ScoreFor,
(Team.isHomeTeam * Team.AwayFinal + (1- Team.isHomeTeam) * Team.HomeFinal) AS ScoreAgainst
FROM TeamGamePowerPlayShotCounts AS Team
LEFT JOIN TeamGamePowerPlayShotCounts AS Opp
ON Team.gameId=Opp.gameId AND Team.TeamId != Opp.TeamId)

SELECT 
tg.*, 
tg.TotalPowerPlayShotsFor * 1.0 / (toi.timeOnIce5v4 / 60.0 / 60.0) AS PowerPlayShotsForPerHour,
tg.TotalPenaltyKillShotsAgainst * 1.0 / (toi.timeOnIce4v5 / 60.0 / 60.0) AS PenaltyKillShotsAgainstPerHour,
toi.timeOnIce5v4 AS timeOnIce5v4,
toi.timeOnIce4v5 AS timeOnIce4v5
FROM TeamGameShotCountsWithOpponent AS tg
LEFT JOIN pk_pp_toi  AS toi
ON tg.gameId = toi.gameId AND tg.TeamId = toi.teamId





