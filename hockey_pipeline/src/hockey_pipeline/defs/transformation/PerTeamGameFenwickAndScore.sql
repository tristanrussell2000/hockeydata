DROP VIEW IF EXISTS FenwickAndScore;
CREATE VIEW FenwickAndScore AS
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
WHERE  typeDescKey != 'blocked-shot'  AND situationCode='1551' AND gameType=2)


,AdjustedFenwick AS
 (SELECT TeamId, gameId, season, t.fullName, 
         MAX(isHomeTeam) AS isHomeTeam, 
         SUM(shot_value) AS AdjustedFenwick, 
         COUNT() AS RawFenwick, 
         MAX(homeScoreAdj) as HomeFinal, 
         MAX(awayScoreAdj) AS AwayFinal
FROM FenwickCounts
LEFT JOIN teams as t ON TeamId=t.id
GROUP BY TeamId, gameId
ORDER BY gameId ASC)

,PerTeamGameAdjustedFenwick AS 
(SELECT 
Team.gameId, Team.season, Team.TeamId AS TeamId, Team.fullName AS TeamName, Team.AdjustedFenwick AS AdjustedFenwickFor, Team.RawFenwick AS RawFenwickFor, Team.isHomeTeam, 
Opp.TeamId AS OpponentId, Opp.fullName AS OpponentName, Opp.AdjustedFenwick AS AdjustedFenwickAgainst, Opp.RawFenwick AS RawFenwickAgainst,
(Team.isHomeTeam * Team.HomeFinal + (1- Team.isHomeTeam) * Team.AwayFinal) AS ScoreFor,
(Team.isHomeTeam * Team.AwayFinal + (1- Team.isHomeTeam) * Team.HomeFinal) AS ScoreAgainst
FROM AdjustedFenwick AS Team
LEFT JOIN AdjustedFenwick AS Opp
ON Team.gameId=Opp.gameId AND Team.TeamId != Opp.TeamId)

SELECT 
f.*, 
f.AdjustedFenwickFor * 1.0 / (toi.timeOnIcePerGame5v5 / 60.0 / 60.0) AS AdjustedFenwickForPerHour,
f.AdjustedFenwickAgainst * 1.0 / (toi.timeOnIcePerGame5v5 / 60.0 / 60.0) AS AdjustedFenwickAgainstPerHour,
toi.timeOnIcePerGame5v5 AS timeOnIcePerGame5v5
FROM PerTeamGameAdjustedFenwick AS f
LEFT JOIN pk_pp_toi  AS toi
ON f.gameId = toi.gameId AND f.TeamId = toi.teamId



