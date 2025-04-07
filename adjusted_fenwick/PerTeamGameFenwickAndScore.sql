DROP VIEW IF EXISTS FenwickAndScore;
CREATE VIEW FenwickAndScore AS
WITH FenwickCounts AS (SELECT
`details.eventOwnerTeamId` as TeamId,
gameid,
(`details.eventOwnerTeamId` = homeTeamId) AS isHomeTeam,
MIN(MAX(homeScore - awayScore, -3), 3) as HomeLead,
((coeff.HomeCoeff * isHomeTeam) + (coeff.AwayCoeff * (1- isHomeTeam))) AS shot_value,
homeScore,
awayScore,
gameType
 FROM event_shots 
 LEFT JOIN fenwick_coeffs as coeff ON HomeLead = coeff.HomeLeadBinned
WHERE  typeDescKey != 'blocked-shot'  AND situationCode='1551' AND gameType=2)


,AdjustedFenwick AS
 (SELECT TeamId, gameid, t.fullName, MAX(isHomeTeam) AS isHomeTeam, SUM(shot_value) AS AdjustedFenwick, COUNT() AS RawFenwick, MAX(homeScore) as HomeFinal, MAX(awayScore) AS AwayFinal
FROM FenwickCounts
LEFT JOIN teams as t ON TeamId=t.id
GROUP BY TeamId, gameid
ORDER BY gameid ASC)

SELECT 
Team.gameid, Team.TeamId AS TeamId, Team.fullName AS TeamName, Team.AdjustedFenwick AS AdjustedFenwickFor, Team.RawFenwick AS RawFenwickFor, Team.isHomeTeam, 
Opp.TeamId AS OpponentId, Opp.fullName AS OpponentName, Opp.AdjustedFenwick AS AdjustedFenwickAgainst, Opp.RawFenwick AS RawFenwickAgainst,
(Team.isHomeTeam * Team.HomeFinal + (1- Team.isHomeTeam) * Team.AwayFinal) AS ScoreFor,
(Team.isHomeTeam * Team.AwayFinal + (1- Team.isHomeTeam) * Team.HomeFinal) AS ScoreAgainst
FROM AdjustedFenwick AS Team
LEFT JOIN AdjustedFenwick AS Opp
ON Team.gameid=Opp.gameid AND Team.TeamId != Opp.TeamId


