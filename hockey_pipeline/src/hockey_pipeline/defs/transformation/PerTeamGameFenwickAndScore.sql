DROP VIEW IF EXISTS FenwickAndScore;
CREATE VIEW  IF NOT EXISTS FenwickAndScore AS
WITH AdjustedFenwickWithOpponent AS 
(SELECT 
Team.GameId, Team.Season, Team.TeamId AS TeamId, Team.TeamFullName AS TeamName, Team.AdjustedFenwick AS AdjustedFenwickFor, Team.RawFenwick AS RawFenwickFor, Team.IsHomeTeam, 
Opp.TeamId AS OpponentId, Opp.TeamFullName AS OpponentName, Opp.AdjustedFenwick AS AdjustedFenwickAgainst, Opp.RawFenwick AS RawFenwickAgainst,
(Team.IsHomeTeam * Team.HomeFinal + (1- Team.IsHomeTeam) * Team.AwayFinal) AS ScoreFor,
(Team.IsHomeTeam * Team.AwayFinal + (1- Team.IsHomeTeam) * Team.HomeFinal) AS ScoreAgainst
FROM PerTeamGameAdjustedFenwick AS Team
LEFT JOIN PerTeamGameAdjustedFenwick AS Opp
ON Team.gameId=Opp.gameId AND Team.TeamId != Opp.TeamId)

SELECT 
f.*, 
f.AdjustedFenwickFor * 1.0 / (toi.timeOnIcePerGame5v5 / 60.0 / 60.0) AS AdjustedFenwickForPerHour,
f.AdjustedFenwickAgainst * 1.0 / (toi.timeOnIcePerGame5v5 / 60.0 / 60.0) AS AdjustedFenwickAgainstPerHour,
toi.timeOnIcePerGame5v5 AS timeOnIcePerGame5v5
FROM AdjustedFenwickWithOpponent AS f
LEFT JOIN pk_pp_toi  AS toi
ON f.gameId = toi.gameId AND f.TeamId = toi.teamId
