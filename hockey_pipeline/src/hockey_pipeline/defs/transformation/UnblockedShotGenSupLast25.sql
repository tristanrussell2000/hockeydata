DROP VIEW IF EXISTS UnblockedShotGenSupLast25;
CREATE VIEW UnblockedShotGenSupLast25 AS
WITH SummedFenwickAndToi AS (SELECT
gameId,
season,
TeamId,
OpponentId,
TeamName,
isHomeTeam,
SUM(AdjustedFenwickFor) OVER (
	PARTITION BY season, TeamId
	ORDER BY gameId ASC
	ROWS BETWEEN 25 PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW
) AS Prev25FenwickForSum,
SUM(AdjustedFenwickAgainst) OVER (
	PARTITION BY season, TeamId
	ORDER BY gameId ASC
	ROWS BETWEEN 25 PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW
) AS Prev25FenwickAgainstSum,
SUM(timeOnIcePerGame5v5)  OVER (
	PARTITION BY season, TeamId
	ORDER BY gameId ASC
	ROWS BETWEEN 25 PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW
) AS Prev25TOI5v5
FROM FenwickAndScore)

, PerHour AS (SELECT 
gameId,
season,
TeamId,
TeamName,
isHomeTeam,
Prev25FenwickForSum * 1.0 / (Prev25TOI5v5 * 1.0 / 60 / 60) AS Prev25FenwickForPerHour,
Prev25FenwickAgainstSum* 1.0 / (Prev25TOI5v5 * 1.0 / 60 / 60) AS Prev25FenwickAgainstPerHour
FROM SummedFenwickAndToi)

SELECT 
Team.*,
Opp.Prev25FenwickForPerHour AS OpponentPrev25FenwickForPerHour,
Opp.Prev25FenwickAgainstPerHour AS OpponentPrev25FenwickAgainstPerHour
FROM PerHour AS Team
LEFT JOIN PerHour AS Opp
ON Team.gameId = Opp.gameId AND Team.TeamId != Opp.TeamId
ORDER BY gameId DESC
