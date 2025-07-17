DROP VIEW IF EXISTS UnblockedShotGenSup;
CREATE VIEW UnblockedShotGenSup AS
WITH SummedFenwickAndToi AS (SELECT
gameId,
season,
TeamId,
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

SELECT 
gameId,
season,
TeamId,
TeamName,
isHomeTeam,
Prev25FenwickForSum * 1.0 / (Prev25TOI5v5 * 1.0 / 60 / 60) AS Prev25FenwickForPerHour,
Prev25FenwickAgainstSum* 1.0 / (Prev25TOI5v5 * 1.0 / 60 / 60) AS Prev25FenwickAgainstPerHour
FROM SummedFenwickAndToi

