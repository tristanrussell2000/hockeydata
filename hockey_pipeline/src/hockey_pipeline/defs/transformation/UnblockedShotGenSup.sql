DROP VIEW IF EXISTS UnblockedShotGenSup;
CREATE VIEW UnblockedShotGenSup AS
SELECT
gameId,
season,
TeamId,
TeamName,
isHomeTeam,
AVG(AdjustedFenwickFor) OVER (
	PARTITION BY season, TeamId
	ORDER BY gameId ASC
	ROWS BETWEEN 25 PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW
) AS Prev25FenwickForAvg,
AVG(AdjustedFenwickAgainst) OVER (
	PARTITION BY season, TeamId
	ORDER BY gameId ASC
	ROWS BETWEEN 25 PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW
) AS Prev25FenwickAgainstAvg
FROM FenwickAndScore
