DROP VIEW IF EXISTS ShotGenSup5v4Last25;
CREATE VIEW ShotGenSup5v4Last25 AS
WITH LaggedShots AS (SELECT
id,
season,
teamId,
oppTeamId,
isHomeTeam,
SUM(TotalPowerPlayShotsFor) OVER (
	PARTITION BY season, teamId
	ORDER BY id ASC
	ROWS BETWEEN 25 PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW
) AS Prev25ShotsFor5v4Sum,
SUM(TotalPenaltyKillShotsAgainst) OVER (
	PARTITION BY season, teamId
	ORDER BY id ASC
	ROWS BETWEEN 25 PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW
) AS Prev25ShotsAgainst4v5Sum,
SUM(timeOnIce5v4)  OVER (
	PARTITION BY season, teamId
	ORDER BY id ASC
	ROWS BETWEEN 25 PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW
) AS Prev25TOI5v4,
SUM(timeOnIce4v5)  OVER (
	PARTITION BY season, teamId
	ORDER BY id ASC
	ROWS BETWEEN 25 PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW
) AS Prev25TOI4v5,
COUNT(timeOnIce4v5) OVER (
    PARTITION BY season, teamId
    ORDER BY id ASC
    ROWS BETWEEN 25 PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW
) AS PrevGamesCounted
FROM ShotGenSup5v4)

SELECT 
id,
season,
teamId,
oppTeamId,
isHomeTeam,
Prev25ShotsFor5v4Sum,
Prev25ShotsFor5v4Sum * 1.0 / (Prev25TOI5v4 * 1.0 / 60 / 60) AS Prev25ShotsFor5v4PerHour,
Prev25ShotsAgainst4v5Sum,
Prev25ShotsAgainst4v5Sum* 1.0 / (Prev25TOI4v5 * 1.0 / 60 / 60) AS Prev25ShotsAgainst4v5PerHour,
PrevGamesCounted
FROM LaggedShots
ORDER BY id DESC

