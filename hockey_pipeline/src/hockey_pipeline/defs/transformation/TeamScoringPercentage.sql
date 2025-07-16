DROP VIEW IF EXISTS TeamScoringPercentage;
CREATE VIEW TeamScoringPercentage AS
WITH GameShotsGoals AS (SELECT 
gameId,
season,
eventOwnerTeamId,
homeTeamId,
eventOwnerTeamId = homeTeamId AS isHomeTeam,
SUM(CASE WHEN typeDescKey='goal' THEN 1 ELSE 0 END) AS gameGoals,
SUM(1) AS gameShots
FROM shot_events
WHERE typeDescKey IN ("goal", "shot-on-goal") AND situationCode=1551
GROUP BY gameId, eventOwnerTeamId, season, homeTeamId)

,Prev25GamesShotsGoals AS (SELECT 
gameId,
season,
eventOwnerTeamId,
isHomeTeam,
SUM(
	gameGoals
) OVER (
	PARTITION BY season, eventOwnerTeamId
	ORDER BY gameId ASC
	ROWS BETWEEN 25 PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW
) AS Prev25GameGoals,
SUM(gameShots) OVER (
PARTITION BY season, eventOwnerTeamId
ORDER BY gameId ASC
ROWS BETWEEN 25 PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW
) AS Prev25GameShots
FROM GameShotsGoals)

SELECT
*,
Prev25GameGoals*1.0 / Prev25GameShots AS Prev25ShootingPercentage
FROM Prev25GamesShotsGoals







