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
WHERE typeDescKey IN ('goal', 'shot-on-goal') AND situationCode=1551
GROUP BY gameId, eventOwnerTeamId, season, homeTeamId)

,AllGameShotsGoals AS (
    SELECT
        TeamGames.id,
        TeamGames.season,
        TeamGames.teamId,
        TeamGames.oppTeamId,
        TeamGames.isHomeTeam,
        GameShotsGoals.gameGoals,
        GameShotsGoals.gameShots
    FROM TeamGames
    LEFT JOIN GameShotsGoals ON TeamGames.id = GameShotsGoals.gameId AND TeamGames.teamId = GameShotsGoals.eventOwnerTeamId
)

,Prev25GamesShotsGoals AS (SELECT 
id,
season,
teamId,
oppTeamId,
isHomeTeam,
SUM(gameGoals) OVER (
	PARTITION BY season, teamId
	ORDER BY id ASC
	ROWS BETWEEN 25 PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW
) AS Prev25GameGoals,
SUM(gameShots) OVER (
    PARTITION BY season, teamId
    ORDER BY id ASC
    ROWS BETWEEN 25 PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW
) AS Prev25GameShots,
COUNT(gameShots) OVER (
    PARTITION BY season, teamId
    ORDER BY id ASC
    ROWS BETWEEN 25 PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW
) AS PrevGamesCounted
FROM AllGameShotsGoals)

SELECT
TeamGames.id,
TeamGames.season,
TeamGames.teamId,
TeamGames.isHomeTeam,
Prev25GameGoals,
Prev25GameShots,
Prev25GameGoals*1.0 / Prev25GameShots AS Prev25ShootingPercentage,
PrevGamesCounted
FROM TeamGames
LEFT JOIN Prev25GamesShotsGoals ON TeamGames.id = Prev25GamesShotsGoals.id AND TeamGames.teamId = Prev25GamesShotsGoals.teamId
WHERE TeamGames.id >= 2010020000
order by  TeamGames.id DESC







