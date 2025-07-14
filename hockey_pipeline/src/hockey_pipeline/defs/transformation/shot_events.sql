DROP VIEW IF EXISTS shot_events;
CREATE VIEW shot_events
AS SELECT e.eventId,  e.gameId, g.gameType, e.timeInPeriod, e.timeRemaining, e.typeDescKey,  e.typeDescKey IN ('shot-on-goal', 'goal') as isSOG, e.periodNumber, e.periodType,
e.shotType, e.shootingPlayerId, e.goalieInNetId,  e.eventOwnerTeamId, (e.eventOwnerTeamId = g.homeTeamId) as isHomeTeam, 
e.homeSOG, e.awaySOG, e.scoringPlayerId, e.assist1PlayerId, e.assist2PlayerId, e.awayScore, e.homeScore, e.situationCode,
e.xCoord, e.yCoord, e.reason, e.blockingPlayerId, e.zoneCode, e.typeCode, homeTeamDefendingSide, g.homeTeamId, g.visitingTeamId, season, e.homeScoreAdj, e.awayScoreAdj
FROM events AS e 
LEFT JOIN games as g 
ON e.gameId=g.id
WHERE (g.gameType = 2 OR g.gameType=3) 
	AND g.season>=20102011 
	AND e.typeDescKey IN ('missed-shot', 'goal', 'shot-on-goal','blocked-shot') 
	AND e.periodType != 'SO'