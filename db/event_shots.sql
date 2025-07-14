DROP VIEW IF EXISTS event_shots;
CREATE VIEW event_shots
AS SELECT e.eventId,  e.gameid, g.gameType, e.timeInPeriod, e.timeRemaining, e.typeDescKey,  e.typeDescKey IN ('shot-on-goal', 'goal') as isSOG, e.`periodDescriptor.number`, e.`periodDescriptor.periodType`,
e.`details.shotType`, e.`details.shootingPlayerId`, e.`details.goalieInNetId`,  e.`details.eventOwnerTeamId`, (e.`details.eventOwnerTeamId` = g.`homeTeamId`) as isHomeTeam, 
e.`details.homeSOG`, e.`details.awaySOG`, e.`details.scoringPlayerId`, e.`details.assist1PlayerId`, e.`details.assist2PlayerId`, e.`details.awayScore`, e.`details.homeScore`, e.situationCode,
e.`details.xCoord`, e.`details.yCoord`, e.`details.reason`, e.`details.blockingPlayerId`, e.`details.zoneCode`, e.typeCode, homeTeamDefendingSide, g.homeTeamId, g.visitingTeamId, season, e.homeScore, e.awayScore
FROM events AS e 
LEFT JOIN games_basic as g 
ON e.gameid=g.id
WHERE (g.gameType = 2 OR g.gameType=3) 
	AND g.season>=20102011 
	AND e.typeDescKey IN ('missed-shot', 'goal', 'shot-on-goal','blocked-shot') 
	AND e.`periodDescriptor.periodType` != 'SO'