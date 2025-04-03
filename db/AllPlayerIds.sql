DROP VIEW IF EXISTS AllPlayerIds;
CREATE VIEW AllPlayerIds AS
WITH playerIds AS (SELECT DISTINCT `details.shootingPlayerId` as pid FROM events WHERE pid NOT NULL
UNION ALL
SELECT DISTINCT  `details.scoringPlayerId` as pid FROM events WHERE pid NOT NULL
UNION ALL
SELECT DISTINCT `details.committedByPlayerId` as pid FROM events WHERE pid NOT NULL
UNION ALL
SELECT DISTINCT  `details.goalieInNetId`as pid FROM events WHERE pid NOT NULL
UNION ALL
SELECT DISTINCT `details.assist1PlayerId` as pid FROM events WHERE pid NOT NULL
UNION ALL
SELECT DISTINCT `details.assist2PlayerId` as pid FROM events WHERE pid NOT NULL)

SELECT DISTINCT cast(pid AS INT ) AS playerId FROM playerIds