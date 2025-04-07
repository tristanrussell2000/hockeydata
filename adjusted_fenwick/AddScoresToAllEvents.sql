-- For each event, looks back for the previous goal to get the score information at the time the event happened
-- Specifically ignores CURRENT ROW because a goal was scored at the time when the score was what the previous goal made it, only the next event has the new score

WITH ScoreEvent AS (SELECT
		eventId, gameid,
		IFNULL(MAX(`details.awayScore`)  FILTER  (WHERE `details.awayScore` IS NOT NULL) OVER (
                PARTITION BY gameid
                ORDER BY `periodDescriptor.number` ASC, timeInPeriod ASC
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW
            ),
            0
        ) AS score_away_at_event,
        IFNULL(
            MAX(`details.homeScore`) FILTER (WHERE `details.awayScore` IS NOT NULL) OVER (
                PARTITION BY gameid
                ORDER BY `periodDescriptor.number`, timeInPeriod
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW
            ),
            0
        ) AS score_home_at_event
FROM events
ORDER BY 	gameid DESC, `periodDescriptor.number` ASC, timeInPeriod ASC)

UPDATE events
SET
    awayScore = ScoreEvent.score_away_at_event,
    homeScore = ScoreEvent.score_home_at_event
FROM
    ScoreEvent
WHERE
    events.eventId = ScoreEvent.eventId AND events.gameid = ScoreEvent.gameid 
