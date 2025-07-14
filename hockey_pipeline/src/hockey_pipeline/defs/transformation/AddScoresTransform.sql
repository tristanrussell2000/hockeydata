-- For each event, looks back for the previous goal to get the score information at the time the event happened
-- Specifically ignores CURRENT ROW because a goal was scored at the time when the score was what the previous goal made it, only the next event has the new score

WITH ScoreEvent AS (SELECT
		eventId, gameId,
		IFNULL(MAX(awayScore)  FILTER  (WHERE awayScore IS NOT NULL) OVER (
                PARTITION BY gameId
                ORDER BY periodNumber ASC, timeInPeriod ASC
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW
            ),
            0
        ) AS score_away_at_event,
        IFNULL(
            MAX(homeScore) FILTER (WHERE awayScore IS NOT NULL) OVER (
                PARTITION BY gameId
                ORDER BY periodNumber ASC, timeInPeriod ASC
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW
            ),
            0
        ) AS score_home_at_event
FROM events
ORDER BY gameId DESC, periodNumber ASC, timeInPeriod ASC)

UPDATE events
SET
    awayScoreAdj = ScoreEvent.score_away_at_event,
    homeScoreAdj = ScoreEvent.score_home_at_event
FROM
    ScoreEvent
WHERE
    events.eventId = ScoreEvent.eventId AND events.gameId = ScoreEvent.gameId 
