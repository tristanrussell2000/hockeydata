-- Finds all 5v5 shot events (goal/shot-on-goal/blocked-shot/missed-shot) for home and away team, groups by score differential at the time of the shot
-- Then bins the differentials, grouping >=3 and <= -3 into the same bucket (garbage time essentially)
-- Then computes coefficients for each of home/away (coeff is multiplier needed to eg. bring home count down to average of home and away count for that score diff)

WITH HomeFenwick AS 
(SELECT (homeScore-awayScore) AS homeLead, COUNT() AS ct 
FROM event_shots
WHERE situationCode='1551' AND typeDescKey != 'blocked-shot' AND isHomeTeam=1 
GROUP BY homeLead),

AwayFenwick AS 
(SELECT (homeScore-awayScore) AS homeLead, COUNT() as ct 
FROM event_shots 
WHERE situationCode='1551' AND typeDescKey != 'blocked-shot' AND isHomeTeam=0 
GROUP BY homeLead),

FenwickCounts AS 
(SELECT HomeFenwick.homeLead AS HomeLead, HomeFenwick.ct AS HomeCount, AwayFenwick.ct AS AwayCount 
FROM HomeFenwick 
LEFT JOIN AwayFenwick 
ON HomeFenwick.homeLead = AwayFenwick.homeLead),

BinnedFenwickCounts AS 
(SELECT
CASE 
	WHEN HomeLead <= -3 THEN -3
	WHEN HomeLead >=3 THEN 3
	ELSE HomeLead
END AS HomeLeadBinned,
SUM(HomeCount) AS HomeCountBinned,
SUM(AwayCount) AS AwayCountBinned
 FROM FenwickCounts
 GROUP BY HomeLeadBinned),
 
 FenwickCoeffs AS (SELECT 
 HomeLeadBinned,
 (CAST(AwayCountBinned AS REAL) / ((AwayCountBinned + HomeCountBinned) / 2)) AS HomeCoeff,
(CAST(HomeCountBinned AS REAL)/ ((AwayCountBinned + HomeCountBinned) / 2)) AS AwayCoeff
FROM BinnedFenwickCounts)

SELECT * FROM FenwickCoeffs

