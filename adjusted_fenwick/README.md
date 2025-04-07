### Comparing Score-Adjusted Fenwick with regular Fenwick

This is a small project to re-create the Score-Adjusted Fenwick statistic from this article: https://www.hockeyviz.com/txt/senstats by Micah Blake McCurdy. All below information except for my implementation details are also explained in the article.

Fenwick is a statistic in hockey that is unblocked shots (ie, goals, shots on net, and missed shots, but not blocked shots), taken at 5v5.

A phenomenon in hockey called "score-effects" means that teams play very differently depending on the score of the game. When a team is leading, especially by multiple goals, they tend to sit back and take fewer shots. Teams behind tend to play more aggressively and looser, and take more shots. This means that shots taken / the shot differential in some situations are less representative of a team's ability than in other situations. One way to adjust for this is to adjust the counting of each shot, depending on the current score in the game.

This method creates a table of adjustments to multiply each shot attempt by to compensate for this effect. Here are the resulting coeffients derived from the data I used.

| HomeLeadBinned | HomeCoeff | AwayCoeff |
| -------------: | --------: | --------: |
|             -3 |  0.854558 |  1.145442 |
|             -2 |  0.881844 |  1.118156 |
|             -1 |  0.913341 |  1.086669 |
|              0 |  0.972246 |  1.027758 |
|              1 |  1.031380 |  0.968629 |
|              2 |  1.066231 |  0.933769 |
|              3 |  1.083466 |  0.916561 |

The formula used (taken straight from the article) is this:

```
(Coefficient for given team) * (Events for given team) = Average events for both teams
```

The source for this data is event data from the NHL's API. Then I filtered down the event data to just fenwick events (unblocked shot attempts at 5v5), taken from 2010-2011 to 2023-24. The main transformation that needed to be done to this data was that not every shot event has information about the score, only goal events get the new score after that goal. To compute this data, the AddScoresToAllEvents.sql file does a window over every game, shots sorted by time in the game, and for each shot looks the score when the last goal was scored, or 0-0 if no goal preceded that shot.

With that information now available, AdjustedFenwick.sql sums up how many shots the home team took, grouped by the score differential for the home team at the time of the shot. Then it does the same for the away team. Then it bins all of the shot counts from differentials >= 3 and <= -3 together, as beyond that margin there are comparatively far fewer shots, and the coefficient wouldn't change very much anyway. Then for each score differential, for each of the home team's shot count and the away team's shot count, it calculates how far away each is the mean of the two counts, and assigns the coefficient needed to bring each count back to the mean. This also adjusts the shot totals for venue (Home / Away), capturing the phenomenon that the home team typically has an advantage and takes more shots. This can be seen in the 0 lead bin, where the home team is assigned a coefficient slightly < 1 to account for this.

I.E. when up by 3 or more, the away team takes comparatively significantly fewer shots than the home team, so it gets a coefficient > 1 to count those shots more, and the home team gets a coefficient < 1 to give those shots less weight.

Then in PerTeamGameFenwickAndScore.sql, for each fenwick event, the adjusted value is calculated based on the current score. Then for each team and each game played, the raw and adjusted fenwick events are summed up, along with some other summary information like the game score.

Then in the notebook, the Adjusted Fenwick and regular Fenwick are compared in their repeatablity, as well as their ability to explain variance in goal differential and winning percentage. For each season/team combination, 100 random samples of 40 games are taken, split into (20 and 20 (or 5 and 35)) games each. Both average Adjusted Fenwick and average Raw Fenwick are computed for the first set of games. For the second set, either the same for repeatability (auto-determination), or goals differential / win percentage is computed. These pairs of data points are added to a dataset, where a regression is run to compute the R^2 score.

In each case, whether using a teams stats in 20 earlier games to predict performance 20 later games, or 5 games to predict performance in 35 games, the Adjusted Fenwick is a better predictor of future Adjusted Fenwick than normal Fenwick is for future Fenwick. Adjusted Fenwick also better predicts future goal differential, and better predicts future winning percentage. Tables for each of these are shows in the notebook.
