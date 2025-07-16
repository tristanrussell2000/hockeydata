# Things to get:

### Measure	Model Coefficient (Beta)	Mean	Standard Deviation
 - 5v5 Unblocked Shot Generation, Home Team	+0.075	40.48	3.92
 - 5v5 Unblocked Shot Generation, Away Team	-0.111	40.59	4.02
 - 5v5 Unblocked Shot Suppression, Home Team	-0.178	40.50	4.00
 - 5v5 Unblocked Shot Suppression, Away Team	+0.106	40.50	4.09

 Plan: Run over shot_events for unblocked shots (not sure if shots or shot attempts, maybe do both) with a window function to compute this
 DONE

### Measure	Model Coefficient (Beta)	Mean	Standard Deviation
 - Lifetime Diluted 5v5 goals per shot-on-goal, Home team	-0.058	0.0782	0.0045
 - Lifetime Diluted 5v5 goals per shot-on-goal, Away team	+0.070	0.0786	0.0046

 Plan: Should already be done in the goalie stats view
 DONE

### Measure	Model Coefficient (Beta)	Mean	Standard Deviation
 - 5v5 Goals per Shot-on-goal, Home team	+0.031	0.0425	0.0092
 - 5v5 Goals per Shot-on-goal, Away team	-0.091	0.0424	0.0092

Plan: also a window-function view?

### Measure	Model Coefficient (Beta)	Mean	Standard Deviation
 - 5v4 Shot Generation, Home Team	+0.057	95.08	12.81
 - 5v4 Shot Generation, Away Team	-0.063	95.02	13.02
 - 4v5 Shot Suppression, Home Team	-0.015	95.13	11.96
 - 4v5 Shot Suppression, Away Team	+0.024	95.22	12.15

Plan: again, a window-function view?

All shots, not just unblocked shots

### Home-ice Advantage
Finally, Oscar includes a home-ice advantage term of 0.215; this corresponds to a historical advantage of 55%.


Basically get a list, for each team-game combo, all of these terms for X, and whether the team won the game +1 / -1 for win / loss, 0 for tie / shootout. Should also do the standardization in Python, make a notebook or something to load all the data, do the adjustments, then combine into one data set, then train a log reg model.




