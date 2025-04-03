BASE_URL = "api-web.nhle.com/v1/"
SEASON_URL = "season"
def season_url(team, season): return f"club-schedule-season/{team}/{season}"
def pbp_url(gameid): return f"gamecenter/{gameid}/play-by-play"

 