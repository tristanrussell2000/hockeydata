import pandas as pd
from dagster import asset, AssetIn
from sklearn.model_selection import cross_val_score, GridSearchCV
from sklearn.linear_model import LogisticRegression
from pickle import dump
from pathlib import Path

@asset(
    ins = {
        "training_data": AssetIn(key_prefix=["model", "oscar"])
    },
    key_prefix=["model", "oscar"],
    description="Fill training dataset for the Oscar model. https://hockeyviz.com/txt/oscar"
)
def logistic_regression(training_data: pd.DataFrame):
    cols = ["HomePrev25FenwickForPerHour",
        "AwayPrev25FenwickForPerHour",
        "HomePrev25FenwickAgainstPerHour", 
        "AwayPrev25FenwickAgainstPerHour",
        "HomePrev25ShotsFor5v4PerHour", 
        "AwayPrev25ShotsFor5v4PerHour",
        "HomePrev25ShotsAgainst4v5PerHour", 
        "AwayPrev25ShotsAgainst4v5PerHour",
        "HomePrev25ShootingPercentage", 
        "AwayPrev25ShootingPercentage",
        "HomeDilutedSavePct", 
        "AwayDilutedSavePct"]
    X = training_data[cols].copy()
    Y = training_data[["HomeTeamWin"]].copy()

    log_reg = LogisticRegression()

    gs = GridSearchCV(log_reg, {
        'C': [0.01, 0.1, 1, 10, 100],
        'fit_intercept': [True, False],
        'solver': ["liblinear", "newton-cholesky", "newton-cg", "saga"]
    }, cv=5)

    gs.fit(X, Y.values)

    print(f"Best Score: {gs.best_score_}")
    print(f"Best Parameters: ", gs.best_params_)
    print(f"Coeffs: ", list(zip(cols, gs.best_estimator_.coef_[0])))

    file_name = Path(__file__).parent / "oscar.pkl"
    with open(file_name, "wb") as f:
        dump(gs.best_estimator_, f, protocol=5)

    return
   