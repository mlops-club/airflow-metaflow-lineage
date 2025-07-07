"""
Forecasting helper functions for generating predictions.
"""

from datetime import timedelta

import pandas as pd


def generate_seasonal_naive_forecast(predict_horizon_hours: int, up_to_as_of_datetime: pd.DataFrame) -> pd.DataFrame:
    """
    Generate seasonal naive forecast: predict value = value from 7 days earlier
    for each pulocationid, year, month, day, hour combination.

    Args:
        predict_horizon_hours: Number of hours into the future to forecast
        up_to_as_of_datetime: DataFrame with historical data up to the as_of datetime
                              Expected columns: ['pulocationid', 'year', 'month', 'day', 'hour', 'total_rides']

    Returns:
        DataFrame with forecast data containing columns:
        ['pulocationid', 'year', 'month', 'day', 'hour', 'forecasted_total_rides']
    """
    # Create datetime column for easier manipulation
    up_to_as_of_datetime["datetime"] = pd.to_datetime(up_to_as_of_datetime[["year", "month", "day", "hour"]])

    # Find the latest datetime in the data (this should be our as_of point)
    max_datetime = up_to_as_of_datetime["datetime"].max()

    # Create forecast periods (next predict_horizon_hours)
    forecast_periods = []
    for h in range(predict_horizon_hours):
        forecast_dt = max_datetime + timedelta(hours=h + 1)  # Start from next hour
        forecast_periods.append(
            {
                "year": forecast_dt.year,
                "month": forecast_dt.month,
                "day": forecast_dt.day,
                "hour": forecast_dt.hour,
                "forecast_datetime": forecast_dt,
            }
        )

    forecast_df = pd.DataFrame(forecast_periods)

    # Get unique pulocationids from historical data
    unique_locations = up_to_as_of_datetime["pulocationid"].unique()

    # Cross join forecast periods with all locations
    location_df = pd.DataFrame({"pulocationid": unique_locations})
    forecast_grid = forecast_df.assign(key=1).merge(location_df.assign(key=1), on="key").drop("key", axis=1)

    # For each forecast period, look up the value from 7 days earlier
    seasonal_forecasts = []

    for _, row in forecast_grid.iterrows():
        # Calculate the datetime 7 days earlier
        seven_days_ago = row["forecast_datetime"] - timedelta(days=7)

        # Look up the actual value from 7 days ago in historical data
        historical_match = up_to_as_of_datetime[
            (up_to_as_of_datetime["pulocationid"] == row["pulocationid"])
            & (up_to_as_of_datetime["year"] == seven_days_ago.year)
            & (up_to_as_of_datetime["month"] == seven_days_ago.month)
            & (up_to_as_of_datetime["day"] == seven_days_ago.day)
            & (up_to_as_of_datetime["hour"] == seven_days_ago.hour)
        ]

        # Use the historical value if available, otherwise use 0 as fallback
        if not historical_match.empty:
            forecasted_total_rides = historical_match["total_rides"].iloc[0]
        else:
            forecasted_total_rides = 0  # fallback for missing historical data

        seasonal_forecasts.append(
            {
                "pulocationid": row["pulocationid"],
                "year": row["year"],
                "month": row["month"],
                "day": row["day"],
                "hour": row["hour"],
                "forecasted_total_rides": forecasted_total_rides,
            }
        )

    return pd.DataFrame(seasonal_forecasts)
