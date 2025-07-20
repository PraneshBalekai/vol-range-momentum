# ETL to update daily data
# poetry run python etl/update_historical_data.py --config_path configs/update_historical_spy.json

# poetry run python etl/update_historical_data.py --config_path configs/update_historical_qqq.json


# Binance data
"""
poetry run bnb_update \
--config-path /Users/praneshbalekai/Desktop/IB_PRD/etl/configs/update_historical_bnb.json \
--symbol BTCUSDT \
--start-date 2025-05-26 \
--end-date 2025-06-13
"""

# // "contract": {
# //     "symbol": "SPY",
# //     "secType": "STK",
# //     "exchange": "SMART",
# //     "currency": "USD"
# // }
