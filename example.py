from candles_sync import synchronize_candle_data, main

main()

# Synchronize data for a specific ticker and timeframe
#success = synchronize_candle_data(
#    exchange="BITFINEX",
#    ticker="tBTCUSD",
#    timeframe="1h",
#    end_date_str="2024-02-10",  # Optional
#    verbose=True                 # Optional
#)
#
#if success:
#    print("Synchronization completed successfully")