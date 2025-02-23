# Sync Candles

## 📌 About
A useful python script to sync candles

## 📂 Folder Structure
Stores files in the following structure:
```
/home/user/.corky/
└── {exchange}
    ├── candles
    │   ├── {ticker}
    │   │   ├── {timeframe}  
```

## 🔍 Expected Behavior 
Candles are always stored in daily files (be it 1m, 4h, or 1d) ..   they are always stored in YYYY-MM-DD.csv

If no .gotstart file exists in the timeframes folder, the script will begin batch polling to fill the entire data set is complete. 
The .gotstart file is always created after the first api request is processed, letting future runs of the script know we have the earliest data.
The script will always validate the entire daily set of data is present by walking incrementally through the files stored, looking for deleted files, or gaps - which it will then re-query, filling in those gaps where possible, or if no data is returned, with blank files.
If no end date is supplied, the script will always update the current daily file to keep it as up to date as possible with no data when it exists/is different from the file.

## Install
Install with `pip install -e .` 
After install you can use directly with just `candles-sync` on the command line, or alternatively you can use the `synchronize_candle_data` function directly in your own python scripts via "from candles_sync import synchronize_candle_data"
