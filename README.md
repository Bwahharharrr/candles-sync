# Candles Sync 📊

A Python utility for efficiently synchronizing and managing historical cryptocurrency candle data. This tool automatically handles data retrieval, storage, and gap-filling while maintaining a clean, organized file structure.

## 🌟 Features

- **Automated Data Synchronization**: Automatically fetches and updates candle data
- **Smart Data Management**: 
  - Stores data in daily CSV files for easy access and management
  - Automatically detects and fills data gaps
  - Maintains data integrity with validation checks
- **Flexible Time Ranges**: Support for various timeframes (1m, 4h, 1d, etc.)
- **Efficient Storage**: Organized file structure for easy data access
- **Progress Tracking**: Uses `.gotstart` file to track initial data population

## 📂 File Structure

Data is stored in a hierarchical structure:
```
~/.corky/
└── {exchange}
    ├── candles
    │   ├── {ticker}
    │   │   ├── {timeframe}
    │   │   │   ├── YYYY-MM-DD.csv
    │   │   │   ├── .gotstart
```

## 🚀 Installation

### Via pip (recommended)
```bash
pip install -e .
```

### Manual Installation
```bash
git clone https://github.com/yourusername/candles-sync
cd candles-sync
pip install -e .
```

## 💻 Usage

### Command Line Interface
```bash
candles-sync --exchange BITFINEX --ticker tBTCUSD --timeframe 1h --end "2024-02-10"
```

### Python Module
```python
from candles_sync import synchronize_candle_data

# Synchronize data for a specific ticker and timeframe
success = synchronize_candle_data(
    exchange="BITFINEX",
    ticker="tBTCUSD",
    timeframe="1h",
    end_date_str="2024-02-10",  # Optional
    verbose=True                 # Optional
)

if success:
    print("Synchronization completed successfully")
```

## 🔍 Expected Behavior

- **Initial Run**: If no `.gotstart` file exists, the script will:
  1. Begin batch polling to fill the entire historical dataset
  2. Create a `.gotstart` file after the first successful API request

- **Subsequent Runs**: The script will:
  1. Validate existing data for completeness
  2. Fill any gaps in the data
  3. Update the current day's data if no end date is specified

- **Data Storage**: 
  - All candles are stored in daily files (YYYY-MM-DD.csv)
  - Applies to all timeframes (1m, 4h, 1d, etc.)

## 🛠️ Development

### Prerequisites
- Python 3.8+
- Required packages:
  - requests>=2.32.3
  - pandas>=2.2.3
  - colorama>=0.4.6

### Setting Up Development Environment
```bash
git clone https://github.com/yourusername/candles-sync
cd candles-sync
pip install -e .
```

## 📝 License

[Your License] - See LICENSE file for details

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## 📧 Contact

Your Name - your.email@example.com

Project Link: [https://github.com/yourusername/candles-sync](https://github.com/yourusername/candles-sync) 