import os
import pandas as pd
from modules.breakout_detector import detect_breakouts

def main():
    data_folder      = "Data"
    raw_folder       = os.path.join(data_folder, "Raw")
    processed_folder = os.path.join(data_folder, "Processed")

    indicators_path = os.path.join(processed_folder, "per_bar_indicators_core.csv")
    macro_path      = os.path.join(raw_folder, "macro_regime_data.csv")
    output_path     = os.path.join(processed_folder, "static_breakouts.csv")

    os.makedirs(processed_folder, exist_ok=True)

    # Load inputs
    df_indicators = pd.read_csv(indicators_path, parse_dates=["date"])
    df_macro      = pd.read_csv(macro_path,      parse_dates=["date"])

    # Detect breakouts
    df_breakouts = detect_breakouts(df_indicators, df_macro)

    # Write output
    df_breakouts.to_csv(output_path, index=False)
    print(f"✅ static_breakouts.csv saved to {output_path}")

if __name__ == "__main__":
    main()

