from pathlib import Path
import json
import pandas as pd

ROOT = Path(__file__).resolve().parent
BAKE = ROOT / "exit_out" / "exit_bakeoff.csv"
RULE = ROOT / "exit_out" / "exit_rule.json"

def main():
    df = pd.read_csv(BAKE)

    family_col = "family"
    param_col  = "param"
    pf_col     = "pf"
    exp_col    = "expectancy"
    dd_col     = "mdd"

    for c in (family_col, param_col, pf_col, exp_col, dd_col):
        if c not in df.columns:
            raise SystemExit(f"Missing column in bakeoff: {c}")

    best = df.sort_values([pf_col, exp_col], ascending=[False, False]).iloc[0]

    # try to include a 6-bar time-cap as a backup, if present
    cap = None
    if (df[family_col] == "time_cap").any():
        caps = df.loc[df[family_col] == "time_cap", param_col].tolist()
        cap = 6 if 6 in caps else (caps[0] if caps else None)

    rule = {
        "chosen": {
            "family":   str(best[family_col]),
            "param":    float(best[param_col]),
            "pf":       float(best[pf_col]),
            "expectancy": float(best[exp_col]),
            "mdd":      float(best[dd_col]),
        },
        "hybrid": {
            "time_cap": cap,
            "policy": "first_trigger" if cap is not None else None
        }
    }

    RULE.write_text(json.dumps(rule, indent=2))
    print(f"[ok] Wrote {RULE}")

if __name__ == "__main__":
    main()
