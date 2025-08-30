import json
from pathlib import Path

# load the chosen exit rule we produced earlier
exit_rule = json.loads(Path("exit_out/exit_rule.json").read_text())

cfg = {
  "name": "Default portfolio",
  "version": "v1",
  "costs": { "fees_bps": 5, "slip_bps": 5 },
  "sizing": { "scheme": "equal_weight", "vt_target": 0.10, "max_leverage": 3.0 },
  "exit_rule": exit_rule
}

out = Path("out"); out.mkdir(exist_ok=True)
(out / "portfolio_defaults.json").write_text(json.dumps(cfg, indent=2))
print("Wrote out/portfolio_defaults.json")
