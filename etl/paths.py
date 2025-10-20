from pathlib import Path

DATA     = Path("data")
RAW      = DATA / "00-raw"
CLEAN    = DATA / "01-clean"
MODEL    = DATA / "02-model"
SANDBOX  = DATA / "03-sandbox"

# ensure write targets exist
CLEAN.mkdir(parents=True, exist_ok=True)
(MODEL).mkdir(parents=True, exist_ok=True)
(CLEAN / "_dq").mkdir(parents=True, exist_ok=True)
