# src/ml/serve_goal_half.py
import os, json
from typing import List, Dict
import numpy as np
import lightgbm as lgb
from fastapi import FastAPI, Body
import uvicorn

ROOT       = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MODELS_DIR = os.environ.get("MODELS_DIR", os.path.join(ROOT, "..", "models"))
HT_MODEL   = os.path.join(MODELS_DIR, "ht_lgbm.txt")
FT_MODEL   = os.path.join(MODELS_DIR, "ft_lgbm.txt")
FNAMES     = os.path.join(MODELS_DIR, "feature_names.json")

def load_booster_utf8(path: str) -> lgb.Booster:
    with open(path, "r", encoding="utf-8") as f:
        model_str = f.read()
    return lgb.Booster(model_str=model_str)

def load_feature_names(path: str) -> List[str]:
    with open(path, "r", encoding="utf-8") as f:
        obj = json.load(f)
    if isinstance(obj, list):
        names = obj
    elif isinstance(obj, dict):
        for k in ("feature_names","names","features"):
            if k in obj and isinstance(obj[k], list):
                names = obj[k]; break
        else:
            names = next((v for v in obj.values() if isinstance(v, list)), None)
            if names is None:
                raise ValueError("feature_names.json inválido")
    else:
        raise ValueError("feature_names.json inválido")
    # dedup preservando ordem
    seen, out = set(), []
    for n in map(str, names):
        if n not in seen:
            seen.add(n); out.append(n)
    return out

bst_ht = load_booster_utf8(HT_MODEL)
bst_ft = load_booster_utf8(FT_MODEL)
FEATURE_NAMES = load_feature_names(FNAMES)
N = len(FEATURE_NAMES)
if bst_ht.num_feature()!=N or bst_ft.num_feature()!=N:
    raise RuntimeError(f"n_features mismatch: file={N} ht={bst_ht.num_feature()} ft={bst_ft.num_feature()}")

app = FastAPI()

@app.get("/health")
def health():
    return {"ok": True, "n_features": N}

@app.post("/predict")
def predict(payload: Dict = Body(...)):
    """
    Espera: {"features": {name:value,...}}
    Retorna: {"p_ht": float, "p_ft": float}
    """
    feats = payload.get("features") or {}
    x = np.array([float(feats.get(n, 0.0)) for n in FEATURE_NAMES], dtype=np.float32)
    p_ht = float(bst_ht.predict(x[None, :])[0])
    p_ft = float(bst_ft.predict(x[None, :])[0])
    return {"p_ht": p_ht, "p_ft": p_ft}

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=int(os.environ.get("ML_PORT", "8009")))
