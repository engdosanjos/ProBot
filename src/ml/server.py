# src/ml/server.py
# Flask server: recebe {"features": {...}} e retorna {"p_ht": float, "p_ft": float}
# Usa os modelos LightGBM treinados em models/.

import os, json
from pathlib import Path
from flask import Flask, request, jsonify
import numpy as np
import lightgbm as lgb

MODEL_DIR = Path(os.environ.get("MODEL_DIR", "models"))
FN_HT = MODEL_DIR / "ht_lgbm.txt"
FN_FT = MODEL_DIR / "ft_lgbm.txt"
FN_META = MODEL_DIR / "feature_names.json"

app = Flask(__name__)

bst_ht = None
bst_ft = None
feat_names = None

def load_models():
    global bst_ht, bst_ft, feat_names
    if not FN_HT.exists() or not FN_FT.exists() or not FN_META.exists():
        raise FileNotFoundError("Modelos/feature_names não encontrados em 'models/'. Treine antes.")

    bst_ht = lgb.Booster(model_file=str(FN_HT))
    bst_ft = lgb.Booster(model_file=str(FN_FT))
    feat_names = json.loads(FN_META.read_text(encoding="utf-8"))["feature_names"]
    app.logger.info("Modelos carregados.")

def vectorize(feats: dict):
    # alinha na ordem dos nomes de features
    x = [float(feats.get(k, 0.0)) for k in feat_names]
    return np.array([x], dtype=np.float32)

@app.route("/predict", methods=["POST"])
def predict():
    data = request.get_json(silent=True) or {}
    if "features" in data:
        x = vectorize(data["features"])
        p_ht = float(bst_ht.predict(x)[0])
        p_ft = float(bst_ft.predict(x)[0])
        return jsonify(dict(p_ht=p_ht, p_ft=p_ft))
    elif "batch" in data and isinstance(data["batch"], list):
        xs = np.vstack([vectorize(f) for f in data["batch"]])
        p_ht = bst_ht.predict(xs).tolist()
        p_ft = bst_ft.predict(xs).tolist()
        return jsonify(dict(p_ht=p_ht, p_ft=p_ft))
    else:
        return jsonify(error="payload deve conter 'features' ou 'batch'."), 400

if __name__ == "__main__":
    load_models()
    host = os.environ.get("ML_HOST","127.0.0.1")
    port = int(os.environ.get("ML_PORT","5005"))
    app.run(host=host, port=port, debug=False)
