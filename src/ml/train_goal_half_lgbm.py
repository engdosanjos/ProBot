# src/ml/train_goal_half_lgbm.py
import os, json, sqlite3, random, time
from typing import List, Dict, Tuple
import numpy as np
import lightgbm as lgb

# ====================== PATHS & PARAMS ======================
ROOT       = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH    = os.environ.get("DB_PATH",    os.path.join(ROOT, "..", "data", "events.db"))
MODELS_DIR = os.environ.get("MODELS_DIR", os.path.join(ROOT, "..", "models"))

HT_MODEL   = os.path.join(MODELS_DIR, "ht_lgbm.txt")
FT_MODEL   = os.path.join(MODELS_DIR, "ft_lgbm.txt")
FNAMES     = os.path.join(MODELS_DIR, "feature_names.json")

LOOKBACK_MIN  = int(os.environ.get("PRESS_LOOKBACK_MIN", "6"))
HT_MAX_MINUTE = int(os.environ.get("HT_MAX_MINUTE", "35"))
FT_MAX_MINUTE = int(os.environ.get("FT_MAX_MINUTE", "80"))

# pesos (positivos mais “caros” para aumentar seletividade)
HT_POS_WEIGHT = float(os.environ.get("HT_POS_WEIGHT", "5.0"))
FT_POS_WEIGHT = float(os.environ.get("FT_POS_WEIGHT", "7.0"))

# LightGBM hparams
LEARNING_RATE   = float(os.environ.get("LGBM_LR", "0.02"))
NUM_LEAVES      = int(os.environ.get("LGBM_LEAVES", "63"))
MIN_DATA_LEAF   = int(os.environ.get("LGBM_MIN_DATA_LEAF", "200"))
FEATURE_FRAC    = float(os.environ.get("LGBM_FEATURE_FRAC", "0.8"))
BAGGING_FRAC    = float(os.environ.get("LGBM_BAGGING_FRAC", "0.8"))
BAGGING_FREQ    = int(os.environ.get("LGBM_BAGGING_FREQ", "1"))
N_ROUNDS        = int(os.environ.get("LGBM_N_ROUNDS", "8000"))
ES_ROUNDS       = int(os.environ.get("LGBM_ES_ROUNDS", "200"))
VAL_FRACTION    = float(os.environ.get("VAL_FRACTION", "0.15"))
SEED            = int(os.environ.get("SEED", "42"))

random.seed(SEED)
np.random.seed(SEED)

# ====================== DB UTILS ============================
def connect():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def q_events(conn) -> List[str]:
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT event_id FROM ticks WHERE minute IS NOT NULL")
    return [r["event_id"] for r in cur.fetchall()]

def q_ticks(conn, event_id: str) -> List[sqlite3.Row]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT *
        FROM ticks
        WHERE event_id = ?
          AND minute IS NOT NULL
        ORDER BY minute ASC, ts ASC
        """,
        (event_id,)
    )
    return cur.fetchall()

# ====================== LABELS ==============================
def goal_between(rows: List[sqlite3.Row], start_min: int, end_min: int) -> int:
    """1 se houve incremento de gols no intervalo (start_min, end_min], senão 0."""
    g_start = None
    for r in reversed(rows):
        m = r["minute"] or 0
        if m <= start_min:
            g_start = (r["goals_home"] or 0) + (r["goals_away"] or 0)
            break
    if g_start is None:
        g_start = 0

    for r in rows:
        m = r["minute"] or 0
        if m <= start_min:
            continue
        if m > end_min:
            break
        g = (r["goals_home"] or 0) + (r["goals_away"] or 0)
        if g > g_start:
            return 1
    return 0

# ====================== FEATURES (compatível com calibrate_thresholds.py) ===
def _getf(row: sqlite3.Row, key: str) -> float:
    try:
        v = row[key]
        return float(v if v is not None else 0.0)
    except Exception:
        return 0.0

def build_feature_dict(rows: List[sqlite3.Row], minute: int) -> Dict[str, float]:
    if not rows:
        return {}

    m_from = max(0, minute - LOOKBACK_MIN)
    win = [r for r in rows if (r["minute"] or 0) >= m_from and (r["minute"] or 0) <= minute]
    if not win:
        last = None
        for r in reversed(rows):
            if (r["minute"] or 0) <= minute:
                last = r; break
        if last is None:
            return {}
        win = [last]
    last = win[-1]; base = win[0]

    def dpair(hkey: str, akey: str) -> Tuple[float, float]:
        dh = _getf(last, hkey) - _getf(base, hkey)
        da = _getf(last, akey) - _getf(base, akey)
        return dh, da

    d_sot_h, d_sot_a = dpair("sot_home", "sot_away")
    d_sof_h, d_sof_a = dpair("soff_home","soff_away")
    d_da_h,  d_da_a  = dpair("da_home",  "da_away")
    d_co_h,  d_co_a  = dpair("corners_home","corners_away")

    press_home = 3*d_sot_h + 1.5*d_sof_h + 0.5*d_da_h + 0.5*d_co_h
    press_away = 3*d_sot_a + 1.5*d_sof_a + 0.5*d_da_a + 0.5*d_co_a

    feat = {}
    feat["minute"] = float(minute)
    feat["goal_diff"] = _getf(last, "goals_home") - _getf(last, "goals_away")
    feat["press_home"] = float(press_home)
    feat["press_away"] = float(press_away)

    feat["d_sot_home"] = float(d_sot_h)
    feat["d_sot_away"] = float(d_sot_a)
    feat["d_soff_home"]= float(d_sof_h)
    feat["d_soff_away"]= float(d_sof_a)
    feat["d_corners_home"]= float(d_co_h)
    feat["d_corners_away"]= float(d_co_a)
    feat["d_da_home"]   = float(d_da_h)
    feat["d_da_away"]   = float(d_da_a)

    for k in ["st_home","st_away","sot_home","sot_away","soff_home","soff_away",
              "da_home","da_away","corners_home","corners_away","goals_home","goals_away"]:
        feat[f"cum_{k}"] = _getf(last, k)

    return feat

FEATURE_ORDER = [
    "minute","goal_diff","press_home","press_away",
    "d_sot_home","d_sot_away","d_soff_home","d_soff_away",
    "d_corners_home","d_corners_away","d_da_home","d_da_away",
    "cum_st_home","cum_st_away","cum_sot_home","cum_sot_away",
    "cum_soff_home","cum_soff_away","cum_da_home","cum_da_away",
    "cum_corners_home","cum_corners_away","cum_goals_home","cum_goals_away"
]

def to_vector(fdict: Dict[str, float], feature_names: List[str]) -> np.ndarray:
    return np.array([float(fdict.get(name, 0.0)) for name in feature_names], dtype=np.float32)

# ====================== DATASET BUILD =======================
def build_dataset(conn, target: str):
    """
    target: "HT" ou "FT"
    - HT: label = gol até 45'; usa minutos 0..HT_MAX_MINUTE
    - FT: label = gol até 90'; usa minutos 0..FT_MAX_MINUTE
    """
    events = q_events(conn)
    random.shuffle(events)

    X, y, gids = [], [], []  # gids = event_id por amostra
    n_events = len(events)

    for i, eid in enumerate(events, 1):
        rows = q_ticks(conn, eid)
        if not rows: continue

        minutes = sorted(set(int(r["minute"]) for r in rows if r["minute"] is not None))
        for m in minutes:
            if target == "HT" and m > HT_MAX_MINUTE: continue
            if target == "FT" and m > FT_MAX_MINUTE: continue

            fdict = build_feature_dict(rows, m)
            if not fdict: continue

            x = to_vector(fdict, FEATURE_ORDER)
            label = goal_between(rows, m, 45 if target=="HT" else 90)

            X.append(x)
            y.append(int(label))
            gids.append(eid)

        if i % 200 == 0:
            print(f"[train] {i}/{n_events}  {target}_samples={len(X)}")

    X = np.vstack(X) if X else np.zeros((0, len(FEATURE_ORDER)), dtype=np.float32)
    y = np.asarray(y, dtype=np.int8)
    gids = np.asarray(gids)
    return X, y, gids

# ====================== SPLIT (Group por jogo) ==============
def train_valid_split_by_game(gids: np.ndarray, val_fraction=0.15, seed=42):
    uniq = np.unique(gids)
    rng = np.random.default_rng(seed)
    rng.shuffle(uniq)
    n_val = max(1, int(len(uniq) * val_fraction))
    val_ids = set(uniq[:n_val])
    train_mask = np.array([g not in val_ids for g in gids], dtype=bool)
    valid_mask = ~train_mask
    return train_mask, valid_mask

# ====================== LIGHTGBM ============================
def make_params(pos_weight: float):
    return dict(
        objective="binary",
        boosting_type="gbdt",
        learning_rate=LEARNING_RATE,
        num_leaves=NUM_LEAVES,
        max_depth=-1,
        min_data_in_leaf=MIN_DATA_LEAF,
        feature_fraction=FEATURE_FRAC,
        bagging_fraction=BAGGING_FRAC,
        bagging_freq=BAGGING_FREQ,
        scale_pos_weight=pos_weight,
        metric=["auc","binary_logloss"],
        verbose=-1,
        seed=SEED
    )

def make_monotone_constraints(feature_names: List[str]) -> List[int]:
    cons = []
    for name in feature_names:
        if name in ("press_home","press_away",
                    "d_sot_home","d_sot_away","d_soff_home","d_soff_away",
                    "d_da_home","d_da_away","d_corners_home","d_corners_away"):
            cons.append(1)
        elif name.startswith("cum_"):
            cons.append(1)
        else:
            cons.append(0)
    return cons

def train_one(tag: str, X: np.ndarray, y: np.ndarray, gids: np.ndarray, pos_weight: float):
    if len(X) == 0:
        raise RuntimeError(f"Dataset vazio para {tag}")

    # split POR JOGO
    train_mask, valid_mask = train_valid_split_by_game(gids, VAL_FRACTION, SEED)
    X_tr, y_tr = X[train_mask], y[train_mask]
    X_va, y_va = X[valid_mask], y[valid_mask]

    dtr = lgb.Dataset(X_tr, label=y_tr, feature_name=FEATURE_ORDER)
    dva = lgb.Dataset(X_va, label=y_va, feature_name=FEATURE_ORDER, reference=dtr)

    params = make_params(pos_weight)
    params["monotone_constraints"] = make_monotone_constraints(FEATURE_ORDER)

    print(f"[train:{tag}] X_tr={X_tr.shape}  X_va={X_va.shape}  pos_weight={pos_weight}")
    # compat com versões antigas e novas do LightGBM
    try:
        booster = lgb.train(
            params,
            train_set=dtr,
            num_boost_round=N_ROUNDS,
            valid_sets=[dva],
            valid_names=["valid"],
            early_stopping_rounds=ES_ROUNDS,
            verbose_eval=200
        )
    except TypeError:
        booster = lgb.train(
            params,
            train_set=dtr,
            num_boost_round=N_ROUNDS,
            valid_sets=[dva],
            valid_names=["valid"],
            callbacks=[lgb.early_stopping(ES_ROUNDS), lgb.log_evaluation(200)]
        )

    best_iter = booster.best_iteration
    auc = booster.best_score["valid"]["auc"]
    try:
        from sklearn.metrics import average_precision_score
        p_va = booster.predict(X_va, num_iteration=best_iter)
        ap = average_precision_score(y_va, p_va)
        print(f"[train:{tag}] AUC={auc:.4f}  AP={ap:.4f}  best_iter={best_iter}")
    except Exception:
        print(f"[train:{tag}] AUC={auc:.4f}  best_iter={best_iter}")

    return booster

# ====================== MAIN ================================
def main():
    os.makedirs(MODELS_DIR, exist_ok=True)
    print(f"[train] salvando em: {MODELS_DIR}")
    print(f"[train] DB: {DB_PATH}")

    conn = connect()

    # monta datasets
    X_ht, y_ht, gid_ht = build_dataset(conn, "HT")
    X_ft, y_ft, gid_ft = build_dataset(conn, "FT")
    print(f"[train] final: HT {X_ht.shape}, FT {X_ft.shape}")

    # treinos
    bst_ht = train_one("HT", X_ht, y_ht, gid_ht, HT_POS_WEIGHT)
    bst_ft = train_one("FT", X_ft, y_ft, gid_ft, FT_POS_WEIGHT)

    # salva
    bst_ht.save_model(HT_MODEL, num_iteration=bst_ht.best_iteration)
    bst_ft.save_model(FT_MODEL, num_iteration=bst_ft.best_iteration)
    print(f"[train] modelos salvos: {os.path.basename(HT_MODEL)}, {os.path.basename(FT_MODEL)}")

    with open(FNAMES, "w", encoding="utf-8") as f:
        json.dump(FEATURE_ORDER, f, ensure_ascii=False, indent=2)
    print("[train] feature_names.json salvo.")

if __name__ == "__main__":
    main()
