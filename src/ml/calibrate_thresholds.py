# src/ml/calibrate_thresholds.py
import os, json, sqlite3, random, time
from typing import List, Dict, Tuple, Optional
import numpy as np
import lightgbm as lgb

# ---------- paths / params ----------
ROOT       = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH    = os.environ.get("DB_PATH",    os.path.join(ROOT, "..", "data", "events.db"))
MODELS_DIR = os.environ.get("MODELS_DIR", os.path.join(ROOT, "..", "models"))

HT_MODEL   = os.path.join(MODELS_DIR, "ht_lgbm.txt")
FT_MODEL   = os.path.join(MODELS_DIR, "ft_lgbm.txt")
FNAMES     = os.path.join(MODELS_DIR, "feature_names.json")
OUT_THRESH = os.path.join(MODELS_DIR, "thresholds.json")

LOOKBACK_MIN   = int(os.environ.get("PRESS_LOOKBACK_MIN", "6"))
HT_MAX_MINUTE  = 35
FT_MAX_MINUTE  = 80
COOLDOWN_MIN   = int(os.environ.get("COOLDOWN_MIN", "3"))

# payoffs (ganho líquido quando acerta / perda quando erra)
HT_WIN_UNIT        = float(os.environ.get("HT_WIN_UNIT", "0.2"))
FT_WIN_UNIT_PRE50  = float(os.environ.get("FT_WIN_UNIT_PRE50", "0.1"))
FT_WIN_UNIT_POST50 = float(os.environ.get("FT_WIN_UNIT_POST50", "0.2"))
LOSE_UNIT          = float(os.environ.get("LOSE_UNIT", "1.0"))

# grade de thresholds
GRID = [round(x, 3) for x in np.arange(0.55, 0.62, 0.01)]

# mínimos de cobertura
MIN_SIGNS_HT        = int(os.environ.get("MIN_SIGNS_HT", "300"))
MIN_SIGNS_FT_PRE50  = int(os.environ.get("MIN_SIGNS_FT_PRE50", "300"))
MIN_SIGNS_FT_POST50 = int(os.environ.get("MIN_SIGNS_FT_POST50", "300"))

# logs de progresso
CALIB_VERBOSE = int(os.environ.get("CALIB_VERBOSE", "0"))
LOG_EVERY     = int(os.environ.get("LOG_EVERY", "200"))

# ---------- DB ----------
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
    cur.execute("""
        SELECT *
        FROM ticks
        WHERE event_id = ?
          AND minute IS NOT NULL
        ORDER BY minute ASC, ts ASC
    """, (event_id,))
    return cur.fetchall()

# ---------- features ----------
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
        if last is None: return {}
        win = [last]
    last, base = win[-1], win[0]

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

def to_vector(fdict: Dict[str, float], feature_names: List[str]) -> np.ndarray:
    return np.array([float(fdict.get(name, 0.0)) for name in feature_names], dtype=np.float32)

def get_feature_names(bst: lgb.Booster) -> List[str]:
    try:
        with open(FNAMES, "r", encoding="utf-8") as f:
            names = json.load(f)
        if isinstance(names, list) and len(names) == len(bst.feature_name()) and len(names) > 0:
            return names
    except Exception:
        pass
    names = list(bst.feature_name())
    os.makedirs(MODELS_DIR, exist_ok=True)
    with open(FNAMES, "w", encoding="utf-8") as f:
        json.dump(names, f, ensure_ascii=False, indent=2)
    print(f"[cal] feature_names.json divergente; reconstruído com {len(names)} features.")
    return names

# ---------- gols ----------
def goal_minutes(rows: List[sqlite3.Row]) -> List[int]:
    mins = []
    prev = None
    for r in rows:
        m = int(r["minute"] or 0)
        g = int((_getf(r,"goals_home") + _getf(r,"goals_away")))
        if prev is None:
            prev = g
            continue
        if g > prev:
            mins.append(m)
            prev = g
        else:
            prev = g
    return sorted(set(mins))

def first_goal_after(goal_mins: List[int], start_min: int) -> Optional[int]:
    for gm in goal_mins:
        if gm > start_min:
            return gm
    return None

# ---------- simulação ----------
def simulate_match_signals(
    rows: List[sqlite3.Row],
    bst: lgb.Booster,
    FEATURE_NAMES: List[str],
    signal_type: str,            # "HT" | "FT_PRE" | "FT_POST"
    thr: float
) -> Tuple[int,int,float]:
    if thr is None:
        return (0,0,0.0)

    gm = goal_minutes(rows)
    minutes = sorted(set(int(r["minute"]) for r in rows if r["minute"] is not None))
    if not minutes:
        return (0,0,0.0)

    open_flag = False
    open_min  = None
    settle_min = None
    win_unit  = 0.0
    n = 0
    hits = 0
    pnl = 0.0
    cooldown_until = -10**9

    if signal_type == "HT":
        max_open_min = HT_MAX_MINUTE
        hard_limit   = 45
    elif signal_type == "FT_PRE":
        max_open_min = min(50-1, FT_MAX_MINUTE)
        hard_limit   = 90
    elif signal_type == "FT_POST":
        max_open_min = FT_MAX_MINUTE
        hard_limit   = 90
    else:
        raise ValueError("signal_type inválido")

    for m in minutes:
        # fechar quando atingir settle_min
        if open_flag and settle_min is not None and m >= settle_min:
            if win_unit > 0:
                pnl += win_unit
                hits += 1
                cooldown_until = max(cooldown_until, settle_min + COOLDOWN_MIN)
            else:
                pnl -= LOSE_UNIT
            open_flag = False
            open_min = None
            settle_min = None
            win_unit = 0.0

        # tentar abrir
        if (not open_flag) and (m >= cooldown_until) and (m <= max_open_min):
            if signal_type == "FT_PRE" and m >= 50:
                pass
            elif signal_type == "FT_POST" and m < 50:
                pass
            else:
                fdict = build_feature_dict(rows, m)
                x = to_vector(fdict, FEATURE_NAMES)
                p = float(bst.predict(x[None, :], predict_disable_shape_check=True)[0])
                if p >= thr:
                    open_flag = True
                    open_min = m
                    n += 1
                    g_after = first_goal_after(gm, open_min)
                    if signal_type == "HT":
                        if g_after is not None and g_after <= 45:
                            settle_min = g_after
                            win_unit = HT_WIN_UNIT
                        else:
                            settle_min = 45
                            win_unit = 0.0
                    else:
                        if g_after is not None and g_after <= 90:
                            settle_min = g_after
                            win_unit = (FT_WIN_UNIT_PRE50 if open_min < 50 else FT_WIN_UNIT_POST50)
                        else:
                            settle_min = 90
                            win_unit = 0.0

    # fim do jogo
    if open_flag and settle_min is not None:
        if win_unit > 0:
            pnl += win_unit
            hits += 1
        else:
            pnl -= LOSE_UNIT

    return (n, hits, float(pnl))

def simulate_dataset(
    events: List[str],
    conn,
    bst: lgb.Booster,
    FEATURE_NAMES: List[str],
    signal_type: str,
    thr: float,
    verbose: int = 0,
    log_every: int = 200
) -> Tuple[int,int,float]:
    n_tot = 0
    hits_tot = 0
    pnl_tot = 0.0
    t0 = time.time()
    total = len(events)
    for i, eid in enumerate(events, 1):
        rows = q_ticks(conn, eid)
        if rows:
            n, h, p = simulate_match_signals(rows, bst, FEATURE_NAMES, signal_type, thr)
            n_tot += n
            hits_tot += h
            pnl_tot  += p
        if verbose and (i % log_every == 0 or i == total):
            acc = (hits_tot / n_tot * 100) if n_tot > 0 else 0.0
            speed = i / max(1e-9, (time.time()-t0))
            print(f"[cal:{signal_type} thr={thr:.2f}] {i}/{total} games  entries={n_tot}  hit={acc:.1f}%  pnl={pnl_tot:.1f}u  ({speed:.1f} g/s)")
    return (n_tot, hits_tot, pnl_tot)

# ---------- main ----------
def main():
    if not (os.path.exists(HT_MODEL) and os.path.exists(FT_MODEL)):
        raise RuntimeError(f"Modelos não encontrados em {MODELS_DIR}")

    bst_ht = lgb.Booster(model_file=HT_MODEL)
    bst_ft = lgb.Booster(model_file=FT_MODEL)

    # features
    try:
        with open(FNAMES, "r", encoding="utf-8") as f:
            FEATURE_NAMES = json.load(f)
        if not isinstance(FEATURE_NAMES, list) or len(FEATURE_NAMES) != len(bst_ht.feature_name()):
            raise ValueError("feature_names.json divergente")
    except Exception:
        FEATURE_NAMES = list(bst_ht.feature_name())
        os.makedirs(MODELS_DIR, exist_ok=True)
        with open(FNAMES, "w", encoding="utf-8") as f:
            json.dump(FEATURE_NAMES, f, ensure_ascii=False, indent=2)
        print(f"[cal] feature_names.json divergente; reconstruído com {len(FEATURE_NAMES)} features.")

    print(f"[cal] usando DB: {DB_PATH}")
    print(f"[cal] models: ht_lgbm.txt(n_feat={len(bst_ht.feature_name())}), ft_lgbm.txt(n_feat={len(bst_ft.feature_name())})")
    print(f"[cal] features: {len(FEATURE_NAMES)}  verbose={CALIB_VERBOSE} log_every={LOG_EVERY}")

    conn = connect()
    events = q_events(conn)
    random.shuffle(events)

    def best_for(signal_type: str, min_sigs: int) -> Dict[str, float]:
        best = {"thr": None, "pnl": -1e18, "n": 0, "hits": 0}
        model = bst_ht if signal_type == "HT" else bst_ft
        t0 = time.time()
        for thr in GRID:
            n, h, p = simulate_dataset(
                events, conn, model, FEATURE_NAMES, signal_type, thr,
                verbose=CALIB_VERBOSE, log_every=LOG_EVERY
            )
            if n >= min_sigs and p > best["pnl"]:
                best = {"thr": float(thr), "pnl": float(p), "n": int(n), "hits": int(h)}
        dt = int(time.time()-t0)
        if best["thr"] is None:
            print(f"[cal] {signal_type}: sem threshold ≥ min_sinais ({min_sigs})")
        else:
            acc = (best["hits"]/best["n"]*100) if best["n"]>0 else 0.0
            roi = (best["pnl"]/best["n"]) if best["n"]>0 else 0.0
            print(f"[cal] {signal_type}: thr={best['thr']:.3f} pnl={best['pnl']:.1f}u n={best['n']} hit={acc:.1f}% pnl/entrada={roi:.3f}u (em {dt}s)")
        return best

    best_ht   = best_for("HT",      MIN_SIGNS_HT)
    best_pre  = best_for("FT_PRE",  MIN_SIGNS_FT_PRE50)
    best_post = best_for("FT_POST", MIN_SIGNS_FT_POST50)

    os.makedirs(MODELS_DIR, exist_ok=True)
    with open(OUT_THRESH, "w", encoding="utf-8") as f:
        json.dump({
            "feature_count": len(FEATURE_NAMES),
            "cooldown_min": COOLDOWN_MIN,
            "ht":  {"threshold": best_ht.get("thr"),  "pnl": best_ht.get("pnl"),
                    "n": best_ht.get("n"), "hits": best_ht.get("hits"),
                    "max_minute": HT_MAX_MINUTE, "win_unit": HT_WIN_UNIT, "lose_unit": LOSE_UNIT},
            "ft_pre50": {"threshold": best_pre.get("thr"), "pnl": best_pre.get("pnl"),
                         "n": best_pre.get("n"), "hits": best_pre.get("hits"),
                         "max_minute": FT_MAX_MINUTE, "win_unit": FT_WIN_UNIT_PRE50, "lose_unit": LOSE_UNIT},
            "ft_post50": {"threshold": best_post.get("thr"), "pnl": best_post.get("pnl"),
                          "n": best_post.get("n"), "hits": best_post.get("hits"),
                          "max_minute": FT_MAX_MINUTE, "win_unit": FT_WIN_UNIT_POST50, "lose_unit": LOSE_UNIT}
        }, f, ensure_ascii=False, indent=2)
    print(f"[cal] thresholds salvos em {OUT_THRESH}")

if __name__ == "__main__":
    main()
