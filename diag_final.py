"""
Find the resampling method matching TV ground truth:
  VALE3: NO signal
  BBAS3: NO signal
  ITUB4: HAS signal (OS crossover)
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from mfi_engine import _compute_mfi, _find_crossover_signal, MFI_LENGTH, HISTORY_DAYS
import yfinance as yf
import pandas as pd

GROUND_TRUTH = {"VALE3.SA": False, "BBAS3.SA": False, "ITUB4.SA": True}

def resample_calendar(df, n, off=0):
    dates = df.index.tz_localize(None)
    bids = (dates.map(lambda d: d.toordinal()) - 719163 + off) // n
    g = df.copy(); g['_b'] = bids
    r = g.groupby('_b').agg({'Open':'first','High':'max','Low':'min','Close':'last','Volume':'sum'})
    r.index = g.groupby('_b').apply(lambda x: x.index[-1], include_groups=False).values
    return r

def resample_trading(df, n, off=0):
    d = df.iloc[off:].copy()
    d['_b'] = [i // n for i in range(len(d))]
    r = d.groupby('_b').agg({'Open':'first','High':'max','Low':'min','Close':'last','Volume':'sum'})
    r.index = d.groupby('_b').apply(lambda x: x.index[-1], include_groups=False).values
    return r

print("Fetching...")
hists = {}
for t in GROUND_TRUTH:
    hists[t] = yf.Ticker(t).history(period=f"{HISTORY_DAYS}d")
    print(f"  {t}: {len(hists[t])} bars")

def test_method(name, func):
    print(f"\n--- {name} ---")
    for off in range(8):
        results = {}
        for t, expect_sig in GROUND_TRUTH.items():
            r = func(hists[t], 8, off)
            mfi = _compute_mfi(r, MFI_LENGTH)
            sig = _find_crossover_signal(mfi)
            got_sig = sig is not None
            correct = (got_sig == expect_sig)
            mfi_val = mfi.dropna().iloc[-1] if not mfi.dropna().empty else 0
            results[t] = {"correct": correct, "got": got_sig, "expect": expect_sig, "mfi": mfi_val, "sig": sig}
        
        all_correct = all(r["correct"] for r in results.values())
        mark = "✅ ALL MATCH" if all_correct else "❌"
        n_correct = sum(1 for r in results.values() if r["correct"])
        
        details = []
        for t, r in results.items():
            sym = t.replace('.SA','')
            sig_info = ""
            if r["sig"]:
                sig_info = f" [{r['sig']['signal_type']} {str(r['sig']['signal_date'])[:10]}]"
            chk = "✓" if r["correct"] else "✗"
            details.append(f"{sym}:{r['mfi']:.1f}{'(SIG)' if r['got'] else ''}{sig_info} {chk}")
        
        print(f"  off={off}: {mark} ({n_correct}/3) | {' | '.join(details)}")

test_method("CALENDAR DAYS", resample_calendar)
test_method("TRADING DAYS", resample_trading)
