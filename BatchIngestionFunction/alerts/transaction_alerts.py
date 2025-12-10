# alerts/transaction_alerts.py
from datetime import timedelta
from dateutil import parser as date_parser

HIGH_VALUE_THRESHOLD = 50000.0
VELOCITY_WINDOW_MINUTES = 2
VELOCITY_TXN_COUNT = 10


def fraud_detection(parsed_rows):
    """
    Apply all transaction fraud rules:
      1. High-value fraud
      2. Velocity attacks
      3. Geo-location switching
      4. Balance drain
    Returns list of alert dicts.
    """
    alerts = []

    # --------------------------------------
    # RULE 1: HIGH VALUE FRAUD
    # --------------------------------------
    for r in parsed_rows:
        try:
            amt = float(r.get("Amount") or 0)
            if amt >= HIGH_VALUE_THRESHOLD:
                alerts.append({
                    "alert_id": f"ALERT_HIGHVALUE_{r.get('TransactionID')}",
                    "type": "HIGH_VALUE",
                    "reason": f"Transaction amount {amt} exceeds threshold {HIGH_VALUE_THRESHOLD}",
                    "transaction": r
                })
        except:
            pass

    # --------------------------------------
    # RULE 2: VELOCITY FRAUD (Many txns in short time)
    # --------------------------------------
    by_customer = {}
    for r in parsed_rows:
        cid = r.get("CustomerID") or r.get("AccountNumber") or "UNKNOWN"
        try:
            ts = date_parser.parse(r.get("Timestamp"))
        except:
            continue
        by_customer.setdefault(cid, []).append((ts, r))

    window = timedelta(minutes=VELOCITY_WINDOW_MINUTES)

    for cid, items in by_customer.items():
        items.sort(key=lambda x: x[0])

        for i in range(len(items)):
            j = i + 1
            count = 1
            while j < len(items) and (items[j][0] - items[i][0]) <= window:
                count += 1
                j += 1

            if count >= VELOCITY_TXN_COUNT:
                group_txns = [it[1] for it in items[i:j]]
                alerts.append({
                    "alert_id": f"ALERT_VELOCITY_{cid}_{items[i][0].isoformat()}",
                    "type": "VELOCITY_ATTACK",
                    "reason": f"{count} transactions within {VELOCITY_WINDOW_MINUTES} minutes",
                    "transactions": group_txns
                })

    # --------------------------------------
    # RULE 3: GEO LOCATION SWITCHING FRAUD
    # --------------------------------------
    for cid, items in by_customer.items():
        loc_map = {}
        for ts, r in items:
            loc = r.get("Location")
            loc_map.setdefault(loc, []).append(ts)

        timestamps = []
        for loc, ts_list in loc_map.items():
            for t in ts_list:
                timestamps.append((loc, t))

        timestamps.sort(key=lambda x: x[1])

        for i in range(len(timestamps)):
            loc1, t1 = timestamps[i]
            for j in range(i + 1, len(timestamps)):
                loc2, t2 = timestamps[j]
                if loc1 != loc2 and (t2 - t1) <= timedelta(minutes=10):
                    alerts.append({
                        "alert_id": f"ALERT_GEO_{cid}_{t1.isoformat()}",
                        "type": "GEO_LOCATION_SWITCH",
                        "reason": f"Transaction from {loc1} → {loc2} within 10 minutes",
                        "transactions": [timestamps[i], timestamps[j]]
                    })

    # --------------------------------------
    # RULE 4: BALANCE DRAIN (Many withdrawals fast)
    # --------------------------------------
    for cid, items in by_customer.items():
        total_amt = 0
        items_sorted = sorted(items, key=lambda x: x[0])
        start = items_sorted[0][0]

        for ts, r in items_sorted:
            try:
                amt = float(r.get("Amount") or 0)
            except:
                amt = 0

            total_amt += amt

            if (ts - start) <= timedelta(minutes=10) and total_amt >= 100000:
                alerts.append({
                    "alert_id": f"ALERT_BALANCE_DRAIN_{cid}_{ts.isoformat()}",
                    "type": "BALANCE_DRAIN",
                    "reason": f"Total withdrawals {total_amt} in 10 minutes",
                    "transactions": [x[1] for x in items_sorted]
                })
                break

    return alerts
