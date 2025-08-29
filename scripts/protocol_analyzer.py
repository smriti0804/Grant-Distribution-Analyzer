
"""
merkl_disperse.py
Combined Merkl fetching + Disperse tracing logic in one file.
Also merges arb_beneficiaries.csv and merkl_beneficiaries.csv into combined_beneficiaries.csv

UPDATED: Fixed beneficiary reward totaling with proper deduplication and unique transaction tracking
UPDATED: Track return address separately and calculate returned amounts
UPDATED: Include total_amount_returned in JSON output
FIXED: Character encoding issues with Unicode symbols
FIXED: combine_beneficiaries function to properly handle file paths and data loading
FIXED: Combined file generation and totaling from combined data
FIXED: Normal intermediary beneficiaries now receive actual amounts sent to them, not scaled amounts
"""

from pymongo import MongoClient
from collections import defaultdict
from datetime import datetime
from decimal import Decimal, getcontext, InvalidOperation
import pandas as pd
import json
import requests
from web3 import Web3
import os
import sys
import hashlib
import csv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pymongo.errors import DuplicateKeyError


# Set UTF-8 encoding for stdout
if sys.stdout.encoding != 'utf-8':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# ------------------------
# CONFIG (edit as needed)
# ------------------------
DEBUG = True
MONGO_URL = os.getenv("MONGO_URI") or "mongodb+srv://urvibala:urvi2003@transaction-flow.k8id3qz.mongodb.net/"
DB_NAME = os.getenv("DB_NAME") or "arb_transactions"
DISPERSE_COLL = "disperse"

# Merkl-specific collections
CREATORS_COLLECTION = "creators"
CAMPAIGN_DATA_COLLECTION = "campaign_data"

# Merkl API / Merkl target
CHAIN_ID = 42161  # Arbitrum
TARGET_TO_ADDRESS = "0x3ef3d8ba38ebe18db133cec108f4d14ce00dd9ae".lower()
API_BASE = "https://api.merkl.xyz"

# Disperse/tracing config - UPDATED: Separate return address from stop addresses
RETURN_ADDRESS = "0x544cbe6698e2e3b676c76097305bba588defb13a".lower()
STOP_ADDRESS = (
    "0x435046800Fb9149eE65159721A92cB7d50a7534b".lower(),
    "0x3350bef226F7BdCA874C5561320aB7EF9DC89E70".lower()
)

ARB_RPC = "https://arb1.arbitrum.io/rpc"
ARBISCAN_API_KEY = os.getenv("ARBISCAN_API_KEY") or "3SPE5TKQ1FDQI6P1YH9KFX4Y5ZSYHRWP5V"
ALCHEMY_KEY = os.getenv("ALCHEMY_KEY") or "alcht_LOKXTf0YhlAvVVJEkLn1tlWuG2ODOD"
ARB_ARB_TOKEN = Web3.to_checksum_address("0x912CE59144191C1204E64559FE8253a0e49E6548")

# CSV path fallback logic
MERKL_CSV_PATH = os.path.join(os.getcwd(), "merkl_beneficiaries.csv")
ARB_CSV_PATH = os.path.join(os.getcwd(), "arb_beneficiaries.csv")
COMBINED_CSV_PATH = os.path.join(os.getcwd(), "combined_beneficiaries.csv")
COMBINED_INTER_CSV_PATH = os.path.join(os.getcwd(), "combined_intermediaries.csv")
COMBINED_RETURNED_CSV_PATH = os.path.join(os.getcwd(), "combined_returned.csv")

# Precision
getcontext().prec = 50

# ------------------------
# SETUP: Web3, Mongo
# ------------------------
w3 = Web3(Web3.HTTPProvider(ARB_RPC))
client = MongoClient(MONGO_URL)
db = client[DB_NAME]

# Collections
disperse_coll = db[DISPERSE_COLL]
creators_col = db[CREATORS_COLLECTION]
campaign_data_col = db[CAMPAIGN_DATA_COLLECTION]

# ------------------------
# Dynamic collection selection for disperse tracing
# ------------------------
selected_transactions_coll = None
selected_transactions_coll_name = None

# Flag to indicate whether Merkl export was performed in this run
merkl_export_performed = False
# Flag to indicate whether Disperse export was performed in this run
disperse_export_performed = False


def select_transactions_collection_for_protocol(protocol_addr: str):
    """
    Find a collection that contains at least one document where 'from' equals the normalized protocol address.
    Returns the collection name if found, else None.
    """
    addr = _normalize_address(protocol_addr)
    try:
        collection_names = [n for n in db.list_collection_names() if not n.startswith("system.")]
    except Exception:
        return None

    # Scan all collections, skipping known non-transaction ones
    skip = {DISPERSE_COLL, CREATORS_COLLECTION, CAMPAIGN_DATA_COLLECTION}
    for name in collection_names:
        if name in skip:
            continue
        try:
            coll = db[name]
            if coll.find_one({"from": addr}, projection={"_id": 1}):
                return name
        except Exception:
            continue
    return None


def set_transactions_collection_by_protocol(protocol_addr: str):
    """Set global selected_transactions_coll based on protocol address presence in 'from'."""
    global selected_transactions_coll, selected_transactions_coll_name
    chosen = select_transactions_collection_for_protocol(protocol_addr)
    if chosen:
        selected_transactions_coll = db[chosen]
        selected_transactions_coll_name = chosen
    else:
        raise ValueError(f"No collection found containing transactions from protocol address: {protocol_addr}")


# ------------------------
# HTTP session helper
# ------------------------
def _build_session():
    retry_strategy = Retry(
        total=5,
        backoff_factor=1.0,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS", "GET"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    s = requests.Session()
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


session = _build_session()


# ------------------------
# Utility helpers
# ------------------------
def parse_iso(ts: str):
    if not ts:
        return None
    if isinstance(ts, (int, float)) or (isinstance(ts, str) and ts.isdigit()):
        try:
            return datetime.utcfromtimestamp(int(ts))
        except Exception:
            pass
    if isinstance(ts, str) and ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(ts)
    except Exception:
        return None


def sum_values(txs):
    return sum(Decimal(str(tx.get("value", 0) or "0")) for tx in txs if tx)


def _normalize_address(addr: str) -> str:
    return (addr or "").lower()


# ------------------------
# Disperse-specific helpers
# ------------------------
def get_verified_source(address: str):
    url = f"https://api.arbiscan.io/api?module=contract&action=getsourcecode&address={address}&apikey={ARBISCAN_API_KEY}"
    try:
        resp = requests.get(url, timeout=20).json()
        if resp.get("status") != "1":
            return None
        return resp["result"][0]
    except Exception:
        return None


def resolve_eip1967_implementation(proxy_addr: str):
    slot = int("0x360894A13BA1A3210667C828492DB98DCA3E2076CC3735A920A3CA505D382BBC", 16)
    try:
        raw = w3.eth.get_storage_at(proxy_addr, slot)
        impl_addr = Web3.to_checksum_address("0x" + raw[-20:].hex())
        if int(impl_addr, 16) != 0 and w3.eth.get_code(impl_addr) != b"":
            return impl_addr
    except Exception:
        pass
    return None


def is_disperse_contract(address: str) -> bool:
    try:
        checksum = Web3.to_checksum_address(address)
    except Exception:
        return False

    if w3.eth.get_code(checksum) == b"":
        return False

    impl_addr = resolve_eip1967_implementation(checksum)
    target_addr = impl_addr or checksum
    src_info = get_verified_source(target_addr)

    if not src_info:
        return False

    if "disperse" in (src_info.get("ContractName") or "").lower():
        return True

    try:
        abi = json.loads(src_info.get("ABI") or "[]")
    except json.JSONDecodeError:
        return False

    funcs = {f.get("name") for f in abi if f.get("type") == "function"}
    return bool({"disperseEther", "disperseToken", "disperseTokenSimple"} & funcs)


def get_transactions(address, incoming=True):
    address = (address or "").lower()
    key = "to" if incoming else "from"
    cursor = selected_transactions_coll.find({key: address})
    txs = []
    for tx in cursor:
        txs.append({
            **tx,
            "from": (tx.get("from") or "").lower(),
            "to": (tx.get("to") or "").lower(),
            "txHash": (tx.get("txHash") or "").lower(),
        })
    return txs

# ===== Core Traversal (trace_flow) - FIXED to track actual amounts received =====
# ===== Core Traversal (trace_flow) - FIXED to track actual amounts received =====
def trace_flow(address, parent_incoming_amt=None, visited=None, is_initial=False, 
            intermediaries=None, beneficiaries=None,
            processed_disperse_payouts=None, amount_received_map=None, 
            recorded_edges=None, beneficiary_unique_txs=None, returned_amounts=None,
            actual_amount_received=None):
    
    visited = visited or set()
    beneficiaries = beneficiaries or defaultdict(Decimal)
    amount_received_map = amount_received_map or {}
    intermediaries = intermediaries or defaultdict(Decimal)
    processed_disperse_payouts = processed_disperse_payouts or set()
    recorded_edges = recorded_edges or set()
    beneficiary_unique_txs = beneficiary_unique_txs or defaultdict(set)
    returned_amounts = returned_amounts or defaultdict(Decimal)

    addr_l = (address or "").lower()
    if not addr_l or addr_l in STOP_ADDRESS:
        return beneficiaries, intermediaries, amount_received_map, beneficiary_unique_txs, returned_amounts

    # UPDATED: Handle return address specially - record amount but don't continue tracing
    if addr_l == RETURN_ADDRESS:
        # Use actual amount received, not parent incoming amount
        actual_amt = actual_amount_received or Decimal("0")
        returned_amounts[addr_l] += actual_amt
        # FIXED: Set amount_received_map for return address
        amount_received_map[addr_l] = actual_amt
        if DEBUG:
            print(f"[DEBUG] Return address {addr_l} received: {actual_amt}")
        return beneficiaries, intermediaries, amount_received_map, beneficiary_unique_txs, returned_amounts

    # Root
    if is_initial:
        outgoing_all = get_transactions(addr_l, incoming=False)
        for tx in sorted(outgoing_all, key=lambda x: parse_iso(x.get("timeStamp") or "") or datetime.min):
            to_addr = tx.get("to")
            if not to_addr or to_addr in visited:
                continue
            amt_sent = Decimal(str(tx.get("value", 0) or "0"))
            edge_key = (addr_l, to_addr)
            # FIXED: Set amount_received_map correctly for initial transactions
            amount_received_map[to_addr] = amt_sent
            recorded_edges.add(edge_key)
            visited.add(to_addr)

            sub_ben, intermediaries, amount_received_map, beneficiary_unique_txs, returned_amounts = trace_flow(
                to_addr, amt_sent, visited, False, intermediaries, beneficiaries, 
                processed_disperse_payouts, amount_received_map, recorded_edges, beneficiary_unique_txs, returned_amounts,
                actual_amount_received=amt_sent  # Pass the actual amount sent
            )
            for k, v in sub_ben.items():
                # Only add if not already set (Disperse block sets directly)
                if k not in beneficiaries or beneficiaries[k] == 0:
                    beneficiaries[k] += v

        return beneficiaries, intermediaries, amount_received_map, beneficiary_unique_txs, returned_amounts

    # Non-root
    incoming_txs = get_transactions(addr_l, incoming=True)
    
    # FIXED: Set amount_received_map at the beginning for all non-initial addresses
    actual_amt = actual_amount_received or Decimal("0")
    amount_received_map[addr_l] = actual_amt
    if not incoming_txs:
        # Use actual amount received for final beneficiaries
        beneficiaries[addr_l] += actual_amt
        if DEBUG:
            print(f"[DEBUG] Final beneficiary {addr_l} received actual amount: {actual_amt}")
        return beneficiaries, intermediaries, amount_received_map, beneficiary_unique_txs, returned_amounts

    # incoming_amt_by_hash = defaultdict(Decimal)
    # for tx in incoming_txs:
    #     txh = (tx.get("txHash") or "").lower()
    #     if txh:
    #         incoming_amt_by_hash[txh] += Decimal(str(tx.get("value", 0) or "0"))

    earliest_dt = parse_iso(
        sorted(incoming_txs, key=lambda x: parse_iso(x.get("timeStamp") or "") or datetime.min)[0].get("timeStamp") or ""
    )
    total_incoming_amt = sum_values(incoming_txs)
    outgoing_all = get_transactions(addr_l, incoming=False)

    # Disperse contract - process ALL outgoing transactions without balance limiting
    if is_disperse_contract(addr_l):
        outgoing_after_sorted = sorted(outgoing_all, key=lambda x: parse_iso(x.get("timeStamp") or "") or datetime.min)
        incoming_hashes_lower = {tx.get("txHash") for tx in incoming_txs if tx.get("txHash")}

        matched_rows = []
        if incoming_hashes_lower:
            cursor = disperse_coll.find({
                "from": addr_l,
                "txHash": {"$in": list(incoming_hashes_lower)}
            })
            for row in cursor:
                matched_rows.append({
                    **row,
                    "from": (row.get("from") or "").lower(),
                    "to": (row.get("to") or "").lower(),
                    "txHash": (row.get("txHash") or "").lower(),
                    "value": str(row.get("value", 0) or "0"),
                    "timeStamp": row.get("timeStamp") or row.get("timestamp") or None
                })

        if DEBUG:
            print(f"[DEBUG] Disperse matched rows for {addr_l}: {len(matched_rows)} rows")

        # ENHANCED DEDUPLICATION LOGIC
        seen_records = set()
        deduplicated_rows = []
        
        for row in matched_rows:
            rec_addr = row.get("to")
            if not rec_addr:
                continue
            
            txh_db = row.get("txHash")
            timestamp_str = str(row.get("timeStamp") or "")
            value_str = str(row.get("value", 0) or "0")
            
            # Create a comprehensive unique identifier
            record_id = f"{txh_db}|{rec_addr}|{value_str}|{timestamp_str}"
            
            if record_id not in seen_records:
                seen_records.add(record_id)
                deduplicated_rows.append(row)

        if DEBUG:
            print(f"[DEBUG] Original matched rows: {len(matched_rows)}")
            print(f"[DEBUG] After exact duplicate removal: {len(deduplicated_rows)}")

        # Step 2: Process all recipients including return address
        beneficiary_sums = defaultdict(Decimal)
        return_sums = defaultdict(Decimal)
        transaction_details = defaultdict(list)
        
        for row in deduplicated_rows:
            rec_addr = row.get("to")
            txh_db = row.get("txHash")
            timestamp_str = str(row.get("timeStamp") or "")
            value_str = str(row.get("value", 0) or "0")
            
            try:
                raw_amt = Decimal(value_str)
            except Exception:
                raw_amt = Decimal("0")
                
            # Only count non-zero amounts
            if raw_amt > 0:
                transaction_details[rec_addr].append({
                    'txHash': txh_db,
                    'timestamp': timestamp_str,
                    'amount': str(raw_amt)
                })
                
                # Track this unique transaction for the recipient
                if txh_db and timestamp_str:
                    tx_signature = f"{txh_db}_{timestamp_str}_{value_str}"
                    beneficiary_unique_txs[rec_addr].add(tx_signature)
                
                # UPDATED: Separate return address from beneficiaries
                if rec_addr == RETURN_ADDRESS:
                    return_sums[rec_addr] += raw_amt
                    if DEBUG:
                        print(f"[DEBUG] Return address {rec_addr} received {raw_amt} from disperse")
                elif rec_addr not in STOP_ADDRESS:
                    beneficiary_sums[rec_addr] += raw_amt

        if DEBUG:
            print(f"[DEBUG] Beneficiaries with non-zero amounts: {len(beneficiary_sums)}")
            print(f"[DEBUG] Return address amounts: {dict(return_sums)}")

        # Step 3: Update dictionaries - FIXED amount_received_map for disperse recipients
        for rec_addr, total_amt in beneficiary_sums.items():
            if rec_addr in beneficiaries and beneficiaries[rec_addr] > 0:
                if DEBUG:
                    print(f"[DEBUG] WARNING: {rec_addr} already has {beneficiaries[rec_addr]} ARB, setting to {total_amt} ARB")
                beneficiaries[rec_addr] = total_amt
            else:
                beneficiaries[rec_addr] = total_amt
            # FIXED: Set amount_received_map for all disperse beneficiaries
            amount_received_map[rec_addr] = total_amt

        # Update returned amounts and amount_received_map for return addresses
        for rec_addr, total_amt in return_sums.items():
            returned_amounts[rec_addr] += total_amt
            amount_received_map[rec_addr] = total_amt

        total_dispersed = sum(beneficiary_sums.values()) + sum(return_sums.values())
        intermediaries[addr_l] += total_dispersed

        if DEBUG:
            print(f"[DEBUG] Total dispersed amount: {total_dispersed}")
            print(f"[DEBUG] Beneficiaries found: {len(beneficiary_sums)}")
            print(f"[DEBUG] Total returned: {sum(return_sums.values())}")

        return beneficiaries, intermediaries, amount_received_map, beneficiary_unique_txs, returned_amounts

    # Normal intermediary - Apply balance limiting logic for non-disperse contracts
    outgoing_after = [
        tx for tx in outgoing_all 
        if (parse_iso(tx.get("timeStamp") or "") or datetime.min) >= earliest_dt
    ] if earliest_dt else outgoing_all
    
    outgoing_after_sorted = sorted(outgoing_after, key=lambda x: parse_iso(x.get("timeStamp") or "") or datetime.min)

    # For non-disperse contracts, keep the balance limiting logic
    limited_outgoing = []
    cumulative = Decimal("0")
    for tx in outgoing_after_sorted:
        amt = Decimal(str(tx.get("value", 0) or "0"))
        if cumulative + amt <= total_incoming_amt:
            limited_outgoing.append(tx)
            cumulative += amt
        else:
            break

    total_outgoing_amt = sum_values(limited_outgoing)
    unspent = total_incoming_amt - total_outgoing_amt

    # New logic for intermediary/beneficiary classification
    unique_outgoing_addresses = set()
    for tx in limited_outgoing:
        to_addr = tx.get("to")
        if to_addr:
            unique_outgoing_addresses.add(to_addr)

    is_intermediary = False
    if total_outgoing_amt > Decimal("500") or len(unique_outgoing_addresses) > 4:
        is_intermediary = True

    if is_intermediary:
        intermediaries[addr_l] += total_incoming_amt
        
        for tx in limited_outgoing:
            to_addr = tx.get("to")
            if not to_addr or to_addr in visited:
                continue

            raw_amt_sent = Decimal(str(tx.get("value", 0) or "0"))
            # FIXED: Don't overwrite amount_received_map here - let the recursive call handle it
            visited.add(to_addr)

            # FIXED: Pass raw_amt_sent as both parent_incoming_amt AND actual_amount_received
            # This ensures beneficiaries get the actual amount sent to them
            sub_ben, intermediaries, amount_received_map, beneficiary_unique_txs, returned_amounts = trace_flow(
                to_addr, raw_amt_sent, visited, False, intermediaries, beneficiaries, 
                processed_disperse_payouts, amount_received_map, recorded_edges, beneficiary_unique_txs, returned_amounts,
                actual_amount_received=raw_amt_sent  # Pass the actual amount sent
            )
            for k, v in sub_ben.items():
                beneficiaries[k] += v
    else:
        # FIXED: Use actual amount received for final beneficiaries
        beneficiaries[addr_l] += actual_amt
        if DEBUG:
            print(f"[DEBUG] Final beneficiary (non-intermediary) {addr_l} received actual amount: {actual_amt}")

    return beneficiaries, intermediaries, amount_received_map, beneficiary_unique_txs, returned_amounts
# Helper: detect if protocol address has any outgoing to a Disperse contract
def protocol_has_disperse_destination(protocol_addr: str) -> bool:
    """
    Check the selected transactions collection for any outgoing txs from protocol_addr
    whose 'to' address is a Disperse contract.
    """
    if selected_transactions_coll is None:
        return False

    addr_l = _normalize_address(protocol_addr)
    try:
        cursor = selected_transactions_coll.find({"from": addr_l}, projection={"to": 1})
    except Exception:
        return False

    seen = set()
    for doc in cursor:
        to_addr = _normalize_address(doc.get("to"))
        if not to_addr or to_addr in seen:
            continue
        seen.add(to_addr)
        try:
            if is_disperse_contract(to_addr):
                return True
        except Exception:
            continue
    return False


# ----- find_beneficiaries wrapper - UPDATED to handle returned amounts =====
def find_beneficiaries(protocol_recipient, out_prefix="arb"):
    # Ensure we select the correct transactions collection dynamically for this protocol
    set_transactions_collection_by_protocol(protocol_recipient)
    
    if DEBUG:
        print(f"[DEBUG] Using transactions collection: {selected_transactions_coll_name}")

    beneficiaries, intermediaries, amount_received_map, beneficiary_unique_txs, returned_amounts = trace_flow(
        (protocol_recipient or "").lower(), is_initial=True)

    if DEBUG:
        print(f"[DEBUG] BEFORE removing intermediaries - Total beneficiaries: {len(beneficiaries)}")
        print(f"[DEBUG] Total returned amounts: {sum(returned_amounts.values())}")
        total_before = sum(beneficiaries.values())
        print(f"[DEBUG] BEFORE removing intermediaries - Total amount: {total_before}")

    # Remove intermediaries from beneficiaries
    removed_intermediaries = []
    for addr in list(beneficiaries):
        if addr in intermediaries:
            removed_amount = beneficiaries[addr]
            removed_intermediaries.append((addr, removed_amount))
            del beneficiaries[addr]
    
    if DEBUG and removed_intermediaries:
        print(f"[DEBUG] Removed {len(removed_intermediaries)} intermediaries from beneficiaries:")
        for addr, amt in removed_intermediaries[:3]:  # Show first 3
            print(f"[DEBUG]   {addr}: {amt}")

    if DEBUG:
        print(f"[DEBUG] AFTER removing intermediaries - Total beneficiaries: {len(beneficiaries)}")
        total_after = sum(beneficiaries.values())
        print(f"[DEBUG] AFTER removing intermediaries - Total amount: {total_after}")

    # Create beneficiaries DataFrame - Save to ARB_CSV_PATH for later combination
    ben_data = []
    for addr in beneficiaries:
        amount = beneficiaries[addr]
        ben_data.append((addr, str(amount)))
    
    ben_df = pd.DataFrame(ben_data, columns=["beneficiary_address", "amount_received"])
    
    if DEBUG:
        print(f"[DEBUG] DataFrame created with {len(ben_df)} rows")
        print(f"[DEBUG] DataFrame total amount: {sum(Decimal(x) for x in ben_df['amount_received'])}")
    
    ben_df["sort_key"] = ben_df["amount_received"].apply(lambda v: Decimal(v))
    ben_df = ben_df.sort_values(by="sort_key", ascending=False).drop(columns=["sort_key"])

    # Save to ARB CSV for later combination
    ben_df.to_csv(ARB_CSV_PATH, index=False)

    # Create intermediaries CSV
    pd.DataFrame(
        sorted(((k, str(v)) for k, v in intermediaries.items()), 
            key=lambda x: Decimal(x[1]), reverse=True),
        columns=["intermediary_address", "amount_received"]
    ).to_csv("arb_intermediaries.csv", index=False)

    # Create returned amounts CSV
    if returned_amounts:
        returned_data = [(addr, str(amt)) for addr, amt in returned_amounts.items()]
        returned_df = pd.DataFrame(returned_data, columns=["return_address", "amount_returned"])
        returned_df.to_csv("arb_returned.csv", index=False)
        print(f"Returned amounts CSV: arb_returned.csv")
        
        if DEBUG:
            total_returned = sum(returned_amounts.values())
            print(f"[DEBUG] Total returned amount: {total_returned}")

    print(f"Beneficiaries: {len(beneficiaries)} | Intermediaries: {len(intermediaries)} | Returned: {sum(returned_amounts.values())}")
    print(f"ARB CSV outputs: {ARB_CSV_PATH}, arb_intermediaries.csv")

    global disperse_export_performed
    disperse_export_performed = True
    return beneficiaries, intermediaries, returned_amounts


# ------------------------
# Merkl-specific helpers (unchanged)
# ------------------------
def to_units_18(value):
    try:
        return float(Decimal(str(value)) / Decimal("1e18"))
    except (InvalidOperation, TypeError):
        return None


def _compute_row_id(campaign_id: str, recipient: str, reason: str, reward_token: str, amount_wei: str) -> str:
    base = f"{campaign_id}|{_normalize_address(recipient)}|{reason or ''}|{_normalize_address(reward_token)}|{amount_wei}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()


def ensure_indexes():
    try:
        campaign_data_col.create_index([("rowId", 1)], unique=True)
    except Exception:
        pass
    try:
        campaign_data_col.create_index([("campaignId", 1)])
        campaign_data_col.create_index([("recipient", 1)])
    except Exception:
        pass


def fetch_creator_addresses():
    """
    Get 'from' addresses (checksum-case) where 'to' == TARGET_TO_ADDRESS from the 
    already identified transactions collection. Only proceed if the target address 
    is present in that collection.
    """
    global selected_transactions_coll, selected_transactions_coll_name
    
    if selected_transactions_coll is None or not selected_transactions_coll_name:
        print("[INFO] No selected transactions collection; skipping creator discovery")
        return []

    try:
        if not db[selected_transactions_coll_name].find_one({"to": TARGET_TO_ADDRESS}, projection={"_id": 1}):
            print("[INFO] Target address {} not found in '{}'; skipping Merkl fetch".format(
                TARGET_TO_ADDRESS, selected_transactions_coll_name))
            return []
    except Exception as e:
        print("[ERROR] Error checking target address presence in '{}': {}".format(
            selected_transactions_coll_name, e))
        return []

    print("[INFO] Looking for creator addresses in '{}' where to={}...".format(
        selected_transactions_coll_name, TARGET_TO_ADDRESS))

    pipeline = [
        {"$match": {"to": TARGET_TO_ADDRESS}},
        {"$group": {"_id": "$from"}}
    ]
    results = list(db[selected_transactions_coll_name].aggregate(pipeline))

    creators = []
    for r in results:
        raw_addr = r.get("_id")
        if raw_addr:
            # Convert to checksum case (Merkl API requirements)
            try:
                checksum_addr = Web3.to_checksum_address(raw_addr)
                creators.append(checksum_addr)
            except Exception as e:
                print("[WARN] Error checksumming addr {}: {}".format(raw_addr, e))

    print("[INFO] Found {} creator addresses".format(len(creators)))
    if DEBUG:
        print(creators)
    return creators


def fetch_campaigns(creator_address, refresh: bool = False):
    # store normalized address in DB for indexing
    existing = list(creators_col.find({"creatorAddress": _normalize_address(creator_address)}))
    if existing and not refresh:
        print("Found {} campaigns in DB for creator {}".format(len(existing), creator_address))
        return existing

    print("Fetching campaigns from Merkl API for creator {} ...".format(creator_address))
    url = f"{API_BASE}/v4/creators/{creator_address}/campaigns"
    try:
        response = session.get(url, timeout=30)
        response.raise_for_status()
        campaigns = response.json()
    except requests.exceptions.RequestException as err:
        print("[WARN] Error fetching campaigns: {}".format(err))
        return existing

    for c in campaigns:
        doc = {
            "creatorAddress": _normalize_address(creator_address),
            "campaignId": c.get("campaignId"),
            "amount": to_units_18(c.get("amount")),
            "createdAt": c.get("createdAt"),
            "endTimestamp": c.get("endTimestamp"),
            "creator_tags": c.get("creator", {}).get("tags", []),
        }
        creators_col.update_one({"campaignId": doc["campaignId"]}, {"$set": doc}, upsert=True)

    print("Stored {} campaigns in DB".format(len(campaigns)))
    return campaigns


def fetch_recipients(campaign_id, refresh: bool = False):
    if not refresh:
        existing = list(campaign_data_col.find({"campaignId": campaign_id}))
        if existing:
            print("Found {} recipients in DB for campaign {}".format(len(existing), campaign_id))
            return existing

    print("Fetching recipients from Merkl API for campaign {} ...".format(campaign_id))
    url = f"{API_BASE}/v3/recipients?chainId={CHAIN_ID}&campaignId={campaign_id}"
    try:
        response = session.get(url, timeout=30)
        response.raise_for_status()
        recipients = response.json()
    except requests.exceptions.RequestException as err:
        print("[ERROR] Could not fetch recipients for {}: {}".format(campaign_id, err))
        return list(campaign_data_col.find({"campaignId": campaign_id}))

    if refresh:
        campaign_data_col.delete_many({"campaignId": campaign_id})

    for r in recipients:
        recipient = r.get("recipient")
        reason = r.get("reason")
        reward_token = r.get("rewardToken")
        amount_wei = str(r.get("amount")) if r.get("amount") is not None else "0"

        doc = {
            "campaignId": campaign_id,
            "recipient": _normalize_address(recipient),
            "reason": reason,
            "rewardToken": _normalize_address(reward_token),
            "amount": to_units_18(amount_wei),
            "amountWei": amount_wei,
            "rowId": _compute_row_id(campaign_id, recipient, reason, reward_token, amount_wei),
        }
        try:
            campaign_data_col.insert_one(doc)
        except DuplicateKeyError:
            pass

    print("Stored {} recipients in DB for campaign {}".format(len(recipients), campaign_id))
    return recipients


def verify_data_integrity(creator_address):
    print("\nVERIFYING DATA INTEGRITY...")
    campaigns = fetch_campaigns(creator_address)
    api_total_rows = 0
    for c in campaigns:
        campaign_id = c.get("campaignId")
        url = f"{API_BASE}/v3/recipients?chainId={CHAIN_ID}&campaignId={campaign_id}"
        try:
            response = session.get(url, timeout=30)
            response.raise_for_status()
            recipients = response.json()
            api_total_rows += len(recipients)
        except:
            continue

    stored_rows = campaign_data_col.count_documents({})
    print("API Total Rows: {}".format(api_total_rows))
    print("MongoDB Stored Rows: {}".format(stored_rows))
    if api_total_rows == stored_rows:
        print("PERFECT! All data preserved - no duplicates lost")
    else:
        print("Data mismatch: {} rows lost".format(api_total_rows - stored_rows))
    return api_total_rows, stored_rows


def export_to_csv(selected_campaign_ids=None):
    """
    Export Merkl beneficiary data to MERKL_CSV_PATH with columns:
    beneficiary_address, amount_received
    If selected_campaign_ids is provided, only include recipients from those campaigns.
    """
    global merkl_export_performed
    print("\n[INFO] Exporting data to {} ...".format(MERKL_CSV_PATH))

    query = {}
    if selected_campaign_ids:
        query = {"campaignId": {"$in": list(selected_campaign_ids)}}

    recipients = list(campaign_data_col.find(query))
    if not recipients:
        print("[WARN] No recipient data found to export")
        return

    beneficiary_totals = {}
    for recipient in recipients:
        beneficiary_addr = recipient.get("recipient", "")
        amount = recipient.get("amount", 0)
        if beneficiary_addr and amount is not None:
            if beneficiary_addr in beneficiary_totals:
                beneficiary_totals[beneficiary_addr] += amount
            else:
                beneficiary_totals[beneficiary_addr] = amount

    # ensure parent dir exists
    parent_dir = os.path.dirname(MERKL_CSV_PATH)
    if parent_dir and not os.path.isdir(parent_dir):
        try:
            os.makedirs(parent_dir, exist_ok=True)
        except Exception:
            pass

    with open(MERKL_CSV_PATH, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['beneficiary_address', 'amount_received'])
        for beneficiary_addr, total_amount in beneficiary_totals.items():
            writer.writerow([beneficiary_addr, total_amount])

    print("[INFO] Exported {} unique beneficiaries to {}".format(len(beneficiary_totals), MERKL_CSV_PATH))
    merkl_export_performed = True
    
    try:
        total_sum = sum(beneficiary_totals.values())
        print("[INFO] Total amount distributed: {:.6f}".format(total_sum))
    except Exception:
        pass


def clear_existing_data():
    print("[INFO] Clearing existing campaign data...")
    deleted_count = campaign_data_col.delete_many({})
    print("[INFO] Deleted {} existing records".format(deleted_count.deleted_count))
    return deleted_count.deleted_count


def combine_beneficiaries(arb_csv=None, merkl_csv=None, out_csv=None, out_inter_csv=None, out_returned_csv=None):
    """
    FIXED: Combine arb_beneficiaries.csv and merkl_beneficiaries.csv.
    If a beneficiary address appears in both, add its amount_received.
    The address 0x3ef3d8ba38ebe18db133cec108f4d14ce00dd9ae is always treated as an intermediary, not a beneficiary.
    """
    print("\n[INFO] Combining beneficiaries from ARB + Merkl CSVs...")

    # Use default paths if not provided
    arb_csv = arb_csv or ARB_CSV_PATH
    merkl_csv = merkl_csv or MERKL_CSV_PATH
    out_csv = out_csv or COMBINED_CSV_PATH
    out_inter_csv = out_inter_csv or COMBINED_INTER_CSV_PATH
    out_returned_csv = out_returned_csv or COMBINED_RETURNED_CSV_PATH
    
    combined = defaultdict(Decimal)
    combined_intermediaries = defaultdict(Decimal)
    combined_returned = defaultdict(Decimal)
    
    special_intermediary = "0x3ef3d8ba38ebe18db133cec108f4d14ce00dd9ae"
    
    files_loaded = []

    def load_csv(path, file_type):
        if not os.path.exists(path):
            print(f"[INFO] File not found: {path}")
            return False
        
        try:
            print(f"[INFO] Loading {file_type} file: {path}")
            df = pd.read_csv(path)
            
            # Check if required columns exist
            required_cols = ["beneficiary_address", "amount_received"]
            if not all(col in df.columns for col in required_cols):
                print(f"[WARN] {path} missing required columns {required_cols}")
                print(f"[WARN] Available columns: {list(df.columns)}")
                return False
            
            loaded_count = 0
            for _, row in df.iterrows():
                addr_raw = row.get("beneficiary_address", "")
                if pd.isna(addr_raw) or addr_raw == "":
                    continue
                    
                addr = str(addr_raw).strip().lower()
                
                try:
                    amt_raw = row.get("amount_received", "0")
                    if pd.isna(amt_raw):
                        amt = Decimal("0")
                    else:
                        amt = Decimal(str(amt_raw))
                except (InvalidOperation, ValueError) as e:
                    print(f"[WARN] Invalid amount '{amt_raw}' for address {addr}: {e}")
                    amt = Decimal("0")
                
                if amt > 0:  # Only process non-zero amounts
                    if addr == special_intermediary:
                        combined_intermediaries[addr] += amt
                        if DEBUG:
                            print(f"[DEBUG] Special intermediary {addr}: +{amt} = {combined_intermediaries[addr]}")
                    else:
                        combined[addr] += amt
                    loaded_count += 1
            
            print(f"[INFO] Loaded {loaded_count} records from {file_type} file")
            return True
            
        except Exception as e:
            print(f"[ERROR] Error loading {path}: {e}")
            return False

    def load_intermediaries_csv(path, file_type):
        if not os.path.exists(path):
            print(f"[INFO] Intermediaries file not found: {path}")
            return False
        
        try:
            print(f"[INFO] Loading {file_type} intermediaries file: {path}")
            df = pd.read_csv(path)
            
            # Check for intermediary columns
            if "intermediary_address" in df.columns and "amount_received" in df.columns:
                for _, row in df.iterrows():
                    addr_raw = row.get("intermediary_address", "")
                    if pd.isna(addr_raw) or addr_raw == "":
                        continue
                        
                    addr = str(addr_raw).strip().lower()
                    
                    try:
                        amt_raw = row.get("amount_received", "0")
                        if pd.isna(amt_raw):
                            amt = Decimal("0")
                        else:
                            amt = Decimal(str(amt_raw))
                    except (InvalidOperation, ValueError):
                        amt = Decimal("0")
                    
                    if amt > 0:
                        combined_intermediaries[addr] += amt
                        
                print(f"[INFO] Loaded intermediaries from {file_type}")
                return True
            else:
                print(f"[INFO] No intermediary columns found in {path}")
                return False
                
        except Exception as e:
            print(f"[ERROR] Error loading intermediaries from {path}: {e}")
            return False

    def load_returned_csv(path, file_type):
        if not os.path.exists(path):
            print(f"[INFO] Returned amounts file not found: {path}")
            return False
        
        try:
            print(f"[INFO] Loading {file_type} returned amounts file: {path}")
            df = pd.read_csv(path)
            
            # Check for return columns
            if "return_address" in df.columns and "amount_returned" in df.columns:
                for _, row in df.iterrows():
                    addr_raw = row.get("return_address", "")
                    if pd.isna(addr_raw) or addr_raw == "":
                        continue
                        
                    addr = str(addr_raw).strip().lower()
                    
                    try:
                        amt_raw = row.get("amount_returned", "0")
                        if pd.isna(amt_raw):
                            amt = Decimal("0")
                        else:
                            amt = Decimal(str(amt_raw))
                    except (InvalidOperation, ValueError):
                        amt = Decimal("0")
                    
                    if amt > 0:
                        combined_returned[addr] += amt
                        
                print(f"[INFO] Loaded returned amounts from {file_type}")
                return True
            else:
                print(f"[INFO] No return columns found in {path}")
                return False
                
        except Exception as e:
            print(f"[ERROR] Error loading returned amounts from {path}: {e}")
            return False

    # Load files based on what was performed in this session and what exists
    if disperse_export_performed:
        if load_csv(arb_csv, "ARB"):
            files_loaded.append("ARB")
        # Load intermediaries
        load_intermediaries_csv("arb_intermediaries.csv", "ARB")
        # Load returned amounts
        load_returned_csv("arb_returned.csv", "ARB")
    else:
        print("[INFO] Skipping ARB CSV - Disperse tracing did not run in this session")

    if merkl_export_performed:
        if load_csv(merkl_csv, "Merkl"):
            files_loaded.append("Merkl")
    else:
        print("[INFO] Skipping Merkl CSV - Merkl export did not run in this session")
    
    if not files_loaded:
        print("[WARN] No files were successfully loaded. Cannot combine beneficiaries.")
        # Create empty files
        pd.DataFrame(columns=["beneficiary_address", "amount_received"]).to_csv(out_csv, index=False)
        pd.DataFrame(columns=["intermediary_address", "amount_received"]).to_csv(out_inter_csv, index=False)
        pd.DataFrame(columns=["return_address", "amount_returned"]).to_csv(out_returned_csv, index=False)
        return None

    print(f"[INFO] Successfully loaded data from: {', '.join(files_loaded)}")

    # Write combined beneficiaries (excluding special intermediary)
    if combined:
        rows = [(addr, str(amt)) for addr, amt in combined.items() if amt > 0]
        df_out = pd.DataFrame(rows, columns=["beneficiary_address", "amount_received"])
        
        # Sort by amount (highest first)
        df_out["sort_key"] = df_out["amount_received"].apply(lambda v: Decimal(str(v)))
        df_out = df_out.sort_values(by="sort_key", ascending=False).drop(columns=["sort_key"])
        
        df_out.to_csv(out_csv, index=False)
        print(f"[INFO] Combined {len(combined)} unique beneficiaries into {out_csv}")
    else:
        # Create empty file
        pd.DataFrame(columns=["beneficiary_address", "amount_received"]).to_csv(out_csv, index=False)
        print(f"[INFO] No beneficiaries found. Created empty file: {out_csv}")

    # Write combined intermediaries
    if combined_intermediaries:
        inter_rows = [(addr, str(amt)) for addr, amt in combined_intermediaries.items() if amt > 0]
        df_inter = pd.DataFrame(inter_rows, columns=["intermediary_address", "amount_received"])
        
        # Sort by amount (highest first)
        df_inter["sort_key"] = df_inter["amount_received"].apply(lambda v: Decimal(str(v)))
        df_inter = df_inter.sort_values(by="sort_key", ascending=False).drop(columns=["sort_key"])
        
        df_inter.to_csv(out_inter_csv, index=False)
        print(f"[INFO] Combined {len(combined_intermediaries)} intermediaries into {out_inter_csv}")
    else:
        # Create empty file
        pd.DataFrame(columns=["intermediary_address", "amount_received"]).to_csv(out_inter_csv, index=False)
        print(f"[INFO] No intermediaries found. Created empty file: {out_inter_csv}")

    # Write combined returned amounts
    if combined_returned:
        returned_rows = [(addr, str(amt)) for addr, amt in combined_returned.items() if amt > 0]
        df_returned = pd.DataFrame(returned_rows, columns=["return_address", "amount_returned"])
        
        # Sort by amount (highest first)
        df_returned["sort_key"] = df_returned["amount_returned"].apply(lambda v: Decimal(str(v)))
        df_returned = df_returned.sort_values(by="sort_key", ascending=False).drop(columns=["sort_key"])
        
        df_returned.to_csv(out_returned_csv, index=False)
        print(f"[INFO] Combined {len(combined_returned)} returned amounts into {out_returned_csv}")
    else:
        # Create empty file
        pd.DataFrame(columns=["return_address", "amount_returned"]).to_csv(out_returned_csv, index=False)
        print(f"[INFO] No returned amounts found. Created empty file: {out_returned_csv}")

    # Calculate and display totals
    try:
        total_beneficiaries = sum(combined.values()) if combined else Decimal("0")
        total_intermediaries = sum(combined_intermediaries.values()) if combined_intermediaries else Decimal("0")
        total_returned = sum(combined_returned.values()) if combined_returned else Decimal("0")
        grand_total = total_beneficiaries + total_intermediaries + total_returned
        
        print(f"\n[INFO] === COMBINED TOTALS ===")
        print(f"[INFO] Total beneficiaries amount: {total_beneficiaries:.6f}")
        print(f"[INFO] Total intermediaries amount: {total_intermediaries:.6f}")
        print(f"[INFO] Total returned amount: {total_returned:.6f}")
        print(f"[INFO] Grand total: {grand_total:.6f}")
    except Exception as e:
        print(f"[ERROR] Error calculating totals: {e}")

    return pd.read_csv(out_csv) if os.path.exists(out_csv) else None


# Programmatic API - UPDATED to read from combined files
def compute_beneficiaries_for_protocol(protocol_addr: str) -> dict:
    """
    Runs the merkl (if applicable) and disperse/normal tracing flows for the given 
    protocol address, and returns a dictionary with beneficiaries, intermediaries, 
    and returned amounts data from the COMBINED files.
    """
    global merkl_export_performed, disperse_export_performed
    merkl_export_performed = False
    disperse_export_performed = False

    ensure_indexes()
    set_transactions_collection_by_protocol(protocol_addr)

    # Merkl flow (only if TARGET_TO_ADDRESS exists in selected collection)
    creator_addresses = fetch_creator_addresses()
    if creator_addresses:
        selected_campaign_ids = set()
        for creator in creator_addresses:
            campaigns = fetch_campaigns(creator, refresh=False)
            for c in campaigns:
                cid = c.get("campaignId")
                if cid:
                    selected_campaign_ids.add(cid)
                    fetch_recipients(cid, refresh=False)
        export_to_csv(selected_campaign_ids)

    # Disperse/normal tracing always runs
    find_beneficiaries(protocol_addr, out_prefix="arb")

    # Combine the data from both sources
    combine_beneficiaries()

    # UPDATED: Read from combined files instead of using in-memory data
    beneficiaries_list = []
    intermediaries_list = []
    returned_list = []

    # Read combined beneficiaries
    try:
        if os.path.exists(COMBINED_CSV_PATH):
            df_ben = pd.read_csv(COMBINED_CSV_PATH)
            for _, row in df_ben.iterrows():
                addr = row.get("beneficiary_address", "")
                amount = row.get("amount_received", "0")
                if addr and amount:
                    beneficiaries_list.append({
                        "beneficiary_address": str(addr),
                        "amount_received": str(amount)
                    })
    except Exception as e:
        print(f"[ERROR] Error reading combined beneficiaries: {e}")

    # Read combined intermediaries
    try:
        if os.path.exists(COMBINED_INTER_CSV_PATH):
            df_inter = pd.read_csv(COMBINED_INTER_CSV_PATH)
            for _, row in df_inter.iterrows():
                addr = row.get("intermediary_address", "")
                amount = row.get("amount_received", "0")
                if addr and amount:
                    intermediaries_list.append({
                        "intermediary_address": str(addr),
                        "amount_received": str(amount)
                    })
    except Exception as e:
        print(f"[ERROR] Error reading combined intermediaries: {e}")

    # Read combined returned amounts
    try:
        if os.path.exists(COMBINED_RETURNED_CSV_PATH):
            df_returned = pd.read_csv(COMBINED_RETURNED_CSV_PATH)
            for _, row in df_returned.iterrows():
                addr = row.get("return_address", "")
                amount = row.get("amount_returned", "0")
                if addr and amount:
                    returned_list.append({
                        "return_address": str(addr),
                        "amount_returned": str(amount)
                    })
    except Exception as e:
        print(f"[ERROR] Error reading combined returned amounts: {e}")

    # Calculate totals from combined data
    total_distributed = "0"
    total_intermediaries_amount = "0"
    total_returned = "0"
    
    try:
        total_distributed = str(sum(
            Decimal(b["amount_received"]) 
            for b in beneficiaries_list 
            if b["amount_received"] not in ["", "nan", "NaN", "None"]
        ))
    except Exception:
        total_distributed = "0"
    
    try:
        total_intermediaries_amount = str(sum(
            Decimal(i["amount_received"]) 
            for i in intermediaries_list 
            if i["amount_received"] not in ["", "nan", "NaN", "None"]
        ))
    except Exception:
        total_intermediaries_amount = "0"
    
    try:
        total_returned = str(sum(
            Decimal(r["amount_returned"]) 
            for r in returned_list 
            if r["amount_returned"] not in ["", "nan", "NaN", "None"]
        ))
    except Exception:
        total_returned = "0"

    return {
        "beneficiaries": beneficiaries_list,
        "intermediaries": intermediaries_list,
        "returned_amounts": returned_list,
        "summary": {
            "total_beneficiaries": len(beneficiaries_list),
            "total_intermediaries": len(intermediaries_list),
            "total_returned_addresses": len(returned_list),
            "total_amount_distributed": total_distributed,
            "total_intermediaries_amount": total_intermediaries_amount,
            "total_amount_returned": total_returned
        }
    }


# ------------------------
# MAIN
# ------------------------
def main(argv):
    do_clear = "--clear" in argv
    do_refresh = "--refresh" in argv
    run_merkl = "--merkl" in argv
    run_disperse = "--disperse" in argv

    if not run_merkl and not run_disperse:
        run_merkl = True
        run_disperse = True

    ensure_indexes()

    if do_clear:
        clear_existing_data()

    protocol_addr = None
    for i, arg in enumerate(argv):
        if arg == "--protocol" and i + 1 < len(argv):
            protocol_addr = argv[i + 1]
            break

    if not protocol_addr:
        print("Usage: python merkl_disperse.py --protocol <address>")
        return

    try:
        set_transactions_collection_by_protocol(protocol_addr)
        if DEBUG:
            print(f"[DEBUG] Using transactions collection (global): {selected_transactions_coll_name}")
    except Exception as e:
        print(f"[ERROR] Could not select transactions collection for {protocol_addr}: {e}")
        return

    if run_merkl:
        print("\n===== RUNNING MERKL FLOW =====")
        creator_addresses = fetch_creator_addresses()
        if creator_addresses:
            selected_campaign_ids = set()
            for creator in creator_addresses:
                campaigns = fetch_campaigns(creator, refresh=do_refresh)
                for c in campaigns:
                    cid = c.get("campaignId")
                    if cid:
                        selected_campaign_ids.add(cid)
                        fetch_recipients(cid, refresh=do_refresh)
            export_to_csv(selected_campaign_ids)

    if run_disperse:
        if protocol_has_disperse_destination(protocol_addr):
            print("\n===== RUNNING DISPERSE TRACING =====")
        else:
            print("\n===== RUNNING NORMAL INTERMEDIARY TRACING =====")
        find_beneficiaries(protocol_addr, out_prefix="arb")

    # Always combine at the end
    combine_beneficiaries()
    print("\n[INFO] All selected flows completed.")


if __name__ == "__main__":
    main(sys.argv[1:])
# return {"status": "ok", "received": data}

# if __name__ == "__main__":
#     raw = sys.argv[1] if len(sys.argv) > 1 else "{}"
#     data = json.loads(raw)
#     result = analyze(data)
#     print(json.dumps(result))