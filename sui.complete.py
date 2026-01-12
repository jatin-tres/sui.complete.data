import streamlit as st
import pandas as pd
import requests
import time
from datetime import datetime, timezone

# --- CONFIGURATION ---
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Content-Type": "application/json"
}

RPC_NODES = [
    "https://fullnode.mainnet.sui.io:443",
    "https://sui-rpc.publicnode.com",
    "https://sui-mainnet.nodeinfra.com:443",
    "https://mainnet.sui.rpcpool.com:443",
    "https://rpc.mainnet.sui.io:443"
]

def make_rpc_call(method, params):
    for node in RPC_NODES:
        payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
        try:
            response = requests.post(node, json=payload, headers=HEADERS, timeout=20)
            if response.status_code == 200:
                data = response.json()
                if "result" in data:
                    return data["result"]
        except Exception:
            continue 
    return None

def get_validator_map():
    validator_map = {}
    try:
        result = make_rpc_call("suix_getLatestSuiSystemStateV2", [])
        if result:
            for v in result.get('activeValidators', []):
                validator_map[v['suiAddress'].lower()] = v['name']
    except:
        pass
    return validator_map

def format_amount(mist_amount):
    """
    Default SUI/Move Decimals = 9. 
    (Note: Some tokens like USDC use 6, but without metadata calls we assume 9 for standardizing).
    """
    if mist_amount is None: return 0.0
    return float(mist_amount) / 1_000_000_000

def parse_token_name(coin_type):
    """
    Extracts 'BLUB' from '0x...::blub::BLUB'
    """
    if not coin_type or coin_type == "0x2::sui::SUI":
        return "SUI"
    try:
        # Split by '::' and take the last part
        return coin_type.split("::")[-1]
    except:
        return "Unknown Token"

def parse_transaction(tx_data, validator_map, target_keyword):
    if not tx_data:
        return {"Type": "Network Error"}

    # 1. TIMESTAMP
    ts_str = "Unknown"
    if 'timestampMs' in tx_data:
        dt = datetime.fromtimestamp(int(tx_data['timestampMs']) / 1000, tz=timezone.utc)
        ts_str = dt.strftime('%d.%m.%Y UTC %H:%M')

    # 2. SENDER
    sender = tx_data.get('transaction', {}).get('data', {}).get('sender', 'Unknown')

    # 3. GAS FEE (Always in SUI)
    gas_used = tx_data.get('effects', {}).get('gasUsed', {})
    comp = int(gas_used.get('computationCost', 0))
    stor = int(gas_used.get('storageCost', 0))
    rebate = int(gas_used.get('storageRebate', 0))
    gas_fee_sui = format_amount(comp + stor - rebate)

    # 4. CORE LOGIC
    tx_type = "Unknown"
    main_amount = 0.0
    token_name = "SUI"  # Default
    recipient = "N/A"
    target_amount = "N/A" 

    events = tx_data.get('events', [])
    balance_changes = tx_data.get('balanceChanges', [])
    
    is_staking_action = False
    
    # --- A. CHECK EVENTS (Staking is always SUI) ---
    for event in events:
        e_type = event.get('type', '')
        parsed = event.get('parsedJson', {})

        # Stake
        if "RequestAddStake" in e_type or "StakingRequest" in e_type:
            tx_type = "Stake"
            is_staking_action = True
            token_name = "SUI"
            
            amount_mist = float(parsed.get('amount', 0))
            sui_val = -format_amount(amount_mist)
            
            val_addr = parsed.get('validator_address', '').lower()
            val_name = validator_map.get(val_addr, "Unknown Validator")
            
            if "0xa36a" in val_addr and val_name == "Unknown Validator":
                val_name = "Nansen (Detected)"

            recipient = val_name
            main_amount = sui_val
            
            if target_keyword.lower() in val_name.lower():
                target_amount = sui_val
            break
        
        # Unstake
        elif "Withdraw" in e_type or "Unstake" in e_type or "UnstakingRequest" in e_type:
            tx_type = "Unstake"
            is_staking_action = True
            token_name = "SUI"
            
            p = float(parsed.get('principal_amount', 0))
            r = float(parsed.get('reward_amount', 0))
            if p == 0 and r == 0: p = float(parsed.get('amount', 0))
                
            main_amount = format_amount(p + r)
            recipient = "N/A"
            break

    # --- B. BALANCE CHANGES (For Send/Receive of ANY Token) ---
    if not is_staking_action:
        # We need to find the "Main" asset that moved.
        # Priority: Non-SUI tokens first (since SUI is also used for gas), then SUI.
        
        primary_change = None
        
        # 1. Look for Non-SUI changes for the Sender
        for change in balance_changes:
            owner = change.get('owner', {})
            addr = owner.get('AddressOwner', '')
            coin_type = change.get('coinType', '0x2::sui::SUI')
            
            if addr == sender and coin_type != "0x2::sui::SUI":
                primary_change = change
                break
        
        # 2. If no Non-SUI change, look for SUI change
        if not primary_change:
            for change in balance_changes:
                owner = change.get('owner', {})
                addr = owner.get('AddressOwner', '')
                if addr == sender:
                    primary_change = change
                    break
        
        # 3. Analyze the found change
        if primary_change:
            raw_amount = float(primary_change.get('amount', 0))
            coin_type = primary_change.get('coinType', '0x2::sui::SUI')
            token_name = parse_token_name(coin_type)
            
            # If SUI, we must add gas back to find the true transfer amount
            if token_name == "SUI":
                # Convert gas fee back to MIST for accurate addition
                gas_mist = (comp + stor - rebate)
                net_change = raw_amount + gas_mist
            else:
                # If it's a Token (BLUB), gas doesn't affect the balance (gas is paid in SUI)
                net_change = raw_amount
            
            # Determine Send vs Receive
            if net_change < 0:
                tx_type = "Send"
                main_amount = format_amount(net_change) # Negative
                
                # Find Recipient (Someone who got THIS token)
                for change in balance_changes:
                    owner = change.get('owner', {})
                    addr = owner.get('AddressOwner', '')
                    c_type = change.get('coinType', '')
                    if addr != sender and float(change.get('amount', 0)) > 0 and c_type == coin_type:
                        recipient = addr
                        break
                        
            elif net_change > 0:
                tx_type = "Receive"
                main_amount = format_amount(net_change) # Positive
                
            else:
                tx_type = "Contract Call"
                main_amount = 0.0

    return {
        "Type": tx_type,
        "Amount": main_amount,
        "Token": token_name,    # NEW COLUMN
        "Target Amount": target_amount, 
        "Timestamp": ts_str,
        "Sender": sender,
        "Recipient": recipient,
        "Gas Fees": gas_fee_sui
    }

def fetch_batch_transactions(hashes):
    params = [hashes, {"showEvents": True, "showBalanceChanges": True, "showInput": True, "showEffects": True}]
    return make_rpc_call("sui_multiGetTransactionBlocks", params)

def fetch_single_transaction(tx_hash):
    params = [tx_hash, {"showEvents": True, "showBalanceChanges": True, "showInput": True, "showEffects": True}]
    return make_rpc_call("sui_getTransactionBlock", params)

# --- UI ---
st.set_page_config(page_title="Sui Complete Data Analyzer", page_icon="⚡", layout="wide")
st.title("⚡ Sui Complete Data Analyzer")

if 'v_map' not in st.session_state:
    with st.spinner("Loading Validator Phonebook..."):
        st.session_state['v_map'] = get_validator_map()
    
if st.session_state['v_map']:
    st.success(f"✅ Online: {len(st.session_state['v_map'])} Validators")
else:
    st.warning("⚠️ Offline Mode: Using manual detection")

uploaded_file = st.file_uploader("Upload CSV/Excel", type=["csv", "xlsx"])

if uploaded_file:
    if uploaded_file.name.endswith('.csv'):
        df = pd.read_csv(uploaded_file)
    else:
        df = pd.read_excel(uploaded_file)
    
    c1, c2 = st.columns(2)
    with c1:
        cols = df.columns.tolist()
        hash_col = st.selectbox("Transaction Hash Column", cols)
    with c2:
        target_keyword = st.text_input("Target Validator (for Staking)", value="Nansen")
    
    if st.button("🚀 Run Analysis"):
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        # Output Containers
        out_types = []
        out_amounts = []
        out_tokens = [] # NEW
        out_target_amounts = []
        out_times = []
        out_senders = []
        out_recipients = []
        out_fees = []
        
        BATCH_SIZE = 10
        all_hashes = df[hash_col].astype(str).str.strip().tolist()
        v_map = st.session_state.get('v_map') or {}
        
        for i in range(0, len(all_hashes), BATCH_SIZE):
            batch_hashes = all_hashes[i : i + BATCH_SIZE]
            status_text.text(f"Processing Batch {i//BATCH_SIZE + 1}...")
            progress_bar.progress((i + 1) / len(all_hashes))
            
            # Fetch
            batch_data = fetch_batch_transactions(batch_hashes)
            batch_lookup = {}
            if batch_data:
                batch_lookup = {item['digest']: item for item in batch_data if item and 'digest' in item}
            
            # Process
            for tx_hash in batch_hashes:
                tx_info = None
                if tx_hash in batch_lookup:
                    tx_info = batch_lookup[tx_hash]
                if not tx_info:
                    time.sleep(0.2)
                    tx_info = fetch_single_transaction(tx_hash)
                
                if tx_info:
                    data = parse_transaction(tx_info, v_map, target_keyword)
                    out_types.append(data["Type"])
                    out_amounts.append(data["Amount"])
                    out_tokens.append(data["Token"]) # NEW
                    out_target_amounts.append(data["Target Amount"])
                    out_times.append(data["Timestamp"])
                    out_senders.append(data["Sender"])
                    out_recipients.append(data["Recipient"])
                    out_fees.append(data["Gas Fees"])
                else:
                    out_types.append("Error")
                    out_amounts.append(0)
                    out_tokens.append("N/A")
                    out_target_amounts.append("N/A")
                    out_times.append("N/A")
                    out_senders.append("N/A")
                    out_recipients.append("N/A")
                    out_fees.append(0)
            
            time.sleep(1)

        # Build DataFrame
        df["Transaction Type"] = out_types
        df["Amount"] = out_amounts # Renamed from Amount (SUI)
        df["Token"] = out_tokens   # NEW COLUMN
        df[f"Amount ({target_keyword})"] = out_target_amounts
        df["Timestamp"] = out_times
        df["Sender"] = out_senders
        df["Recipient"] = out_recipients
        df["Gas Fees (SUI)"] = out_fees
        
        st.success("✅ Done!")
        st.dataframe(df)
        st.download_button("📥 Download Report", df.to_csv(index=False).encode('utf-8'), "sui_results_tokens.csv", "text/csv")
