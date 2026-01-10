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
            response = requests.post(node, json=payload, headers=HEADERS, timeout=15)
            if response.status_code == 200:
                data = response.json()
                if "result" in data:
                    return data["result"]
        except Exception:
            continue 
    return None

def get_validator_map():
    """Downloads Validator List (Phonebook)."""
    validator_map = {}
    try:
        result = make_rpc_call("suix_getLatestSuiSystemStateV2", [])
        if result:
            for v in result.get('activeValidators', []):
                validator_map[v['suiAddress'].lower()] = v['name']
    except:
        pass
    return validator_map

def format_sui(mist_amount):
    if mist_amount is None: return 0.0
    return float(mist_amount) / 1_000_000_000

def parse_transaction(tx_data, validator_map, target_keyword):
    """
    Master Logic: Determines Type, Amount, and checks for DYNAMIC Target Validator.
    """
    if not tx_data:
        return {"Type": "Network Error"}

    # 1. TIMESTAMP
    ts_str = "Unknown"
    if 'timestampMs' in tx_data:
        dt = datetime.fromtimestamp(int(tx_data['timestampMs']) / 1000, tz=timezone.utc)
        ts_str = dt.strftime('%d.%m.%Y UTC %H:%M')

    # 2. SENDER
    sender = tx_data.get('transaction', {}).get('data', {}).get('sender', 'Unknown')

    # 3. GAS FEE
    gas_used = tx_data.get('effects', {}).get('gasUsed', {})
    comp_cost = int(gas_used.get('computationCost', 0))
    stor_cost = int(gas_used.get('storageCost', 0))
    stor_rebate = int(gas_used.get('storageRebate', 0))
    total_gas_mist = comp_cost + stor_cost - stor_rebate
    gas_fee_sui = format_sui(total_gas_mist)

    # 4. DEEP ANALYSIS (Type & Amounts)
    tx_type = "Unknown"
    main_amount = 0.0
    recipient = "N/A"
    
    # This variable fills ONLY if the validator matches your search keyword
    target_amount = "N/A" 

    events = tx_data.get('events', [])
    balance_changes = tx_data.get('balanceChanges', [])
    
    is_staking_action = False
    
    # --- CHECK EVENTS (Priority for Stake/Unstake) ---
    for event in events:
        e_type = event.get('type', '')
        parsed = event.get('parsedJson', {})

        # A. STAKE DETECTION
        if "RequestAddStake" in e_type or "StakingRequest" in e_type:
            tx_type = "Stake"
            is_staking_action = True
            
            amount_mist = float(parsed.get('amount', 0))
            sui_val = -format_sui(amount_mist) # Negative for Stake
            
            # Resolve Validator Name
            val_addr = parsed.get('validator_address', '').lower()
            val_name = validator_map.get(val_addr, "Unknown Validator")
            
            # Smart Nansen Detection (Hidden Bonus for Nansen specifically)
            if "0xa36a" in val_addr and val_name == "Unknown Validator":
                val_name = "Nansen (Detected)"

            recipient = val_name
            main_amount = sui_val
            
            # DYNAMIC FILTER: Check if the validator name contains your keyword
            if target_keyword.lower() in val_name.lower():
                target_amount = sui_val
            
            break
        
        # B. UNSTAKE DETECTION
        elif "Withdraw" in e_type or "Unstake" in e_type or "UnstakingRequest" in e_type:
            tx_type = "Unstake"
            is_staking_action = True
            
            p = float(parsed.get('principal_amount', 0))
            r = float(parsed.get('reward_amount', 0))
            if p == 0 and r == 0: p = float(parsed.get('amount', 0))
                
            main_amount = format_sui(p + r) # Positive
            recipient = "N/A"
            break

    # --- BALANCE CHANGE FALLBACK (For Send/Receive) ---
    if not is_staking_action:
        sender_net_change = 0
        found_recipient = False
        
        for change in balance_changes:
            owner = change.get('owner', {})
            addr = owner.get('AddressOwner', '')
            
            if addr == sender:
                net_change_mist = float(change.get('amount', 0))
                sender_net_change = net_change_mist + total_gas_mist 
                
            elif addr != sender and float(change.get('amount', 0)) > 0:
                recipient = addr
                found_recipient = True

        if sender_net_change < -1000:
            tx_type = "Send"
            main_amount = format_sui(sender_net_change)
        elif sender_net_change > 1000:
            tx_type = "Receive"
            main_amount = format_sui(sender_net_change)
            if not found_recipient: recipient = "N/A"
        else:
            tx_type = "Contract Call"
            main_amount = 0.0

    return {
        "Type": tx_type,
        "Amount": main_amount,          
        "Target Amount": target_amount, 
        "Timestamp": ts_str,
        "Sender": sender,
        "Recipient": recipient,
        "Gas Fees": gas_fee_sui
    }

def fetch_batch_transactions(hashes):
    params = [hashes, {"showEvents": True, "showBalanceChanges": True, "showInput": True, "showEffects": True}]
    return make_rpc_call("sui_multiGetTransactionBlocks", params)

# --- UI ---
st.set_page_config(page_title="Sui Unified Analyzer", page_icon="⚡", layout="wide")
st.title("⚡ Sui Unified Analyzer (Dynamic Filter)")

st.markdown("""
**How to use:**
1.  **Upload** your file with Transaction Hashes.
2.  **Type the Validator Name** (e.g., 'InfStones', 'Nansen', 'Obelisk') in the box below.
3.  The app will auto-detect **Stake/Unstake/Send** types.
4.  The **Target Amount** column will only fill if the transaction matches your keyword.
""")

# Load Validator Map
if 'v_map' not in st.session_state:
    with st.spinner("Loading Validator Phonebook..."):
        st.session_state['v_map'] = get_validator_map()
    
if st.session_state['v_map']:
    st.success(f"✅ Online: {len(st.session_state['v_map'])} Validators Loaded")
else:
    st.warning("⚠️ Offline Mode: Phonebook blocked (Using manual detection)")

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
        # DYNAMIC INPUT BOX
        target_keyword = st.text_input("Enter Target Validator Name (e.g., Nansen, InfStones)", value="Nansen")
    
    if st.button("🚀 Run Analysis"):
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        out_types = []
        out_amounts = []
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
            
            batch_data = fetch_batch_transactions(batch_hashes)
            batch_lookup = {}
            if batch_data:
                batch_lookup = {item['digest']: item for item in batch_data if item and 'digest' in item}
            
            for tx_hash in batch_hashes:
                if tx_hash in batch_lookup:
                    data = parse_transaction(batch_lookup[tx_hash], v_map, target_keyword)
                    out_types.append(data["Type"])
                    out_amounts.append(data["Amount"])
                    out_target_amounts.append(data["Target Amount"])
                    out_times.append(data["Timestamp"])
                    out_senders.append(data["Sender"])
                    out_recipients.append(data["Recipient"])
                    out_fees.append(data["Gas Fees"])
                else:
                    out_types.append("Error")
                    out_amounts.append(0)
                    out_target_amounts.append("N/A")
                    out_times.append("N/A")
                    out_senders.append("N/A")
                    out_recipients.append("N/A")
                    out_fees.append(0)
            
            time.sleep(1)

        # Build DataFrame
        df["Transaction Type"] = out_types
        df["Amount (SUI)"] = out_amounts
        
        # DYNAMIC COLUMN HEADER based on what you typed
        df[f"Amount ({target_keyword})"] = out_target_amounts
        
        df["Timestamp"] = out_times
        df["Sender"] = out_senders
        df["Recipient"] = out_recipients
        df["Gas Fees (SUI)"] = out_fees
        
        st.success("✅ Done!")
        st.dataframe(df)
        st.download_button("📥 Download Master Report", df.to_csv(index=False).encode('utf-8'), "sui_unified_results.csv", "text/csv")
