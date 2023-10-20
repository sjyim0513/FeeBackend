import sqlite3
from web3 import Web3
from dotenv import load_dotenv
import os

load_dotenv()

# Ethereum 네트워크에 연결
w3 = Web3(Web3.HTTPProvider(os.getenv("W3_URL")))
Scrollw3 = Web3(Web3.HTTPProvider(os.getenv("SEP_W3_URL")))
WEEK = 600
pairFactory_address = "0x72A4B2F0EC0D588D6c3388b294D5c4EE8af9Bc95"
minter_address = "0x886cAE8Ac5DD9cB375CF27B58e48B583B5b121b5"
pair_abi = [
    {
        "anonymous": False,
        "inputs": [
            {"indexed": False, "internalType": "uint256", "name": "amount0", "type": "uint256"},
            {"indexed": False, "internalType": "uint256", "name": "amount1", "type": "uint256"}
        ],
        "name": "Fees",
        "type": "event"
    }
]
minter_abi = [
    {
        "inputs": [],
        "name": "active_period",
        "outputs": [
            {
                "internalType": "uint256",
                "name": "",
                "type": "uint256",
            },
        ],
        "stateMutability": "view",
        "type": "function",
    },
]

minterContract = Scrollw3.eth.contract(address=minter_address, abi=minter_abi)

activePeriod = minterContract.functions.active_period().call()
activePeriodEnd = int(activePeriod) + WEEK
print(f"activePeriod : {activePeriod}")
print(f"activePeriodEnd : {activePeriodEnd}")

conn = sqlite3.connect('contract_data.db')
cursor = conn.cursor()

def handle_epoch():
    global activePeriodEnd, cursor, conn
    new_db_name = f"contract_data_{activePeriodEnd}.db"
    new_conn = sqlite3.connect(new_db_name)
    new_cursor = new_conn.cursor()

    print(f"New database created: {new_db_name}")

    cursor.close()
    conn.close()
    cursor = new_cursor
    conn = new_conn

def handle_event(event):
    sender = event['args']['sender']
    amount0 = event['args']['amount0']
    amount1 = event['args']['amount1']

    cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS {sender} (
            id INTEGER PRIMARY KEY,
            sender TEXT,
            amount0 REAL,
            amount1 REAL,
            totalFee0 REAL,
            totalFee1 REAL
        )
    ''')
    conn.commit()

    cursor.execute(f"INSERT INTO {sender} (sender, amount0, amount1) VALUES (?, ?, ?)", (sender, amount0, amount1))
    conn.commit()

    print(f"Received and saved data: sender={sender}, amount0={amount0}, amount1={amount1}")

    cursor.execute(f"SELECT totalFee0, totalFee1 FROM {sender} WHERE sender = ?", (sender,))
    result = cursor.fetchone()

    if result:
        totalFee0, totalFee1 = result
    else:
        totalFee0, totalFee1 = 0, 0

    # amount0 및 amount1 더하기
    totalFee0 += amount0
    totalFee1 += amount1

    # totalFee0 및 totalFee1 업데이트
    cursor.execute(f"UPDATE {sender} SET totalFee0 = ?, totalFee1 = ? WHERE sender = ?", (totalFee0, totalFee1, sender))
    conn.commit()

    print(f"Updated totalFee0={totalFee0}, totalFee1={totalFee1}")

contract = Scrollw3.eth.contract(address=pairFactory_address, abi=pair_abi)
event_filter = contract.events.Fees.createFilter(fromBlock="latest")
event_filter.watch(handle_event)

try:
    while True:
        current_block_number = Scrollw3.eth.block_number
        print(f"current block: {current_block_number}")

        if current_block_number >= activePeriodEnd:
            handle_epoch()
            activePeriod = minterContract.functions.active_period().call()
            activePeriodEnd = int(activePeriod) + WEEK
            print(f"activePeriodEnd updated: {activePeriodEnd}")
            
except KeyboardInterrupt:
    event_filter.stop_watching()
    