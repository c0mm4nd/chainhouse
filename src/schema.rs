use klickhouse::{Row, u256, Bytes};

#[derive(Row, Clone, Debug, Default)]
pub struct BlockRow {
    pub hash: Bytes,
    pub number: u64,
    pub parentHash: Bytes,
    pub uncles: Vec<Bytes>,
    pub sha3Uncles: Bytes,
    pub totalDifficulty: u256,
    pub difficulty: u256,
    pub miner: Bytes,
    pub nonce: Bytes,
    pub mixHash: Bytes,
    pub baseFeePerGas: Option<u256>,
    pub gasLimit: u256,
    pub gasUsed: u256,
    pub stateRoot: Bytes,
    pub transactionsRoot: Bytes,
    pub receiptsRoot: Bytes,
    pub logsBloom: Bytes,
    pub withdrawlsRoot: Option<Bytes>,
    pub extraData: Bytes,
    pub timestamp: u256,
    pub size: u256,
}


#[derive(Row, Clone, Debug, Default)]
pub struct TransactionRow {
    pub hash: Bytes,
    pub blockHash: Bytes,
    pub blockNumber: u64,
    pub blockTimestamp: u256,
    pub transactionIndex: u64,
    pub chainId: Option<u256>,
    pub r#type: Option<u64>,
    pub from: Bytes,
    pub to: Option<Bytes>,
    pub value: u256,
    pub nonce: u256,
    pub input: Bytes,
    pub gas: u256,
    pub gasPrice: Option<u256>,
    pub maxFeePerGas: Option<u256>,
    pub maxPriorityFeePerGas: Option<u256>,
    pub r: u256,
    pub s: u256,
    pub v: u64,
    pub accessList: Option<String>,
    pub contractAddress: Option<Bytes>,
    pub cumulativeGasUsed: u256,
    pub effectiveGasPrice: Option<u256>,
    pub gasUsed: u256,
    pub logsBloom: Bytes,
    pub root: Option<Bytes>,
    pub status: Option<u64>,
}

#[derive(Row, Clone, Debug, Default)]
pub struct EventRow {
    pub blockHash: Bytes,
    pub blockNumber: u64,
    pub blockTimestamp: u256,
    pub transactionHash: Bytes,
    pub transactionIndex: u64,
    pub logIndex: u256,
    pub removed: bool,
    pub topics: Vec<Bytes>,
    pub data: Bytes,
    pub address: Bytes,
}

#[derive(Row, Clone, Debug, Default)]
pub struct WithdrawalRow {
    pub blockHash: Bytes,
    pub blockNumber: u64,
    pub blockTimestamp: u256,
    pub index: u64,
    pub validatorIndex: u64,
    pub address: Bytes,
    pub amount: u256,
}
