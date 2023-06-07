use clap::Parser;
use clickhouse::inserter::Inserter;
use clickhouse::Client;
use clickhouse::Row;
use ethers::types::transaction::eip2930::AccessList;
use ethers::types::Bloom;
use ethers::types::Bytes;
use ethers::types::H160;
use ethers::types::H64;
use ethers::types::U64;
use ethers::{
    providers::{Middleware, Provider, Ws},
    types::{Block, Transaction, H256, U256},
};
use serde::{Deserialize, Serialize};
use std::error::Error;

extern crate pretty_env_logger;
#[macro_use]
extern crate log;

mod schema;

/// Simple DDL program to load ethereum data into clickhouse
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// clickhouse endpoint url
    #[arg(short, long, default_value = "http://localhost:8123")]
    clickhouse: String,

    /// ethereum endpoint url
    #[arg(short, long, default_value = "ws://localhost:8545")]
    ethereum: String,

    /// from
    #[arg(long)]
    from: u64,

    /// (inclusive) to
    #[arg(long)]
    to: u64,

    /// initialize schema
    #[arg(long, default_value_t = false)]
    schema: bool,
}

macro_rules! option {
    ($name:ident, $doc:literal) => {
        #[doc = $doc]
        pub mod option {
            use super::*;

            struct $name(super::$name);

            impl Serialize for $name {
                fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                    super::serialize(&self.0, serializer)
                }
            }

            impl<'de> Deserialize<'de> for $name {
                fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                    super::deserialize(deserializer).map($name)
                }
            }

            pub fn serialize<S>(v: &Option<super::$name>, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                v.clone().map($name).serialize(serializer)
            }

            pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<super::$name>, D::Error>
            where
                D: Deserializer<'de>,
            {
                let opt: Option<$name> = Deserialize::deserialize(deserializer)?;
                Ok(opt.map(|v| v.0))
            }
        }
    };
}

mod u64 {
    use ethers::types::U64;
    use serde::{
        de::{Deserialize, Deserializer},
        ser::{Serialize, Serializer},
    };

    option!(U64, "Ser/de `Option<U64>` to/from `Nullable(U64)`.");

    pub fn serialize<S: Serializer>(u: &U64, serializer: S) -> Result<S::Ok, S::Error> {
        u.0.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<U64, D::Error>
    where
        D: Deserializer<'de>,
    {
        let u: [u64; 1] = Deserialize::deserialize(deserializer)?;
        Ok(U64(u))
    }
}

mod h64 {
    use ethers::types::H64;
    use serde::{
        de::{Deserialize, Deserializer},
        ser::{Serialize, Serializer},
    };

    option!(H64, "Ser/de `Option<H64>` to/from `Nullable(H64)`.");

    pub fn serialize<S: Serializer>(u: &H64, serializer: S) -> Result<S::Ok, S::Error> {
        u.0.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<H64, D::Error>
    where
        D: Deserializer<'de>,
    {
        let u: [u8; 8] = Deserialize::deserialize(deserializer)?;
        Ok(H64(u))
    }
}

mod u256 {
    use ethers::types::U256;
    use serde::{
        de::{Deserialize, Deserializer},
        ser::{Serialize, Serializer},
    };

    option!(U256, "Ser/de `Option<U256>` to/from `Nullable(U256)`.");

    pub fn serialize<S: Serializer>(u: &U256, serializer: S) -> Result<S::Ok, S::Error> {
        u.0.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<U256, D::Error>
    where
        D: Deserializer<'de>,
    {
        let u: [u64; 4] = Deserialize::deserialize(deserializer)?;
        Ok(U256(u))
    }
}

mod h160 {
    use ethers::types::H160;
    use serde::{
        de::{Deserialize, Deserializer},
        ser::{Serialize, Serializer},
    };

    option!(H160, "Ser/de `Option<H160>` to/from `Nullable(H160)`.");

    pub fn serialize<S: Serializer>(u: &H160, serializer: S) -> Result<S::Ok, S::Error> {
        u.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<H160, D::Error>
    where
        D: Deserializer<'de>,
    {
        let u: H160 = Deserialize::deserialize(deserializer)?;
        Ok(u)
    }
}

mod h256 {
    use ethers::types::H256;
    use serde::{
        de::{Deserialize, Deserializer},
        ser::{Serialize, Serializer},
    };

    option!(H256, "Ser/de `Option<H256>` to/from `Nullable(H256)`.");

    pub fn serialize<S: Serializer>(u: &H256, serializer: S) -> Result<S::Ok, S::Error> {
        u.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<H256, D::Error>
    where
        D: Deserializer<'de>,
    {
        let u: H256 = Deserialize::deserialize(deserializer)?;
        Ok(u)
    }
}

mod access_list {
    use ethers::types::transaction::eip2930::{AccessList, AccessListItem};
    use serde::{
        de::{Deserialize, Deserializer},
        ser::{Serialize, Serializer},
    };

    option!(
        AccessList,
        "Ser/de `Option<AccessList>` to/from `Nullable(AccessList)`."
    );

    pub fn serialize<S: Serializer>(u: &AccessList, serializer: S) -> Result<S::Ok, S::Error> {
        let raw = serde_json::to_string(u).unwrap();
        raw.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<AccessList, D::Error>
    where
        D: Deserializer<'de>,
    {
        let u: String = Deserialize::deserialize(deserializer)?;
        let al: AccessList = serde_json::from_str(&u).unwrap();
        Ok(al)
    }
}

#[derive(Row, Serialize, Deserialize)]
struct BlockRow {
    hash: H256,
    #[serde(with = "u64")]
    number: U64,
    parentHash: H256,
    uncles: Vec<H256>,
    sha3Uncles: H256,
    #[serde(with = "u256")]
    totalDifficulty: U256,
    miner: H160,
    #[serde(with = "h64")]
    nonce: H64,
    mixHash: H256,
    #[serde(with = "u256::option")]
    baseFeePerGas: Option<U256>,
    #[serde(with = "u256")]
    gasLimit: U256,
    #[serde(with = "u256")]
    gasUsed: U256,
    stateRoot: H256,
    transactionsRoot: H256,
    receiptsRoot: H256,
    logsBloom: Bloom,
    withdrawlsRoot: Option<H256>,
    extraData: Bytes,
    #[serde(with = "u256")]
    timestamp: U256,
    #[serde(with = "u256")]
    size: U256,
}

#[derive(Row, Serialize, Deserialize)]
struct TransactionRow {
    hash: H256,
    blockHash: H256,
    #[serde(with = "u64")]
    blockNumber: U64,
    #[serde(with = "u256")]
    blockTimestamp: U256,
    #[serde(with = "u64")]
    transactionIndex: U64,
    #[serde(with = "u256::option")]
    chainId: Option<U256>,
    #[serde(with = "u64::option")]
    r#type: Option<U64>,
    #[serde(with = "h160")]
    from: H160,
    #[serde(with = "h160::option")]
    to: Option<H160>,
    #[serde(with = "u256")]
    value: U256,
    #[serde(with = "u256")]
    nonce: U256,
    input: Bytes,
    #[serde(with = "u256")]
    gas: U256,
    #[serde(with = "u256::option")]
    gasPrice: Option<U256>,
    #[serde(with = "u256::option")]
    maxFeePerGas: Option<U256>,
    #[serde(with = "u256::option")]
    maxPriorityFeePerGas: Option<U256>,
    #[serde(with = "u256")]
    r: U256,
    #[serde(with = "u256")]
    s: U256,
    #[serde(with = "u64")]
    v: U64,
    #[serde(with = "access_list::option")]
    accessList: Option<AccessList>,
    #[serde(with = "h160::option")]
    contractAddress: Option<H160>,
    #[serde(with = "u256")]
    cumulativeGasUsed: U256,
    #[serde(with = "u256")]
    effectiveGasPrice: U256,
    #[serde(with = "u256")]
    gasUsed: U256,
    logsBloom: Bloom,
    #[serde(with = "h256::option")]
    root: Option<H256>,
    #[serde(with = "u64")]
    status: U64,
}

#[derive(Row, Serialize, Deserialize)]
struct EventRow {
    blockHash: H256,
    #[serde(with = "u64")]
    blockNumber: U64,
    #[serde(with = "u256")]
    blockTimestamp: U256,
    transactionHash: H256,
    #[serde(with = "u64")]
    transactionIndex: U64,
    #[serde(with = "u256")]
    logIndex: U256,
    removed: bool,
    topics: Vec<H256>,
    data: Bytes,
    address: H160,
}

#[derive(Row, Serialize, Deserialize)]
pub struct WithdrawalRow {
    blockHash: H256,
    #[serde(with = "u64")]
    blockNumber: U64,
    #[serde(with = "u256")]
    blockTimestamp: U256,
    #[serde(with = "u64")]
    index: U64,
    #[serde(with = "u64")]
    validatorIndex: U64,
    address: H160,
    #[serde(with = "u256")]
    amount: U256,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 10)]
async fn main() -> Result<(), Box<dyn Error>> {
    pretty_env_logger::init_timed();

    let args = Args::parse();

    let client = Client::default()
        .with_url(args.clickhouse)
        .with_database("ethereum");

    let provider = Provider::<Ws>::connect(args.ethereum).await?;

    if args.schema {
        schema::create_schema(&client).await?;
    }

    let mut insert_block = client.inserter("blocks")?;
    let mut insert_tx = client.inserter("transactions")?;
    let mut insert_event = client.inserter("events")?;
    let mut insert_withdraw = client.inserter("withdraw")?;

    for i in args.from..=args.to {
        parse_block(
            &provider,
            &mut insert_block,
            &mut insert_tx,
            &mut insert_event,
            &mut insert_withdraw,
            i,
        )
        .await?;
        if i % 1000 == 0 {
            insert_event.commit().await?;
            insert_tx.commit().await?;
            insert_block.commit().await?;
            warn!("{} done", i);
        }
    }

    insert_event.end().await?;
    insert_tx.end().await?;
    insert_block.end().await?;
    insert_withdraw.end().await?;

    Ok(())
}

async fn parse_block(
    provider: &Provider<Ws>,
    insert_block: &mut Inserter<BlockRow>,
    insert_tx: &mut Inserter<TransactionRow>,
    insert_event: &mut Inserter<EventRow>,
    insert_withdraw: &mut Inserter<WithdrawalRow>,
    block_number: u64,
) -> Result<(), Box<dyn Error>> {
    let block = provider
        .get_block_with_txs(block_number)
        .await
        .unwrap()
        .unwrap();

    insert_block
        .write(&BlockRow {
            hash: block.hash.unwrap(),
            number: block.number.unwrap(),
            parentHash: block.parent_hash,
            uncles: block.uncles,
            sha3Uncles: block.uncles_hash,
            totalDifficulty: block.total_difficulty.unwrap(),
            miner: block.author.unwrap(),
            nonce: block.nonce.unwrap(),
            mixHash: block.mix_hash.unwrap(),
            baseFeePerGas: block.base_fee_per_gas,
            gasLimit: block.gas_limit,
            gasUsed: block.gas_used,
            stateRoot: block.state_root,
            transactionsRoot: block.transactions_root,
            receiptsRoot: block.receipts_root,
            logsBloom: block.logs_bloom.unwrap(),
            withdrawlsRoot: block.withdrawals_root,
            extraData: block.extra_data,
            timestamp: block.timestamp,
            size: block.size.unwrap(),
        })
        .await?;

    let receipts = provider.get_block_receipts(block_number).await?;

    for (i, tx) in block.transactions.iter().enumerate() {
        let receipt = &receipts[i];

        insert_tx
            .write(&TransactionRow {
                hash: tx.hash,
                blockHash: tx.block_hash.unwrap(),
                blockNumber: tx.block_number.unwrap(),
                blockTimestamp: block.timestamp,
                transactionIndex: tx.transaction_index.unwrap(),
                chainId: tx.chain_id,
                r#type: tx.transaction_type,
                from: tx.from,
                to: tx.to,
                value: tx.value,
                nonce: tx.nonce,
                input: tx.input.clone(),
                gas: tx.gas,
                gasPrice: tx.gas_price,
                maxFeePerGas: tx.max_fee_per_gas,
                maxPriorityFeePerGas: tx.max_priority_fee_per_gas,
                r: tx.r,
                s: tx.s,
                v: tx.v,
                accessList: tx.access_list.clone(),
                contractAddress: receipt.contract_address,
                cumulativeGasUsed: receipt.cumulative_gas_used,
                effectiveGasPrice: receipt.effective_gas_price.unwrap(),
                gasUsed: receipt.gas_used.unwrap(),
                logsBloom: receipt.logs_bloom,
                root: receipt.root,
                status: receipt.status.unwrap(),
            })
            .await?;
        for log in &receipt.logs {
            insert_event
                .write(&EventRow {
                    blockHash: log.block_hash.unwrap(),
                    blockNumber: log.block_number.unwrap(),
                    blockTimestamp: block.timestamp,
                    transactionHash: log.transaction_hash.unwrap(),
                    transactionIndex: log.transaction_index.unwrap(),
                    logIndex: log.log_index.unwrap(),
                    removed: log.removed.unwrap(),
                    topics: log.topics.clone(),
                    data: log.data.clone(),
                    address: log.address,
                })
                .await?;
        }
    }

    if let Some(withdraws) = block.withdrawals {
        for w in withdraws {
            insert_withdraw
                .write(&WithdrawalRow {
                    blockHash: block.hash.unwrap(),
                    blockNumber: block.number.unwrap(),
                    blockTimestamp: block.timestamp,
                    index: w.index,
                    validatorIndex: w.validator_index,
                    address: w.address,
                    amount: w.amount,
                })
                .await?;
        }
    }

    Ok(())
}
