use clap::Parser;
use ethers::providers::{Middleware, Provider, Ws};
use klickhouse::{u256, Bytes};
use klickhouse::{Client, ClientOptions};
use schema::{BlockRow, EventRow, TransactionRow, WithdrawalRow};
use std::error::Error;

extern crate pretty_env_logger;
#[macro_use]
extern crate log;

mod helpers;
mod schema;

/// Simple ETL program to load ethereum data into clickhouse
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

#[tokio::main(flavor = "multi_thread", worker_threads = 10)]
async fn main() -> Result<(), Box<dyn Error>> {
    pretty_env_logger::init_timed();

    let args = Args::parse();

    let client = Client::connect("127.0.0.1:9000", ClientOptions::default())
        .await
        .unwrap();

    let provider = Provider::<Ws>::connect(args.ethereum).await?;

    if args.schema {
        debug!("start  initializing schema");
        client
            .execute(
                "
        CREATE DATABASE IF NOT EXISTS ethereum;
        ",
            )
            .await
            .unwrap();
        client
            .execute(
                "
        CREATE TABLE IF NOT EXISTS ethereum.blocks (
            hash             FixedString(32),
            number           UInt64,
            parentHash       FixedString(32),
            uncles           Array(String),
            sha3Uncles       FixedString(32),           
            totalDifficulty  UInt256,
            miner            FixedString(20),
            difficulty       UInt256,
            nonce            FixedString(8),
            mixHash          FixedString(32),
            baseFeePerGas    Nullable(UInt256),
            gasLimit         UInt256,
            gasUsed          UInt256,
            stateRoot        FixedString(32),
            transactionsRoot FixedString(32),
            receiptsRoot     FixedString(32),
            logsBloom        String,
            withdrawlsRoot  Nullable(FixedString(32)),
            extraData        String,
            timestamp        UInt256,
            size             UInt256,
        ) ENGINE=ReplacingMergeTree 
        ORDER BY (hash, number);
        ",
            )
            .await
            .unwrap();
        client.execute("
        CREATE TABLE IF NOT EXISTS ethereum.transactions (
            hash             FixedString(32),
            blockHash        FixedString(32),
            blockNumber      UInt64,
            blockTimestamp   UInt256,
            transactionIndex UInt64,
            chainId Nullable(UInt256),
            type    Nullable(UInt64),
            from             FixedString(20),
            to               Nullable(FixedString(20)),
            value            UInt256,
            nonce            UInt256,
            input            String,
            gas                  UInt256,
            gasPrice             Nullable(UInt256),
            maxFeePerGas         Nullable(UInt256),
            maxPriorityFeePerGas Nullable(UInt256),
            r UInt256,
            s UInt256,
            v UInt64,
            accessList Nullable(String),
            contractAddress Nullable(FixedString(20)),
            cumulativeGasUsed UInt256,
            effectiveGasPrice Nullable(UInt256),
            gasUsed           UInt256,
            logsBloom         String,
            root              Nullable(FixedString(32)) COMMENT 'Only present before activation of [EIP-658]',
            status            Nullable(UInt64) COMMENT 'Only present after activation of [EIP-658]'
        ) ENGINE=ReplacingMergeTree
        ORDER BY hash;
        ").await.unwrap();
        client
            .execute(
                "
        CREATE TABLE IF NOT EXISTS ethereum.events (
            address FixedString(20),
            blockHash FixedString(32),
            blockNumber UInt64,
            blockTimestamp UInt256,
            transactionHash FixedString(32),
            transactionIndex UInt64,
            logIndex UInt256,
            removed Boolean,
            topics Array(FixedString(32)),
            data String,
        ) ENGINE=ReplacingMergeTree
        ORDER BY (transactionHash, logIndex);
        ",
            )
            .await
            .unwrap();
        client
            .execute(
                "
        CREATE TABLE IF NOT EXISTS ethereum.withdraws (
            blockHash String,
            blockNumber UInt64,
            blockTimestamp UInt256,
            `index` UInt64,
            validatorIndex UInt64,
            address FixedString(20),
            amount UInt256
        ) ENGINE=ReplacingMergeTree
        ORDER BY (blockHash, index);
        ",
            )
            .await
            .unwrap();
        debug!("schema initialized");
    }

    let batch: u64 = 1_000;

    let mut block_row_list = Vec::with_capacity((batch + 1_u64) as usize);
    let mut transaction_row_list = Vec::new();
    let mut event_row_list = Vec::new();
    let mut withdraw_row_list = Vec::new();

    for i in args.from..=args.to {
        let block = provider.get_block_with_txs(i).await.unwrap().unwrap();
        let receipts = provider.get_block_receipts(i).await.unwrap();

        let block_row = BlockRow {
            hash: block.hash.unwrap().0.to_vec().into(), //block.hash.unwrap()),
            number: block.number.unwrap().as_u64(),
            parentHash: block.parent_hash.0.to_vec().into(),
            uncles: block
                .uncles
                .iter()
                .map(|uncle| uncle.0.to_vec().into())
                .collect(),
            sha3Uncles: block.uncles_hash.0.to_vec().into(),
            totalDifficulty: u256(block.total_difficulty.unwrap().into()),
            difficulty: u256(block.difficulty.into()),
            miner: block.author.unwrap().0.to_vec().into(),
            nonce: block.nonce.unwrap().0.to_vec().into(),
            mixHash: block.mix_hash.unwrap().0.to_vec().into(),
            baseFeePerGas: block
                .base_fee_per_gas
                .and_then(|fee| Some(u256(fee.into()))),
            gasLimit: u256(block.gas_limit.into()),
            gasUsed: u256(block.gas_used.into()),
            stateRoot: block.state_root.0.to_vec().into(),
            transactionsRoot: block.transactions_root.0.to_vec().into(),
            receiptsRoot: block.receipts_root.0.to_vec().into(),
            logsBloom: block.logs_bloom.unwrap().0.to_vec().into(),
            withdrawlsRoot: block
                .withdrawals_root
                .and_then(|root| Some(root.0.to_vec().into())),
            extraData: block.extra_data.to_vec().into(),
            timestamp: u256(block.timestamp.into()),
            size: u256(block.size.unwrap().into()),
        };
        block_row_list.push(block_row);

        for (transaction_index, transaction) in block.transactions.iter().enumerate() {
            let receipt = &receipts[transaction_index];

            let transaction_row = TransactionRow {
                hash: transaction.hash.0.to_vec().into(),
                blockHash: transaction.block_hash.unwrap().0.to_vec().into(),
                blockNumber: transaction.block_number.unwrap().as_u64(),
                blockTimestamp: u256(block.timestamp.into()),
                transactionIndex: transaction.transaction_index.unwrap().as_u64(),
                chainId: transaction.chain_id.and_then(|id| Some(u256(id.into()))),
                r#type: transaction.transaction_type.and_then(|t| Some(t.as_u64())),
                from: transaction.from.0.to_vec().into(),
                to: transaction.to.and_then(|to| Some(to.0.to_vec().into())),
                value: u256(transaction.value.into()),
                nonce: u256(transaction.nonce.into()),
                input: transaction.input.to_vec().into(),
                gas: u256(transaction.gas.into()),
                gasPrice: transaction
                    .gas_price
                    .and_then(|price| Some(u256(price.into()))),
                maxFeePerGas: transaction
                    .max_fee_per_gas
                    .and_then(|fee| Some(u256(fee.into()))),
                maxPriorityFeePerGas: transaction
                    .max_priority_fee_per_gas
                    .and_then(|fee| Some(u256(fee.into()))),
                r: u256(transaction.r.into()),
                s: u256(transaction.s.into()),
                v: transaction.v.as_u64(),
                accessList: transaction
                    .access_list
                    .as_ref()
                    .and_then(|al| Some(serde_json::to_string(&al.clone().to_owned()).unwrap())),
                contractAddress: receipt
                    .contract_address
                    .and_then(|contract| Some(contract.0.to_vec().into())),
                cumulativeGasUsed: u256(receipt.cumulative_gas_used.into()),
                effectiveGasPrice: receipt
                    .effective_gas_price
                    .and_then(|price| Some(u256(price.into()))),
                gasUsed: u256(receipt.gas_used.unwrap().into()),
                logsBloom: receipt.logs_bloom.0.to_vec().into(),
                root: receipt.root.and_then(|root| Some(root.0.to_vec().into())), // Only present before activation of [EIP-658]
                status: receipt.status.and_then(|status| Some(status.as_u64())), // Only present after activation of [EIP-658]
            };
            transaction_row_list.push(transaction_row);

            for log in &receipt.logs {
                let mut event_row = EventRow {
                    blockHash: log.block_hash.unwrap().0.to_vec().into(),
                    blockNumber: log.block_number.unwrap().as_u64(),
                    blockTimestamp: u256(block.timestamp.into()),
                    transactionHash: transaction.hash.0.to_vec().into(),
                    transactionIndex: transaction.transaction_index.unwrap().as_u64(),
                    logIndex: u256(log.log_index.unwrap().into()),
                    removed: log.removed.unwrap(),
                    topics: log
                        .topics
                        .iter()
                        .map(|topic| topic.0.to_vec().into())
                        .collect(),
                    data: log.data.to_vec().into(),
                    address: log.address.0.to_vec().into(),
                };
                event_row_list.push(event_row);
            }
        }

        if let Some(withdraws) = block.withdrawals {
            for withdraw in withdraws {
                let withdraw_row = WithdrawalRow {
                    blockHash: block.hash.unwrap().0.to_vec().into(),
                    blockNumber: block.number.unwrap().as_u64(),
                    blockTimestamp: u256(block.timestamp.into()),
                    index: withdraw.index.as_u64(),
                    validatorIndex: withdraw.validator_index.as_u64(),
                    address: withdraw.address.0.to_vec().into(),
                    amount: u256(withdraw.amount.into()),
                };
                withdraw_row_list.push(withdraw_row);
            }
        }

        if i % batch == 0 {
            tokio::try_join!(
                client.insert_native_block(
                    "INSERT INTO ethereum.blocks FORMAT native",
                    block_row_list.to_vec()
                ),
                client.insert_native_block(
                    "INSERT INTO ethereum.transactions FORMAT native",
                    transaction_row_list.to_vec()
                ),
                client.insert_native_block(
                    "INSERT INTO ethereum.events FORMAT native",
                    event_row_list.to_vec()
                ),
                client.insert_native_block(
                    "INSERT INTO ethereum.withdraws FORMAT native",
                    withdraw_row_list.to_vec()
                )
            )
            .unwrap();

            block_row_list.clear();
            transaction_row_list.clear();
            event_row_list.clear();
            withdraw_row_list.clear();

            info!("{} done", i)
        }
    }

    tokio::try_join!(
        client.insert_native_block("INSERT INTO ethereum.blocks FORMAT native", block_row_list),
        client.insert_native_block(
            "INSERT INTO ethereum.transactions FORMAT native",
            transaction_row_list
        ),
        client.insert_native_block("INSERT INTO ethereum.events FORMAT native", event_row_list),
        client.insert_native_block(
            "INSERT INTO ethereum.withdraws FORMAT native",
            withdraw_row_list
        )
    )
    .unwrap();

    Ok(())
}
