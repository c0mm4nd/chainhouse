use std::error::Error;

use clickhouse::Client;

pub async fn create_schema(client: &Client) -> Result<(), Box<dyn Error>>  {
    debug!("start  initializing schema");
    let db_ddl = r"
    CREATE DATABASE IF NOT EXISTS ethereum;
    ";

    let blocks_ddl = r"
    CREATE TABLE IF NOT EXISTS ethereum.blocks (
        hash             FixedString(32),
        number           UInt64,
        parentHash       FixedString(32),
        uncles           Array(String),
        sha3Uncles       FixedString(32),           
        totalDifficulty  UInt256,
        miner            FixedString(32),
        difficulty       UInt256,
        nonce            UInt64,
        mixHash          FixedString(32),
        baseFeePerGas    Nullable(UInt256),
        gasLimit         UInt256,
        gasUsed          UInt256,
        stateRoot        FixedString(32),
        transactionsRoot FixedString(32),
        receiptsRoot     FixedString(32),
        logsBloom        String,
        withdrawlsRoot  Nullable(String),
        extraData        String,
        timestamp        UInt256,
        size             UInt256,
    ) ENGINE=ReplacingMergeTree 
    ORDER BY (hash, number);
    ";

    let tx_ddl = r"
    CREATE TABLE IF NOT EXISTS ethereum.transactions (
        hash             FixedString(32),
        blockHash        FixedString(32),
        blockNumber      UInt64,
        blockTimestamp   UInt256,
        transactionIndex UInt64,
        chainId Nullable(UInt256),
        type    Nullable(UInt64),
        from             String,
        to               Nullable(String),
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
        -- accessList Array(Tuple(String, Array(String))) COMMENT 'item in accessList: {address: tuple.0, storageKeys: turple.1 }',
        accessList Nullable(String),
        contractAddress Nullable(String),
        cumulativeGasUsed UInt256,
        effectiveGasPrice UInt256,
        gasUsed           UInt256,
        logsBloom         String,
        root              Nullable(FixedString(32)),
        status            UInt64
    ) ENGINE=ReplacingMergeTree
    ORDER BY hash;
    ";

    let event_ddl = r"
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
    ";

    let withdraw_ddl = r"
    CREATE TABLE IF NOT EXISTS ethereum.withdraws (
        blockHash String,
        blockNumber UInt64,
        blockTimestamp UInt256,
        `index` UInt64,
        validatorIndex UInt64,
        address String,
        amount UInt256
    ) ENGINE=ReplacingMergeTree
    ORDER BY (blockHash, index);
    ";

    client.query(db_ddl).execute().await?;
    client.query(blocks_ddl).execute().await?;
    client.query(tx_ddl).execute().await?;
    client.query(event_ddl).execute().await?;
    client.query(withdraw_ddl).execute().await?;

    debug!("schema initialized");
    Ok(())
}
