# chainhouse
Simple DDL program to load ethereum data into clickhouse

## Ready

1. Get your clickhouse & ethereum node (with websocket) ready.

2. Install latest rust and run
 
```bash
git clone https://github.com/c0mm4nd/chainhouse && cd chainhouse
cargo build --release
./target/release/chainhouse -h
```

## Usage

```bash
Simple DDL program to load ethereum data into clickhouse

Usage: chainhouse [OPTIONS] --from <FROM> --to <TO>

Options:
  -c, --clickhouse <CLICKHOUSE>  clickhouse endpoint url [default: http://localhost:8123]
  -e, --ethereum <ETHEREUM>      ethereum endpoint url [default: ws://localhost:8545]
      --from <FROM>              from
      --to <TO>                  (inclusive) to
      --schema                   initialize schema
  -h, --help                     Print help
  -V, --version                  Print version
```
