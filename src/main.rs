use anyhow::{Context, Result};
use async_std::{fs::File, stream::StreamExt};
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::sync::Arc;
use std::{collections::HashMap, env};
use tokio::sync::{mpsc, Mutex, RwLock};

#[derive(Debug, Deserialize)]
struct TransactionCsv {
    #[serde(rename = "type")]
    transaction_type: String,
    client: u16,
    tx: u32,
    amount: Option<f64>,
}

#[derive(Debug, Deserialize, Serialize)]
struct ClientAccount {
    available: f64,
    held: f64,
    total: f64,
    locked: bool,
}

#[derive(Debug, Clone)]
struct Transaction {
    transaction_type: String,
    client: u16,
    tx: u32,
    amount: Option<f64>,
    disputed: Arc<Mutex<bool>>,
}

impl ClientAccount {
    fn new() -> ClientAccount {
        ClientAccount {
            available: 0.0,
            held: 0.0,
            total: 0.0,
            locked: false,
        }
    }
}

impl Transaction {
    fn new(transaction_csv: TransactionCsv) -> Transaction {
        Transaction {
            transaction_type: transaction_csv.transaction_type,
            client: transaction_csv.client,
            tx: transaction_csv.tx,
            amount: transaction_csv.amount,
            disputed: Arc::new(Mutex::new(false)),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Get command-line arguments
    let args: Vec<String> = env::args().collect();

    if args.len() != 2 {
        eprintln!("Usage: cargo run -- <input_csv_file>");
        std::process::exit(1);
    }

    // Get the input file name from the command-line arguments
    let file_in = &args[1];

    let client_accounts = process_transaction(file_in).await?;

    print!("client, available, held, total, locked\n");
    print_client_accounts(client_accounts).await;

    Ok(())
}

async fn process_transaction(
    file_in: &str,
) -> Result<Arc<RwLock<HashMap<u16, Arc<Mutex<ClientAccount>>>>>, Box<dyn Error>>
{
    let client_accounts: Arc<RwLock<HashMap<u16, Arc<Mutex<ClientAccount>>>>> =
        Arc::new(RwLock::new(HashMap::new()));
    let txs: Arc<RwLock<HashMap<u32, Transaction>>> =
        Arc::new(RwLock::new(HashMap::new()));
    let mut senders: HashMap<u16, Arc<Mutex<mpsc::Sender<Transaction>>>> =
        HashMap::new();
    let mut rdr =
        csv_async::AsyncReader::from_reader(File::open(file_in).await?);

    let mut tasks = vec![];

    while let Some(result) = rdr.records().next().await {
        match result {
            Ok(record) => {
                let transaction_csv: TransactionCsv =
                    record.deserialize(None)?;
                let transaction = Transaction::new(transaction_csv);
                let client_id = transaction.client;

                let sender = senders
                    .entry(client_id)
                    .or_insert_with(|| {
                        let (sender, mut receiver) =
                            mpsc::channel::<Transaction>(32);
                        let txs_clone = txs.clone();
                        let client_accounts_clone = client_accounts.clone();
                        let task = tokio::spawn(async move {
                            while let Some(transaction) = receiver.recv().await
                            {
                                if transaction.transaction_type == "end" {
                                    break;
                                }
                                let _ = handle_transaction(
                                    &transaction,
                                    &client_accounts_clone,
                                    &txs_clone,
                                )
                                .await;
                            }
                        });
                        tasks.push(task);
                        Arc::new(Mutex::new(sender.clone()))
                    })
                    .clone();

                let mut client_accounts_write_lock =
                    client_accounts.write().await;

                if client_accounts_write_lock.get(&client_id).is_none() {
                    let client_account_mutex =
                        Arc::new(tokio::sync::Mutex::new(ClientAccount::new()));
                    client_accounts_write_lock
                        .insert(client_id, client_account_mutex.clone());
                }

                sender
                    .lock()
                    .await
                    .send(transaction.clone())
                    .await
                    .context("Error sending transaction")?;
            }
            Err(e) => {
                eprintln!("Error reading CSV record: {}", e);
            }
        }
    }

    let end_transaction = Transaction::new(TransactionCsv {
        transaction_type: String::from("end"),
        client: 0,
        tx: 0,
        amount: None,
    });

    for (_, sender) in senders.iter() {
        sender
            .lock()
            .await
            .send(end_transaction.clone())
            .await
            .context("Error sending end transaction")?;
    }

    for task in tasks {
        task.await?;
    }

    Ok(client_accounts)
}

async fn handle_transaction(
    transaction: &Transaction,
    client_accounts: &Arc<RwLock<HashMap<u16, Arc<Mutex<ClientAccount>>>>>,
    txs: &Arc<RwLock<HashMap<u32, Transaction>>>,
) {
    match transaction.transaction_type.as_str() {
        "deposit" => {
            handle_deposit(client_accounts, &transaction).await;
            txs.write()
                .await
                .insert(transaction.tx, transaction.clone());
        }
        "withdrawal" => {
            handle_withdrawal(client_accounts, &transaction).await;
            txs.write()
                .await
                .insert(transaction.tx, transaction.clone());
        }
        "dispute" => {
            handle_dispute(client_accounts, &transaction, txs).await;
        }
        "resolve" => {
            handle_resolve(client_accounts, &transaction, &txs).await;
        }
        "chargeback" => {
            handle_chargeback(client_accounts, &transaction, &txs).await;
        }
        _ => {
            println!("Unknown transaction type: {:?}", transaction);
        }
    }
}

async fn handle_deposit(
    client_accounts: &Arc<RwLock<HashMap<u16, Arc<Mutex<ClientAccount>>>>>,
    transaction: &Transaction,
) {
    let mut client_accounts = client_accounts.write().await;
    if let Some(client_account_mutex) =
        client_accounts.get_mut(&transaction.client)
    {
        let mut account = client_account_mutex.lock().await;
        if account.locked {
            return;
        }
        if let Some(amount) = transaction.amount {
            account.available += amount;
            account.total += amount;
        }
    }
}

async fn handle_withdrawal(
    client_accounts: &Arc<RwLock<HashMap<u16, Arc<Mutex<ClientAccount>>>>>,
    transaction: &Transaction,
) {
    let mut client_accounts = client_accounts.write().await;
    if let Some(client_account_mutex) =
        client_accounts.get_mut(&transaction.client)
    {
        let mut account = client_account_mutex.lock().await;
        if account.locked {
            return;
        }
        if let Some(amount) = transaction.amount {
            if account.available >= amount {
                account.available -= amount;
                account.total -= amount;
            }
        }
    }
}

async fn handle_dispute(
    client_accounts: &Arc<RwLock<HashMap<u16, Arc<Mutex<ClientAccount>>>>>,
    transaction: &Transaction,
    txs: &Arc<RwLock<HashMap<u32, Transaction>>>,
) {
    let mut client_accounts = client_accounts.write().await;
    if let Some(client_account_mutex) =
        client_accounts.get_mut(&transaction.client)
    {
        let mut account = client_account_mutex.lock().await;
        if account.locked {
            return;
        }
        if let Some(disputed_tx) = txs.read().await.get(&transaction.tx) {
            let mut disputed = disputed_tx.disputed.lock().await;
            if !*disputed {
                if let Some(amount) = disputed_tx.amount {
                    account.available -= amount;
                    account.held += amount;
                    *disputed = true;
                }
            }
        }
    }
}

async fn handle_resolve(
    client_accounts: &Arc<RwLock<HashMap<u16, Arc<Mutex<ClientAccount>>>>>,
    transaction: &Transaction,
    txs: &Arc<RwLock<HashMap<u32, Transaction>>>,
) {
    let mut client_accounts = client_accounts.write().await;
    if let Some(client_account_mutex) =
        client_accounts.get_mut(&transaction.client)
    {
        let mut account = client_account_mutex.lock().await;
        if let Some(disputed_tx) = txs.read().await.get(&transaction.tx) {
            let mut disputed = disputed_tx.disputed.lock().await;
            if *disputed {
                if let Some(amount) = disputed_tx.amount {
                    account.available += amount;
                    account.held -= amount;
                    *disputed = false;
                }
            }
        }
    }
}

async fn handle_chargeback(
    client_accounts: &Arc<RwLock<HashMap<u16, Arc<Mutex<ClientAccount>>>>>,
    transaction: &Transaction,
    txs: &Arc<RwLock<HashMap<u32, Transaction>>>,
) {
    let mut client_accounts = client_accounts.write().await;
    if let Some(client_account_mutex) =
        client_accounts.get_mut(&transaction.client)
    {
        let mut account = client_account_mutex.lock().await;
        if let Some(disputed_tx) = txs.read().await.get(&transaction.tx) {
            let disputed = disputed_tx.disputed.lock().await;
            if *disputed {
                if let Some(amount) = disputed_tx.amount {
                    account.held -= amount;
                    account.total -= amount;
                    account.locked = true;
                }
            }
        }
    }
}
async fn print_client_accounts(
    client_accounts: Arc<RwLock<HashMap<u16, Arc<Mutex<ClientAccount>>>>>,
) {
    let mut tasks = vec![];

    let read_lock = client_accounts.read().await;

    for (client_id, account_mutex) in read_lock.iter() {
        let account_mutex_clone = account_mutex.clone();
        let client_id_clone = *client_id;
        let task = tokio::spawn(async move {
            let account = account_mutex_clone.lock().await;
            println!(
                "{},{:.4},{:.4},{:.4},{}",
                client_id_clone,
                account.available,
                account.held,
                account.total,
                account.locked
            );
        });
        tasks.push(task);
    }

    for task in tasks {
        task.await.expect("Printing task failed");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::main]
    #[test]
    async fn test_deposit() {
        let client_accounts =
            process_transaction("test_cases/test_deposit.csv")
                .await
                .unwrap();

        let account1 = client_accounts.read().await;
        let account1 = account1.get(&1).unwrap().lock().await;
        assert_eq!(account1.available, 100.0);
        assert_eq!(account1.held, 0.0);
        assert_eq!(account1.total, 100.0);
        assert_eq!(account1.locked, false);

        let account2 = client_accounts.read().await;
        let account2 = account2.get(&2).unwrap().lock().await;
        assert_eq!(account2.available, 200.0);
        assert_eq!(account2.held, 0.0);
        assert_eq!(account2.total, 200.0);
        assert_eq!(account2.locked, false);
    }

    #[tokio::main]
    #[test]
    async fn test_withdrawal() {
        let client_accounts =
            process_transaction("test_cases/test_withdrawal.csv")
                .await
                .unwrap();

        let account1 = client_accounts.read().await;
        let account1 = account1.get(&1).unwrap().lock().await;
        assert_eq!(account1.available, 50.0);
        assert_eq!(account1.held, 0.0);
        assert_eq!(account1.total, 50.0);
        assert_eq!(account1.locked, false);
    }

    #[tokio::main]
    #[test]
    async fn test_dispute() {
        let client_accounts =
            process_transaction("test_cases/test_dispute.csv")
                .await
                .unwrap();

        let account1 = client_accounts.read().await;
        let account1 = account1.get(&1).unwrap().lock().await;
        assert_eq!(account1.available, 0.0);
        assert_eq!(account1.held, 100.0);
        assert_eq!(account1.total, 100.0);
        assert_eq!(account1.locked, false);

        let account2 = client_accounts.read().await;
        let account2 = account2.get(&2).unwrap().lock().await;
        assert_eq!(account2.available, 200.0);
        assert_eq!(account2.held, 0.0);
        assert_eq!(account2.total, 200.0);
        assert_eq!(account2.locked, false);
    }

    #[tokio::main]
    #[test]
    async fn test_resolve() {
        let client_accounts =
            process_transaction("test_cases/test_resolve.csv")
                .await
                .unwrap();

        let account1 = client_accounts.read().await;
        let account1 = account1.get(&1).unwrap().lock().await;
        assert_eq!(account1.available, 100.0);
        assert_eq!(account1.held, 0.0);
        assert_eq!(account1.total, 100.0);
        assert_eq!(account1.locked, false);

        let account2 = client_accounts.read().await;
        let account2 = account2.get(&2).unwrap().lock().await;
        assert_eq!(account2.available, 200.0);
        assert_eq!(account2.held, 0.0);
        assert_eq!(account2.total, 200.0);
        assert_eq!(account2.locked, false);
    }

    #[tokio::main]
    #[test]
    async fn test_chargeback() {
        let client_accounts =
            process_transaction("test_cases/test_chargeback.csv")
                .await
                .unwrap();

        let account1 = client_accounts.read().await;
        let account1 = account1.get(&1).unwrap().lock().await;
        assert_eq!(account1.available, 0.0);
        assert_eq!(account1.held, 0.0);
        assert_eq!(account1.total, 0.0);
        assert_eq!(account1.locked, true);

        let account2 = client_accounts.read().await;
        let account2 = account2.get(&2).unwrap().lock().await;
        assert_eq!(account2.available, 200.0);
        assert_eq!(account2.held, 0.0);
        assert_eq!(account2.total, 200.0);
        assert_eq!(account2.locked, false);
    }

    #[tokio::main]
    #[test]
    async fn mismatched_dispute_resolve() {
        let client_accounts =
            process_transaction("test_cases/mismatch_dispute_resolve.csv")
                .await
                .unwrap();

        let account1 = client_accounts.read().await;
        let account1 = account1.get(&1).unwrap().lock().await;
        assert_eq!(account1.available, 200.0);
        assert_eq!(account1.held, 100.0);
        assert_eq!(account1.total, 300.0);
        assert_eq!(account1.locked, false);
    }

    #[tokio::main]
    #[test]
    async fn test_deposit_after_frozen() {
        let client_accounts =
            process_transaction("test_cases/test_deposit_after_frozen.csv")
                .await
                .unwrap();

        let account1 = client_accounts.read().await;
        let account1 = account1.get(&1).unwrap().lock().await;
        assert_eq!(account1.available, 0.0);
        assert_eq!(account1.held, 0.0);
        assert_eq!(account1.total, 0.0);
        assert_eq!(account1.locked, true);

        let account2 = client_accounts.read().await;
        let account2 = account2.get(&2).unwrap().lock().await;
        assert_eq!(account2.available, 200.0);
        assert_eq!(account2.held, 0.0);
        assert_eq!(account2.total, 200.0);
        assert_eq!(account2.locked, false);
    }
}
