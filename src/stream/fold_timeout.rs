use std::{collections::HashSet, time::Duration};

use tokio::sync::mpsc::Receiver;

// TODO: implement a stream

pub fn fold_timeout(mut inbound: Receiver<String>, timeout: Duration) -> Receiver<HashSet<String>> {
    let (tx, rx) = tokio::sync::mpsc::channel(1);

    tokio::spawn(async move {
        let mut received: HashSet<String> = HashSet::new();

        loop {
            tokio::select! {
                maybe_value = inbound.recv() => {
                    match maybe_value {
                        Some(value) => { received.insert(value); },
                        None => break,
                    }
                }

                _ = tokio::time::sleep(timeout), if !received.is_empty() => {
                    if tx.send(std::mem::take(&mut received)).await.is_err() {
                        break;
                    }
                }
            }
        }

        if !tx.is_closed() && !received.is_empty() {
            let _ = tx.send(received).await;
        }
    });

    rx
}
