use std::{sync::mpsc::Sender, thread, time::Duration};

/// Action execution debouncer, this is struct is built by calling [`debounce_action`]
pub struct Debouncer {
    tx: Sender<()>,
}

impl Debouncer {
    /// Invoke the action once, after the timeout elapsed.
    pub fn invoke(&self) {
        self.tx.send(()).unwrap();
    }
}

/// Creates a [`Debouncer`] for `action` with `timeout` duration.
pub fn debounce_action<F: 'static + FnMut() + Send + Sync>(
    timeout: Duration,
    mut action: F,
) -> Debouncer {
    let (tx, rx) = std::sync::mpsc::channel::<()>();

    thread::spawn(move || loop {
        if rx.recv().is_err() {
            break;
        }

        loop {
            match rx.recv_timeout(timeout) {
                Ok(_) => {
                    continue;
                }
                Err(_) => {
                    action();
                    break;
                }
            }
        }
    });

    Debouncer { tx }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;

    #[test]
    fn test_debouncer() {
        let count = Arc::new(Mutex::new(0));

        let c = count.clone();
        let debouncer = debounce_action(Duration::from_millis(100), move || {
            *c.lock().unwrap() += 1;
        });

        for _ in 0..1000 {
            debouncer.invoke();
        }

        thread::sleep(Duration::from_millis(150));
        assert_eq!(1, *count.lock().unwrap());

        for _ in 0..100 {
            debouncer.invoke();
        }

        thread::sleep(Duration::from_millis(250));
        assert_eq!(2, *count.lock().unwrap());
    }
}
