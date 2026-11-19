use crate::prelude::*;
use tokio::sync::mpsc;
use std::sync::Mutex;

pub struct CollectorService {
    pub rx: Mutex<Option<mpsc::Receiver<Message>>>,
}

impl CollectorService {
    pub fn new() -> (Self, mpsc::Sender<Message>) {
        let (tx, rx) = mpsc::channel(100);
        let cs = CollectorService { rx: Mutex::new(Some(rx)) };
        (cs, tx)
    }
}
pub struct Message ( pub String);


#[async_trait]
impl BackgroundService for CollectorService {
    async fn start(&self, mut shutdown: ShutdownWatch) {
        let mut rx = self.rx.lock().unwrap()
            .take()
            .expect("Logger already running");

        loop {
            tokio::select! {
                msg = rx.recv() => {
                    match msg {
                        Some(message) => print!("{}\n", message.0),
                        None => break,
                    }
                }

                result = shutdown.changed() => {
                    if result.is_ok() {
                        rx.close();

                        while let Some(message) = rx.recv().await {
                            println!("{}", message.0);
                        }
                    }

                    break;
                }
            }
        }
    }
}



