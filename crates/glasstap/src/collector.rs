use crate::prelude::*;
use tokio::sync::mpsc;
use std::sync::Mutex;


pub trait Formatter<T>: Send + Sync + 'static {
    fn format(&self, event: &T) -> Result<String, anyhow::Error>;
}



//pub struct CollectorService {
//    pub rx: Mutex<Option<mpsc::Receiver<Message>>>,
//}


pub struct CollectorService<T, F> {
    rx: Mutex<Option<mpsc::Receiver<T>>>,
    formatter: F,
}

impl<T, F> CollectorService<T, F>
where
    F: Formatter<T>,
{
    pub fn new(capacity: usize, formatter: F) -> (Self, mpsc::Sender<T>) {
        let (tx, rx) = mpsc::channel(capacity);
        (
            Self {
                rx: Mutex::new(Some(rx)),
                formatter,
            },
            tx,
        )
    }
}

// pub struct Message ( pub String);


#[async_trait]
impl<T, F> BackgroundService for CollectorService<T, F>
where
    T: Send + 'static,
    F: Formatter<T>,
{
    async fn start(&self, mut shutdown: ShutdownWatch) {
        let mut rx = self.rx.lock().unwrap()
            .take()
            .expect("Collector already running");

        loop {
            tokio::select! {
                msg = rx.recv() => {
                    match msg {
                        Some(event) => {
                            match self.formatter.format(&event) {
                                Ok(output) => println!("{output}"),
                                Err(err) => {
                                    eprintln!("Formatter error: {err}");
                                }
                            }
                        }

                        None => break,
                    }
                }
                result = shutdown.changed() => {
                    if result.is_ok() {
                        rx.close();

                        while let Some(event) = rx.recv().await {
                            match self.formatter.format(&event) {
                                Ok(output) => println!("{output}"),
                                Err(err) => {
                                    eprintln!("Formatter error: {err}");
                                }
                            }
                        }
                    }
                    break;
                }
            }
        }
    }
}



