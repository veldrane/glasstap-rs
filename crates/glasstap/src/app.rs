use crate::prelude::*;
use crate::config::Config;
use tokio::sync::mpsc;
use crate::collector::Message;



pub struct AppState {
    pub lb: Arc<ArcSwap<LoadBalancer<RoundRobin>>>,
    pub tx: mpsc::Sender<Message>,
}


// Implement the update_lb method for LB to check for new endpoints and update the load balancer - contract with LB touple
impl AppState {

    pub fn build(_config: &Config) -> Self {

        let upstreams = match LoadBalancer::<RoundRobin>::try_from_iter(["127.0.0.1:8080"]) {
            Ok(upstreams) => Arc::new(ArcSwap::from_pointee(upstreams)),
            Err(_) => panic!("failed to build load balancer from upstreams"),
        };

        let tx = mpsc::channel(100).0;

        AppState { lb: upstreams, tx }
        
    }

    pub fn set_tx(&mut self, tx: mpsc::Sender<Message>) {
        self.tx = tx;
    }

}


// Endpoint parts - all stuff related to endpoints and their management
