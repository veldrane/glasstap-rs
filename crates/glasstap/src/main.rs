use glasstap::prelude::*;
use glasstap::config::Config;
use glasstap::app::AppState;
use glasstap::discovery::ServiceDiscovery;
use glasstap::collector::CollectorService;





fn main() {
    init_panic();
    let mut app_state = AppState::build(&Config::default());

    let (cs, collector_tx) = CollectorService::new();
    app_state.set_tx(collector_tx);

    let collector = background_service("Collector", cs);
    let d = ServiceDiscovery { lb: Arc::clone(&app_state.lb) };
    let discovery = background_service("Discovery", d);


    let mut my_server = Server::new(None).unwrap();
    my_server.bootstrap();




    // let mut lb = http_proxy_service(&my_server.configuration, LB(Arc::clone(&upstreams)));
    let mut proxy = http_proxy_service(&my_server.configuration, app_state);

    proxy.add_tcp("0.0.0.0:8000");
    my_server.add_service(proxy);
    my_server.add_service(collector);
    my_server.add_service(discovery);
    my_server.run_forever();
}


// Initialize panic hook to print backtrace on panic and shutdown whole stack!
fn init_panic() {
    panic::set_hook(Box::new(|info| {
        eprintln!("fatal panic: {info}");
        eprintln!("{}", Backtrace::force_capture());
        std::process::abort();
    }));
}

