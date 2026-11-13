// std library modules
use std::net::IpAddr;
use std::sync::{Arc, LazyLock};
use std::fmt::Display;
use std::time::Duration;
use std::backtrace::Backtrace;
use std::panic;

// Lock free atomic swap for loadbalancer target updates
use arc_swap::ArcSwap;


// Pingora stack
use pingora::prelude::*;
use pingora_load_balancing::{prelude::RoundRobin, LoadBalancer};
use pingora_proxy::{ ProxyHttp, Session, http_proxy_service};
use pingora::server::{Server, ShutdownWatch};
use pingora::services::background::BackgroundService;

// async traits support 
use async_trait::async_trait;

// helpers for working with IP networks - comparing etc
use ipnet::Ipv4Net;

// timer for async tasks - tokio backgroupd svc loop
use tokio::time::interval;

// DNS resolving modules
use hickory_resolver::TokioResolver;
use hickory_resolver::proto::rr::{RData, rdata::SRV };
use hickory_resolver::config::{ServerGroup, ResolverConfig, ResolverOpts};
use hickory_resolver::net::runtime::TokioRuntimeProvider;


pub struct LB(Arc<ArcSwap<LoadBalancer<RoundRobin>>>);

static POD_NET: LazyLock<Ipv4Net> =
    LazyLock::new(|| "10.244.0.0/16".parse().unwrap());

static RESOLVER: LazyLock<TokioResolver> = LazyLock::new(|| {
    init_resolver()
});


#[derive(Debug)]
pub enum Zone {
    Localhost,
    Private,
    Public,
    PodNet,
    None
}

impl From<IpAddr> for Zone {
    fn from(ip: IpAddr) -> Zone {
        match ip {
            IpAddr::V4(ipv4) => {
                if ipv4.is_loopback() {
                    Zone::Localhost
                } else if POD_NET.contains(&ipv4) {
                    Zone::PodNet
                } else if ipv4.is_private() {
                    Zone::Private
                } else {
                    Zone::Public
                }
            }
            IpAddr::V6(_) => Zone::Public,
        }
    }
}


impl Display for Zone {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let zone_str = match self {
            Zone::Localhost => "Localhost",
            Zone::Private => "Private",
            Zone::Public => "Public",
            Zone::PodNet => "PodNet",
            Zone::None => "None"
        };
        write!(f, "{}", zone_str)
    }
}

#[derive(Debug)]
pub struct Context {
    zone: Zone,
}

fn main() {
    init_panic();

    let mut my_server = Server::new(None).unwrap();
    my_server.bootstrap();

    let upstreams = match LoadBalancer::<RoundRobin>::try_from_iter(["127.0.0.1:80"]) {
        Ok(upstreams) => Arc::new(ArcSwap::from_pointee(upstreams)),
        Err(e) => {
            eprintln!("Failed to create load balancer: {e}");
            return;
        }
    };

    // let mut lb = http_proxy_service(&my_server.configuration, LB(Arc::clone(&upstreams)));
    let mut lb = http_proxy_service(&my_server.configuration, LB(Arc::clone(&upstreams)));
    let svc = background_service("ServiceDiscovery", LB(Arc::clone(&upstreams)));
    lb.add_tcp("0.0.0.0:8000");
    my_server.add_service(lb);
    my_server.add_service(svc);
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

// Initialize DNS resolver based on environment variable or system configuration
fn init_resolver() -> TokioResolver {

    let mut opts = ResolverOpts::default();

    opts.cache_size = 0;

    match std::env::var("DNS_SERVER") {
        Ok(value) => {
            let dns_ip: IpAddr = value
                .parse()
                .expect("Invalid DNS_SERVER IP address");

            let server_group = ServerGroup {
                ips: &[dns_ip],
                server_name: "dns.k8s",
                path: "/dns-query",
            };

            TokioResolver::builder_with_config(
                ResolverConfig::udp_and_tcp(&server_group),
                TokioRuntimeProvider::default(),
            )
            .with_options(opts)
            .build()
            .expect("Failed to create DNS resolver")
        }

        Err(_) => TokioResolver::builder_tokio()
            .expect("Failed to read system DNS configuration")
            .with_options(opts)
            .build()
            .expect("Failed to create system DNS resolver"),
    }
}


// Implement the ProxyHttp trait for LB - Main request http flow!
#[async_trait]
impl ProxyHttp for LB {

    type CTX = Context;
    
    fn new_ctx(&self) -> Self::CTX {
        Context { zone: Zone::None }
    }

    async fn request_filter(&self, session: &mut Session,  ctx: &mut Self::CTX) -> Result<bool> {

        let socket_addr = match session.client_addr() {
            Some(addr) => addr,
            None => {
                println!("No client address found, treating as Zone None");
                return Ok(false);
            }
        };

        let client_ip = match socket_addr.as_inet() {
            Some(inet) => inet.ip(),
            None => {
                println!("Client address is not an IP socket, treating as Zone None");
                return Ok(false);
            }
        };

        ctx.zone = Zone::from(client_ip);

        Ok(false)
    }

    async fn upstream_peer( &self, _session: &mut Session, ctx: &mut Self::CTX) -> Result<Box<HttpPeer>> {


        let upstream = {
            let lb = self.0.load();
            match lb.select(b"", 256) {
                Some(upstream) => upstream,
                None => return Err(Error::new(ErrorType::HTTPStatus(504)))
            }
        };
            
        println!("upstream peer is: {upstream:?}, zone is: {:?}", ctx.zone );

        let peer = Box::new(HttpPeer::new(upstream, false, "".to_string()));
        Ok(peer)
    }

    async fn upstream_request_filter(&self,_session: &mut Session, upstream_request: &mut RequestHeader,ctx: &mut Self::CTX,
    ) -> Result<()>
    {
        upstream_request.insert_header("X-Zone", ctx.zone.to_string())?;
        Ok(())
    }

}


// Implement the BackgroundService trait for upstream target discovery and load balancer updates
#[async_trait]
impl BackgroundService for LB {
    
    async fn start(&self, mut shutdown: ShutdownWatch) {

        let mut period = interval(Duration::from_secs(10));
        let mut endpoints = Endpoints::new();

        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    println!("Resolver: Shutdown requested.");
                    break;
                }
                _ = period.tick() => {

                    println!("Resolver: Checking for new upstreams...");                    
                    self.update_lb(&mut endpoints).await;
                    continue;                    
                }
            }
        }
    }
}

// Implement the update_lb method for LB to check for new endpoints and update the load balancer - contract with LB touple
impl LB {

    async fn update_lb(&self, endpoints: &mut Endpoints) {

        let new = Endpoints::from_service("_http._tcp.simple-api-hs.simple.svc.cluster.local").await;

        if new.0 == endpoints.0 {
            println!("No change in endpoints, load balancer remains the same.");
            return;
        }

        println!("Updating load balancer with new endpoints: {:?}", new);
        let new_lb = LoadBalancer::<RoundRobin>::try_from_iter(new.0.iter().map(|e| format!("{}:{}", e.ip, e.port))).expect("Failed to create load balancer");
        self.0.store(Arc::new(new_lb));
        *endpoints = new;
    }
}



// Endpoint parts - all stuff related to endpoints and their management
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endpoint {
    pub ip: String,
    pub port: u16,
}


#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endpoints(pub Vec<Endpoint>);


impl Endpoint {
    pub fn new(ip: String, port: u16) -> Self {
        Endpoint { ip, port }
    }

    pub async fn from_srv(srv_record: &SRV) -> Result<Self, String> {

        let ip = match RESOLVER.lookup_ip(srv_record.target.clone()).await {
            Ok(ip) => ip.iter().next().ok_or("No A record found".to_string())?,
            Err(e) => return Err(format!("Failed to resolve IP address: {}", e)),
        };

        let port = srv_record.port;
        let endpoint = Endpoint { ip: ip.to_string(), port };

        Ok(endpoint)

        }

}


impl Endpoints {
    pub fn new() -> Self {
        Endpoints(Vec::new())
    }

    pub fn add(&mut self, endpoint: Endpoint) {
        self.0.push(endpoint);
    }

    pub async fn from_service (service_name: &str) -> Self {

        let response = match  RESOLVER.srv_lookup(service_name).await {
            Ok(response) => response,
            Err(e) => {
                eprintln!("Failed to perform SRV lookup for {}: {}", service_name, e);
                return Endpoints::new();
            }
        };

        let records = response.answers();

        let mut endpoints = Endpoints::new();

        for record in records {
            match &record.data {
                RData::SRV(srv) => {
                    endpoints.0.push(Endpoint::from_srv(srv).await.expect("Failed to create endpoint from SRV record"));
                }
                _ => {
                    continue; // Ignore non-SRV records
                }
            }
        }

        // println!("DNS lookup response: {:?}", endpoints);
        endpoints
    }
}

