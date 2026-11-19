use crate::prelude::*;

pub static RESOLVER: LazyLock<TokioResolver> = LazyLock::new(|| {
    init_resolver()
});


pub struct ServiceDiscovery {
    pub lb: Arc<ArcSwap<LoadBalancer<RoundRobin>>>,
}

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


#[async_trait]
impl BackgroundService for ServiceDiscovery {
    
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

                    // println!("Resolver: Checking for new upstreams...");                    
                    self.update_lb(&mut endpoints).await;
                    continue;                    
                }
            }
        }
    }
}

impl ServiceDiscovery {

        async fn update_lb(&self, endpoints: &mut Endpoints) {

        let service_name = match std::env::var("TARGET_SERVICE") {
            Ok(value) => value,
            Err(_) => "_glass-tap".to_string(),
        };

        let new = Endpoints::from_service(&service_name).await;

        if new.0 == endpoints.0 {
            // println!("No change in endpoints, load balancer remains the same.");
            return;
        }

        // println!("Updating load balancer with new endpoints: {:?}", new); - uncomment for debugging
        let new_lb = LoadBalancer::<RoundRobin>::try_from_iter(new.0.iter().map(|e| format!("{}:{}", e.ip, e.port))).expect("Failed to create load balancer");
        self.lb.store(Arc::new(new_lb));
        *endpoints = new;
    }
}