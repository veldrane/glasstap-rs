// Glass Tap - A simple HTTP proxy for Kubernetes service discovery and load balancing
pub use crate::app;

// std library modules
pub use std::net::IpAddr;
pub use std::sync::{Arc, LazyLock};
pub use std::fmt::Display;
pub use std::time::Duration;
pub use std::backtrace::Backtrace;
pub use std::panic;
pub use std::net::SocketAddr;

// Lock free atomic swap for loadbalancer target updates
pub use arc_swap::ArcSwap;


// Pingora stack
pub use pingora::prelude::*;
pub use pingora_load_balancing::{prelude::RoundRobin, LoadBalancer};
pub use pingora_proxy::{ ProxyHttp, Session, http_proxy_service};
pub use pingora::server::{Server, ShutdownWatch};
pub use pingora::services::background::BackgroundService;

// async traits support 
pub use async_trait::async_trait;

// helpers for working with IP networks - comparing etc
pub use ipnet::Ipv4Net;

// timer for async tasks - tokio backgroupd svc loop
pub use tokio::time::interval;

// DNS resolving modules
pub use hickory_resolver::TokioResolver;
pub use hickory_resolver::proto::rr::{RData, rdata::SRV };
pub use hickory_resolver::config::{ServerGroup, ResolverConfig, ResolverOpts};
pub use hickory_resolver::net::runtime::TokioRuntimeProvider;
