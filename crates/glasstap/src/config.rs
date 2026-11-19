#[derive(Clone, Debug)]
pub struct Config {
    pub target_service: String,
    pub k8s_domain: String,
    pub port: u16,
    pub output: String,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            target_service: "_glass-tap".to_string(),
            k8s_domain: "cluster.local".to_string(),
            port: 8080,
            output: "stdout".to_string(),
        }
    }
}