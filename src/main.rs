use std::sync::Arc;
use std::time::{Duration, Instant};
use std::collections::{HashMap, HashSet};
use pingora::prelude::*;
use pingora::proxy::HttpProxy;
use pingora::protocols::http::{HttpTask, ServerSession};
use metrics::{counter, gauge, histogram};
use metrics_exporter_prometheus::PrometheusBuilder;
use tokio::time::sleep;
use openssl::ssl::{SslAcceptor, SslFiletype, SslMethod};
use pingora::tls::listeners::Acceptor;
use tracing::{info, warn, error, instrument, Level};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use serde::{Deserialize, Serialize};
use futures::future::join_all;
use tokio::sync::RwLock;
use hyper::header::{HeaderValue, CONTENT_TYPE};
use async_trait::async_trait;
use futures::TryFuture;
use hyper::StatusCode;
use pingora::apps::http_app::HttpServer;
use pingora::modules::http::HttpModule;
use pingora::protocols::{GetProxyDigest, TcpKeepalive};
use pingora::server::configuration::ServerConf;
use pingora::upstreams::peer;
use pingora::upstreams::peer::Proxy;

// Spring Cloud Eureka response structures
#[derive(Deserialize, Debug)]
struct EurekaResponse {
    applications: Applications,
}

#[derive(Deserialize, Debug)]
struct Applications {
    application: Vec<Application>,
}

#[derive(Deserialize, Debug)]
struct Application {
    name: String,
    instance: Vec<Instance>,
}

#[derive(Deserialize, Debug)]
struct Instance {
    instanceId: String,
    hostName: String,
    port: Port,
    status: String,
    metadata: HashMap<String, String>,
}

#[derive(Deserialize, Debug)]
struct Port {
    #[serde(rename = "$")]
    number: u16,
}

// Distributed tracing configuration
#[derive(Clone, Debug)]
struct TracingConfig {
    enabled: bool,
    datadog_agent_host: String,
    service_name: String,
}

// Kubernetes service discovery configuration
#[derive(Clone, Debug)]
struct KubernetesConfig {
    enabled: bool,
    namespace: String,
    service_label_selector: String,
}

// Caching configuration
#[derive(Clone, Debug)]
struct CachingConfig {
    enabled: bool,
    max_cache_size: u64,
    ttl_seconds: u64,
}

// A/B testing configuration
#[derive(Clone, Debug)]
struct AbTestingConfig {
    enabled: bool,
    experiments: HashMap<String, AbExperiment>,
}

#[derive(Clone, Debug)]
struct AbExperiment {
    name: String,
    variants: Vec<AbVariant>,
}

#[derive(Clone, Debug)]
struct AbVariant {
    name: String,
    weight: u32,
    backend_suffix: String,
}

// Cache implementation
struct CacheLayer {
    cache: Cache<String, Vec<u8>>,
}

impl CacheLayer {
    async fn new(config: &CachingConfig) -> Self {
        let cache = Cache::builder()
            .max_capacity(config.max_cache_size)
            .time_to_live(Duration::from_secs(config.ttl_seconds))
            .build();

        Self { cache }
    }

    async fn get(&self, key: &str) -> Option<Vec<u8>> {
        self.cache.get(key).await
    }

    async fn set(&self, key: String, value: Vec<u8>) {
        self.cache.insert(key, value).await;
    }
}

// Enhanced service discovery with Kubernetes support
// Extracted error types for better error handling
#[derive(Debug, thiserror::Error)]
enum ProxyError {
    #[error("Service discovery failed: {0}")]
    ServiceDiscovery(String),
    #[error("No healthy backends available")]
    NoHealthyBackends,
    #[error("Invalid service path")]
    InvalidServicePath,
    #[error("Rate limit exceeded")]
    RateLimitExceeded,
}

// Result type alias for proxy operations
type ProxyResult<T> = Result<T, ProxyError>;

// Enhanced service discovery with early returns
impl EnhancedServiceDiscovery {
    async fn refresh_kubernetes_services(&self) -> ProxyResult<()> {
        let client = match &self.kube_client {
            None => return Ok(()), // Early return if Kubernetes is not configured
            Some(client) => client,
        };

        let services: Api<Service> = Api::namespaced(
            client.clone(),
            &self.kube_config.namespace,
        );

        let lp = ListParams::default()
            .labels(&self.kube_config.service_label_selector);

        let service_list = services.list(&lp)
            .await
            .map_err(|e| ProxyError::ServiceDiscovery(e.to_string()))?;

        let service_map = self.build_service_map(service_list.items).await?;

        let mut services = self.services.write().await;
        *services = service_map;

        Ok(())
    }

    // Extracted service map building logic
    async fn build_service_map(
        &self,
        services: Vec<Service>
    ) -> ProxyResult<HashMap<String, Vec<Instance>>> {
        let mut service_map = HashMap::with_capacity(services.len());

        for svc in services {
            let instance = self.create_instance_from_service(&svc)?;

            if let Some(name) = svc.metadata.name {
                service_map.entry(name)
                    .or_insert_with(Vec::new)
                    .push(instance);
            }
        }

        Ok(service_map)
    }

    // Extracted instance creation logic
    fn create_instance_from_service(&self, svc: &Service) -> ProxyResult<Instance> {
        let spec = svc.spec.as_ref()
            .ok_or_else(|| ProxyError::ServiceDiscovery("Missing service spec".to_string()))?;

        let first_port = spec.ports.as_ref()
            .and_then(|ports| ports.first())
            .ok_or_else(|| ProxyError::ServiceDiscovery("No ports defined".to_string()))?;

        let name = svc.metadata.name.clone()
            .ok_or_else(|| ProxyError::ServiceDiscovery("Missing service name".to_string()))?;

        Ok(Instance {
            instanceId: name.clone(),
            hostName: format!(
                "{}.{}.svc.cluster.local",
                name,
                self.kube_config.namespace
            ),
            port: Port {
                number: first_port.port as u16,
            },
            status: "UP".to_string(),
            metadata: HashMap::new(),
        })
    }
}

// Optimized cache implementation with connection pooling
struct CacheLayer {
    cache: Cache<String, Arc<Vec<u8>>>,
    pool: deadpool::Pool<deadpool::managed::Object<CacheConnection>>,
}

impl CacheLayer {
    async fn new(config: &CachingConfig) -> Self {
        let cache = Cache::builder()
            .max_capacity(config.max_cache_size)
            .time_to_live(Duration::from_secs(config.ttl_seconds))
            .build();

        let pool = deadpool::Pool::builder()
            .max_size(32)
            .build(CacheManagerConfig {})
            .expect("Failed to create connection pool");

        Self { cache, pool }
    }

    async fn get(&self, key: &str) -> Option<Arc<Vec<u8>>> {
        let conn = self.pool.get().await.ok()?;
        self.cache.get(key).await
    }

    async fn set(&self, key: String, value: Vec<u8>) {
        let conn = match self.pool.get().await {
            Ok(conn) => conn,
            Err(e) => {
                error!("Failed to get cache connection: {}", e);
                return;
            }
        };

        self.cache.insert(key, Arc::new(value)).await;
    }
}

// Optimized A/B testing implementation
impl EnterpriseProxy {
    fn select_ab_variant(&self, service_name: &str) -> Option<&AbVariant> {
        if !self.ab_testing_config.enabled {
            return None;
        }

        let experiment = self.ab_testing_config.experiments.get(service_name)?;

        let total_weight: u32 = experiment.variants.iter()
            .map(|v| v.weight)
            .sum();

        if total_weight == 0 {
            return None;
        }

        let selected_weight = rand::thread_rng().gen_range(0..total_weight);
        self.find_variant(experiment, selected_weight)
    }

    fn find_variant(&self, experiment: &AbExperiment, selected_weight: u32) -> Option<&AbVariant> {
        let mut cumulative_weight = 0;

        experiment.variants.iter()
            .find(|variant| {
                cumulative_weight += variant.weight;
                selected_weight < cumulative_weight
            })
    }
}

#[async_trait]
impl ProxyHttp for EnterpriseProxy {
    #[instrument(skip(self, session))]
    async fn upstream_peer<'a>(&'a self, session: &'a mut ServerSession, _ctx: <EnterpriseProxy as HttpProxy>::CTX) -> Result<Box<HttpPeer>, pingora::Error> {
        let start_time = Instant::now();

        // Create tracing span if enabled
        let _span = self.tracer.as_ref().map(|tracer| tracer.start("proxy_request"));

        // Extract service path
        let service_name = self.extract_service_name(session)?;

        // Try cache first if enabled
        if let Some(cached_response) = self.try_cache(service_name, session).await? {
            return Ok(cached_response);
        }

        // Get backend with A/B testing if enabled
        let backend = self.get_backend_with_ab_testing(service_name).await?;

        // Create and configure peer
        let peer = self.create_configured_peer(backend).await?;

        // Record metrics
        self.record_metrics(&backend, start_time.elapsed()).await;

        Ok(Box::new(peer))
    }
}

// Helper implementations for cleaner proxy logic
impl EnterpriseProxy {
    fn extract_service_name(&self, session: &Session) -> ProxyResult<&str> {
        session.req_header()
            .uri
            .path()
            .split('/')
            .nth(1)
            .ok_or(ProxyError::InvalidServicePath)
    }

    async fn try_cache(&self, service_name: &str, session: &mut Session)
                       -> ProxyResult<Option<Box<HttpPeer>>> {
        let Some(cache) = &self.cache_layer else {
            return Ok(None);
        };

        let cache_key = format!("{}{}", service_name, session.req_header().uri.path());

        if let Some(cached_response) = cache.get(&cache_key).await {
            session.set_response_body(cached_response.to_vec());
            return Ok(Some(Box::new(HttpPeer::new("cache".to_string(), 0))));
        }

        Ok(None)
    }

    async fn get_backend_with_ab_testing(&self, service_name: &str) -> ProxyResult<String> {
        let base_backend = self.load_balancer
            .get_backend(service_name)
            .await
            .ok_or(ProxyError::NoHealthyBackends)?;

        Ok(match self.select_ab_variant(service_name) {
            Some(variant) => format!("{}-{}", base_backend, variant.backend_suffix),
            None => base_backend,
        })
    }

    async fn create_configured_peer(&self, backend: String) -> ProxyResult<HttpPeer> {

        let mut peer = HttpPeer::new(backend.clone(), 80);
        let mut peer_options = peer::PeerOptions::new();
        peer_options.tcp_fast_open = true;

        // Apply performance optimizations
        peer_options.tcp_keepalive = Some(TcpKeepalive {
            idle: Duration::from_secs(300),
            interval: Duration::from_secs(5),
            count: 5,
        });


        Ok(peer)
    }

    async fn record_metrics(&self, backend: &str, response_time: Duration) {
        let mut metrics = self.metrics.write().await;
        let backend_metrics = metrics.entry(backend.to_string())
            .or_insert_with(BackendMetrics::new);

        backend_metrics.request_count += 1;
        backend_metrics.response_times.push(response_time.as_secs_f64());
        if backend_metrics.response_times.len() > 1000 {
            backend_metrics.response_times.remove(0);
        }

        // Export metrics to Prometheus
        gauge!("backend_response_time",
            &[("backend", backend.to_string()),
                ("response_time", format!("{}!", response_time.as_secs_f64()))],
        );
    }
}

// Circuit Breaker
#[derive(Clone, Debug)]
struct CircuitBreakerConfig {
    failure_threshold: u32,
    success_threshold: u32,
    timeout: Duration,
}

// Backend Stats
#[derive(Clone, Debug)]
struct BackendStats {
    connections: usize,
    errors: usize,
    last_checked: Instant,
}

// Enhanced backend configuration
#[derive(Clone, Debug)]
struct BackendConfig {
    name: String,
    health_check_path: String,
    max_connections: usize,
    circuit_breaker_config: CircuitBreakerConfig,
    response_timeout: Duration,
}

// Traffic shaping configuration
#[derive(Clone, Debug)]
struct TrafficConfig {
    rate_limit: u32,  // requests per second
    burst_size: u32,
    connection_limit: u32,
}

// Enhanced metrics for high-throughput monitoring
#[derive(Debug)]
struct BackendMetrics {
    request_count: u64,
    error_count: u64,
    timeout_count: u64,
    response_times: Vec<f64>,
    last_minute_requests: Vec<Instant>,
    peak_rps: f64,
}

impl BackendMetrics {
    fn new() -> Self {
        Self {
            request_count: 0,
            error_count: 0,
            timeout_count: 0,
            response_times: Vec::with_capacity(1000),  // Rolling window
            last_minute_requests: Vec::new(),
            peak_rps: 0.0,
        }
    }

    fn update_rps(&mut self) {
        let now = Instant::now();
        self.last_minute_requests.retain(|&time| now.duration_since(time) <= Duration::from_secs(60));
        let current_rps = self.last_minute_requests.len() as f64 / 60.0;
        self.peak_rps = self.peak_rps.max(current_rps);
    }
}

// Service discovery manager
struct ServiceDiscovery {
    eureka_client: reqwest::Client,
    eureka_urls: Vec<String>,
    services: Arc<RwLock<HashMap<String, Vec<Instance>>>>,
    refresh_interval: Duration,
}

impl ServiceDiscovery {
    fn new(eureka_urls: Vec<String>) -> Self {
        Self {
            eureka_client: reqwest::Client::new(),
            eureka_urls,
            services: Arc::new(RwLock::new(HashMap::new())),
            refresh_interval: Duration::from_secs(30),
        }
    }

    #[instrument(skip(self))]
    async fn refresh_services(&self) -> Result<(), reqwest::Error> {
        for eureka_url in &self.eureka_urls {
            match self.eureka_client.get(format!("{}/eureka/apps", eureka_url))
                .header(CONTENT_TYPE, "application/json")
                .send()
                .await
            {
                Ok(response) => {
                    if let Ok(eureka_response) = response.json::<EurekaResponse>().await {
                        let mut services = self.services.write().await;
                        for app in eureka_response.applications.application {
                            services.insert(app.name, app.instance);
                        }
                        info!("Successfully refreshed service registry from Eureka");
                        return Ok(());
                    }
                }
                Err(e) => {
                    error!("Failed to fetch from Eureka: {}", e);
                    continue;
                }
            }
        }
        Err(reqwest::Error::from(<StatusCode as TryInto<Error>>::Error::new(
            std::io::ErrorKind::Other,
            "All Eureka endpoints failed",
        )))
    }

    async fn start_refresh_loop(self: Arc<Self>) {
        loop {
            if let Err(e) = self.refresh_services().await {
                error!("Service refresh failed: {}", e);
            }
            sleep(self.refresh_interval).await;
        }
    }
}

// Enhanced load balancer with service discovery
struct EnterpriseLoadBalancer {
    backends: Arc<RwLock<HashMap<String, BackendStats>>>,
    service_discovery: Arc<ServiceDiscovery>,
    metrics: Arc<RwLock<HashMap<String, BackendMetrics>>>,
    traffic_config: TrafficConfig,
}

impl EnterpriseLoadBalancer {
    fn new(
        eureka_urls: Vec<String>,
        traffic_config: TrafficConfig,
    ) -> Self {
        let service_discovery = Arc::new(ServiceDiscovery::new(eureka_urls));
        let sd_clone = service_discovery.clone();
        
        // Start service discovery refresh loop
        tokio::spawn(async move {
            sd_clone.start_refresh_loop().await;
        });

        Self {
            backends: Arc::new(RwLock::new(HashMap::new())),
            service_discovery,
            metrics: Arc::new(RwLock::new(HashMap::new())),
            traffic_config,
        }
    }

    #[instrument(skip(self))]
    async fn get_backend(&self, service_name: &str) -> Option<String> {
        let services = self.service_discovery.services.read().await;
        let instances = services.get(service_name)?;
        
        // Filter healthy instances
        let healthy_instances: Vec<_> = instances.iter()
            .filter(|i| i.status == "UP")
            .collect();

        if healthy_instances.is_empty() {
            return None;
        }

        // Load balancing with connection counting
        let backends = self.backends.read().await;
        let selected = healthy_instances.iter()
            .min_by_key(|i| backends
                .get(&i.instanceId)
                .map(|stats| stats.connections)
                .unwrap_or(0))?;

        Some(format!("http://{}:{}", selected.hostName, selected.port.number))
    }

    #[instrument(skip(self))]
    async fn record_metrics(&self, backend: &str, response_time: Duration, success: bool) {
        let mut metrics = self.metrics.write().await;
        let backend_metrics = metrics.entry(backend.to_string())
            .or_insert_with(BackendMetrics::new);

        backend_metrics.request_count += 1;
        if !success {
            backend_metrics.error_count += 1;
        }
        backend_metrics.response_times.push(response_time.as_secs_f64());
        if backend_metrics.response_times.len() > 1000 {
            backend_metrics.response_times.remove(0);
        }
        backend_metrics.last_minute_requests.push(Instant::now());
        backend_metrics.update_rps();

        // Export metrics to Prometheus
        gauge!("backend_response_time",
            &[("backend", backend.to_string()),
                ("response_time", format!("{}!", response_time.as_secs_f64()))],
        );
        gauge!("backend_rps",
            &[("backend", backend.to_string()),
                ("peak_rps", format!("{}!", backend_metrics.peak_rps))],
        );
    }
}

// Enhanced proxy service
struct EnterpriseProxy {
    load_balancer: Arc<EnterpriseLoadBalancer>,
    timeout: Duration,
}

#[async_trait]
impl HttpModule for EnterpriseProxy {
    #[instrument(skip(self, session))]
    async fn upstream_peer<'a>(&'a self, session: &'a mut ServerSession, _ctx: <EnterpriseProxy as HttpProxy>::CTX) -> Result<Box<HttpPeer>, pingora::Error> {
        let start_time = Instant::now();
        
        // Extract service name from request path
        let path = session.req_header().uri.path();
        let service_name = path.split('/').nth(1)
            .ok_or_else(|| Error::err(ErrorType::new("InvalidServicePath".into())))?;

        let backend = self.load_balancer.get_backend(service_name).await
            .ok_or_else(|| Error::err(ErrorType::new("NoHealthyServiceFound".into())))?;

        // Rate limiting check
        let metrics = self.load_balancer.metrics.read().await;
        if let Some(backend_metrics) = metrics.get(&backend) {
            let current_rps = backend_metrics.last_minute_requests.len() as f64 / 60.0;
            if current_rps > self.load_balancer.traffic_config.rate_limit as f64 {
                return Err(*Error::err("Rate limit exceeded".into()));
            }
        }

        let mut peer = HttpPeer::new(backend.clone(), true, service_name.to_string());
        let mut peer_options = peer::PeerOptions::new();
        peer_options.connection_timeout = Some(self.timeout);

        // Apply optimizations based on load
        let metrics = self.load_balancer.metrics.read().await;
        if let Some(backend_metrics) = metrics.get(&backend) {
            if backend_metrics.peak_rps > 500.0 {  // High load optimizations
                peer_options.tcp_fast_open = true;
                // peer.set_tcp_quickack(true);
                peer_options.tcp_keepalive = Some(TcpKeepalive {
                    idle: Duration::from_secs(30),
                    interval: Duration::from_secs(5),
                    count: 5,
                });
            }
        }

        histogram!("backend_selection_time_ms",
            [("backend", backend),
                ("elapsed_time_ms", format!("{}!", start_time.elapsed().as_secs_f64() * 1000.00))],
        );

        Ok(Box::new(peer))
    }
}

#[instrument]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize enhanced logging
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(
            std::env::var("RUST_LOG")
                .unwrap_or_else(|_| "info,pingora=debug".into()),
        ))
        .with(
            tracing_subscriber::fmt::layer()
                .with_thread_ids(true)
                .with_target(false)
                .with_file(true)
                .with_line_number(true)
        )
        .init();

    let mut server = Server::new(None)?;
    
    // Configure for high throughput
    server.set_num_workers(num_cpus::get() * 2);  // 2 workers per CPU core
    server.set_tcp_nodelay(true);
    server.set_tcp_keepalive(Some(Duration::from_secs(60)));

    // Configure TLS with modern cipher suites
    let mut tls_acceptor = SslAcceptor::mozilla_modern(SslMethod::tls())?;
    tls_acceptor.set_private_key_file("key.pem", SslFiletype::PEM)?;
    tls_acceptor.set_certificate_chain_file("cert.pem")?;
    let tls = Acceptor::new(tls_acceptor.build());

    // Configure Eureka endpoints
    let eureka_urls = vec![
        "http://eureka1:8761".to_string(),
        "http://eureka2:8761".to_string(),
    ];

    // Configure traffic shaping for high throughput
    let traffic_config = TrafficConfig {
        rate_limit: 2000,  // 2k requests per second per backend
        burst_size: 500,   // Allow bursts up to 500 requests
        connection_limit: 10000,  // Max 10k concurrent connections
    };

    // Create proxy service
    let proxy = EnterpriseProxy {
        load_balancer: Arc::new(EnterpriseLoadBalancer::new(
            eureka_urls,
            traffic_config,
        )),
        timeout: Duration::from_secs(60),
    };
    let proxy_arc = Arc::new(proxy);

    // Add HTTP service
    let mut http_proxy = HttpProxy::new(proxy_arc.clone());
    http_proxy.set_listen("0.0.0.0:8080".parse()?);
    server.add_service(http_proxy);

    // Add HTTPS service
    let mut https_proxy = HttpProxy::new(proxy_arc, Arc::new(ServerConf::new()));
    https_proxy.set_listen("0.0.0.0:8443".parse()?);
    server.add_service(https_proxy);

    info!("Starting enterprise proxy server...");
    server.run_forever()?;
    // Ok(())
}
