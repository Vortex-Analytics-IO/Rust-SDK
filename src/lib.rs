use chrono::Utc;
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;
use std::time::Duration;
use uuid::Uuid;

// Data structures
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ValueWrapper {
    pub data: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TrackingData {
    pub name: String,
    pub value: String,
    pub identity: String,
    pub session_id: String,
    pub platform: String,
    pub app_version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub custom: Option<String>,
    pub timestamp: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Tracking {
    pub tenant_id: String,
    pub tracking: TrackingData,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct BatchedTracks {
    pub tracks: Vec<Tracking>,
}

// Internal state to be wrapped in Arc<Mutex>
struct State {
    tenant_id: String,
    url: String,
    platform: String,
    app_version: String,
    auto_batching: bool,
    auto_flush_interval_ms: u64,
    identity: String,
    session_id: String,
    custom_data: Option<String>,
    initialized: bool,
    server_alive: bool,
    is_server_checked: bool,
    internal_queue: Vec<Tracking>,
    manual_batched_tracks: Vec<Tracking>,
    verbose: bool,
}

pub struct AnalyticsManager {
    state: Arc<Mutex<State>>,
    client: Client,
    is_shutting_down: Arc<AtomicBool>,
}

static INSTANCE: OnceLock<AnalyticsManager> = OnceLock::new();

impl AnalyticsManager {
    /// Gets the global singleton instance.
    pub fn instance() -> &'static AnalyticsManager {
        INSTANCE.get_or_init(|| AnalyticsManager {
            state: Arc::new(Mutex::new(State {
                tenant_id: String::new(),
                url: "https://in.hintway.app".to_string(),
                platform: String::new(),
                app_version: "1.0.0".to_string(),
                auto_batching: false,
                auto_flush_interval_ms: 10000,
                identity: String::new(),
                session_id: String::new(),
                custom_data: None,
                initialized: false,
                server_alive: false,
                is_server_checked: false,
                internal_queue: Vec::new(),
                manual_batched_tracks: Vec::new(),
                verbose: false,
            })),
            // Default 5 second timeout for requests
            client: Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap(),
            is_shutting_down: Arc::new(AtomicBool::new(false)),
        })
    }

    pub fn set_verbose(&self, verbose: bool) {
        if let Ok(mut state) = self.state.lock() {
            state.verbose = verbose;
        }
    }

    fn hintway_log(&self, msg: &str) {
        let verbose = self.state.lock().map(|s| s.verbose).unwrap_or(false);
        if verbose {
            println!("[Hintway] {}", msg);
        }
    }

    pub fn init(
        &self,
        tenant_id: &str,
        url: &str,
        platform: &str,
        app_version: &str,
        auto_batching: bool,
        flush_interval_sec: u64,
        identity: Option<&str>,
    ) {
        let mut initialize_needed = false;
        {
            let mut state = self.state.lock().unwrap();
            if state.initialized {
                return;
            }

            state.tenant_id = tenant_id.to_string();
            state.url = url.trim_end_matches('/').to_string();
            state.platform = platform.to_string();
            state.app_version = app_version.to_string();
            state.auto_batching = auto_batching;
            state.auto_flush_interval_ms = flush_interval_sec * 1000;
            state.initialized = true;

            state.identity = match identity {
                Some(id) => id.to_string(),
                None => Self::get_persistent_identity(&state.verbose),
            };
            state.session_id = Uuid::new_v4().to_string();

            initialize_needed = true;

            if state.verbose {
                println!(
                    "[Hintway] Init called: tenantId={}, url={}, platform={}, appVersion={}, autoBatching={}, flushIntervalSec={}",
                    tenant_id, url, platform, app_version, auto_batching, flush_interval_sec
                );
            }
        }

        if initialize_needed {
            self.hintway_log("AnalyticsManager initialized");

            // Spawn background thread to check server availability
            let state_clone = Arc::clone(&self.state);
            let client_clone = self.client.clone();
            thread::spawn(move || {
                Self::check_server_availability(state_clone, client_clone);
            });

            self.track_event_string("app_started", "");
        }
    }

    fn get_persistent_identity(verbose: &bool) -> String {
        let path: PathBuf = ["analytics.id"].iter().collect();
        if path.exists() {
            if let Ok(id) = fs::read_to_string(&path) {
                if *verbose {
                    println!("[Hintway] Loaded persistent identity: {}", id);
                }
                return id;
            }
        }
        let new_id = Uuid::new_v4().to_string();
        let _ = fs::write(path, &new_id);
        if *verbose {
            println!("[Hintway] Generated new persistent identity: {}", new_id);
        }
        new_id
    }

    fn create_tracking(&self, name: String, value: String) -> Option<Tracking> {
        let state = self.state.lock().unwrap();
        if !state.initialized {
            return None;
        }

        let tracking_data = TrackingData {
            name,
            value,
            identity: state.identity.clone(),
            session_id: state.session_id.clone(),
            platform: state.platform.clone(),
            app_version: state.app_version.clone(),
            custom: state.custom_data.clone(),
            timestamp: Utc::now().to_rfc3339(),
        };

        Some(Tracking {
            tenant_id: state.tenant_id.clone(),
            tracking: tracking_data,
        })
    }

    fn check_server_availability(state: Arc<Mutex<State>>, client: Client) {
        let (url, tenant_id, auto_batching) = {
            let s = state.lock().unwrap();
            (s.url.clone(), s.tenant_id.clone(), s.auto_batching)
        };

        if url.is_empty() {
            return;
        }

        let validate_url = format!("{}/validate?tenant_id={}", url, tenant_id);
        let verbose = state.lock().unwrap().verbose;
        if verbose {
            println!("[Hintway] Validating tenant at {}", validate_url);
        }

        let mut server_alive = false;
        match client.get(&validate_url).send() {
            Ok(response) if response.status().is_success() => {
                server_alive = true;
                if verbose {
                    println!("[Hintway] Tenant validation succeeded");
                }
            }
            Ok(response) => {
                if verbose {
                    println!("[Hintway] Tenant validation failed - HTTP {}", response.status());
                }
            }
            Err(e) => {
                if verbose {
                    println!("[Hintway] Tenant validation request failed: {}", e);
                }
            }
        }

        {
            let mut s = state.lock().unwrap();
            s.server_alive = server_alive;
            s.is_server_checked = true;
        }

        if server_alive {
            if auto_batching {
                AnalyticsManager::instance().start_auto_flush();
            } else {
                AnalyticsManager::instance().flush_internal_queue();
            }
        }
    }

    fn send_request(&self, endpoint: &str, data: &Value) -> bool {
        let url = {
            let s = self.state.lock().unwrap();
            format!("{}{}", s.url, endpoint)
        };

        self.hintway_log(&format!("Sending POST to {}", url));

        match self.client.post(&url).json(data).send() {
            Ok(res) if res.status().is_success() => {
                self.hintway_log(&format!("Request succeeded: {}", url));
                true
            }
            Ok(res) => {
                self.hintway_log(&format!("Request failed: {} | Status: {}", url, res.status()));
                false
            }
            Err(e) => {
                self.hintway_log(&format!("Request exception: {}", e));
                false
            }
        }
    }

    fn start_auto_flush(&self) {
        let state_clone = Arc::clone(&self.state);
        let shutdown_flag = Arc::clone(&self.is_shutting_down);

        let interval_ms = state_clone.lock().unwrap().auto_flush_interval_ms;

        thread::spawn(move || {
            // Sleep in small 100ms increments to allow for quick interruption during shutdown
            let sleep_chunk = Duration::from_millis(100);
            let mut elapsed = 0;

            while !shutdown_flag.load(Ordering::SeqCst) {
                thread::sleep(sleep_chunk);
                elapsed += 100;

                if elapsed >= interval_ms {
                    elapsed = 0;
                    let server_alive = state_clone.lock().unwrap().server_alive;
                    if server_alive {
                        AnalyticsManager::instance().flush_internal_queue();
                    }
                }
            }
        });
    }

    fn flush_internal_queue(&self) {
        let to_send: Vec<Tracking> = {
            let mut s = self.state.lock().unwrap();
            if s.internal_queue.is_empty() {
                return;
            }
            s.internal_queue.drain(..).collect()
        };

        let batch = BatchedTracks { tracks: to_send };
        self.hintway_log(&format!("Flushing internal queue with {} events", batch.tracks.len()));

        if let Ok(json_val) = serde_json::to_value(&batch) {
            self.send_request("/batch", &json_val);
        }
    }

    pub fn flush_manual_batch(&self) {
        thread::spawn(|| {
            let to_send: Vec<Tracking> = {
                let mut s = AnalyticsManager::instance().state.lock().unwrap();
                if s.manual_batched_tracks.is_empty() {
                    return;
                }
                s.manual_batched_tracks.drain(..).collect()
            };

            let batch = BatchedTracks { tracks: to_send };
            AnalyticsManager::instance().hintway_log(&format!(
                "Posting manual batch with {} events",
                batch.tracks.len()
            ));

            if let Ok(json_val) = serde_json::to_value(&batch) {
                AnalyticsManager::instance().send_request("/batch", &json_val);
            }
        });
    }

    pub fn set_custom_data(&self, custom_data: Option<HashMap<String, Value>>) {
        let mut s = self.state.lock().unwrap();
        if let Some(data) = custom_data {
            if !data.is_empty() {
                s.custom_data = Some(serde_json::to_string(&data).unwrap_or_default());
                return;
            }
        }
        s.custom_data = None;
    }

    pub fn track_event(&self, event_name: &str, props: HashMap<String, Value>) {
        if self.should_skip() {
            return;
        }
        let value = serde_json::to_string(&props).unwrap_or_default();
        self.process_track_event(event_name, value);
    }

    pub fn track_event_string(&self, event_name: &str, props: &str) {
        if self.should_skip() {
            return;
        }
        let value = if props.is_empty() {
            "".to_string()
        } else {
            let wrapper = ValueWrapper {
                data: props.to_string(),
            };
            serde_json::to_string(&wrapper).unwrap_or_default()
        };
        self.process_track_event(event_name, value);
    }

    fn should_skip(&self) -> bool {
        let s = self.state.lock().unwrap();
        !s.server_alive && s.is_server_checked && !s.auto_batching
    }

    fn process_track_event(&self, event_name: &str, value: String) {
        if let Some(t) = self.create_tracking(event_name.to_string(), value.clone()) {
            self.hintway_log(&format!("TrackEvent: {} value: {}", event_name, value));

            let (is_checked, auto_batching) = {
                let s = self.state.lock().unwrap();
                (s.is_server_checked, s.auto_batching)
            };

            if !is_checked || auto_batching {
                // Scope the lock so it drops before we log
                let queue_len = {
                    let mut s = self.state.lock().unwrap();
                    s.internal_queue.push(t);
                    s.internal_queue.len()
                };
                
                self.hintway_log(&format!(
                    "Event queued internally. Queue size: {}",
                    queue_len
                ));
            } else {
                if let Ok(json_val) = serde_json::to_value(&t) {
                    thread::spawn(move || {
                        AnalyticsManager::instance().send_request("/track", &json_val);
                    });
                }
            }
        }
    }

    pub fn batched_track_event(&self, event_name: &str, props: HashMap<String, Value>) {
        if !self.state.lock().unwrap().server_alive {
            return;
        }
        let value = serde_json::to_string(&props).unwrap_or_default();
        if let Some(tracking) = self.create_tracking(event_name.to_string(), value) {
            let batch_len = {
                let mut s = self.state.lock().unwrap();
                s.manual_batched_tracks.push(tracking);
                s.manual_batched_tracks.len()
            };
            
            self.hintway_log(&format!(
                "BatchedTrackEvent: {} (dict) added. Batch size: {}",
                event_name,
                batch_len
            ));
        }
    }

    pub fn batched_track_event_string(&self, event_name: &str, props: &str) {
        if !self.state.lock().unwrap().server_alive {
            return;
        }
        if let Some(tracking) = self.create_tracking(event_name.to_string(), props.to_string()) {
            let batch_len = {
                let mut s = self.state.lock().unwrap();
                s.manual_batched_tracks.push(tracking);
                s.manual_batched_tracks.len()
            };
            
            self.hintway_log(&format!(
                "BatchedTrackEvent: {} (string) added. Batch size: {}",
                event_name,
                batch_len
            ));
        }
    }

    pub fn shutdown(&self) {
        // Signal background threads to stop
        self.is_shutting_down.store(true, Ordering::SeqCst);

        let to_send = {
            let mut s = self.state.lock().unwrap();
            if let Some(exit_track) = self.create_tracking("app_exit".to_string(), "".to_string()) {
                s.manual_batched_tracks.push(exit_track);
            }

            if !s.internal_queue.is_empty() {
                let mut queue = std::mem::take(&mut s.internal_queue);
                s.manual_batched_tracks.append(&mut queue);
            }

            std::mem::take(&mut s.manual_batched_tracks)
        };

        if !to_send.is_empty() {
            self.hintway_log(&format!(
                "Attempting final flush before exit with {} events",
                to_send.len()
            ));
            let batch = BatchedTracks { tracks: to_send };
            
            if let Ok(json_val) = serde_json::to_value(&batch) {
                // Blocks the main thread up to the client timeout (5s) to guarantee flush
                let url = format!("{}/batch", self.state.lock().unwrap().url);
                let _ = self.client.post(&url).json(&json_val).send();
            }
        }
    }
}