# AnalyticsManager Rust Documentation

`AnalyticsManager` is a thread-safe singleton struct responsible for sending analytics events to a remote server. It is designed for use in any Rust environment (CLI tools, game engines, desktop apps, servers, etc.).

It supports:

- Immediate event tracking
- Batched event tracking
- Automatic retry when the server becomes available
- Local file-based session and device identification
- Thread-safe queue management

## Dependencies

Add the following to your `Cargo.toml`:

```toml
[dependencies]
vortex_analytics = { git = "https://github.com/Vortex-Analytics-IO/Rust-SDK" }
```

The crate internally depends on:

- `reqwest` (blocking HTTP client with JSON support)
- `serde` / `serde_json` (serialization)
- `uuid` (device and session ID generation)
- `chrono` (timestamps)

## Initialization

`AnalyticsManager` is a singleton accessed via `AnalyticsManager::instance()`. It must be initialized explicitly in your application's entry point (e.g., `main()`).

### Basic Setup

```rust
use vortex_analytics::AnalyticsManager;

fn main() {
    // 1. Configure settings (Optional)
    // Set to true to queue events and send them periodically instead of immediately.
    // Default is false (immediate send).
    let enable_auto_batching = true;
    let flush_interval_seconds = 10;

    // 2. Initialize the Singleton
    AnalyticsManager::instance().init(
        "my_app",                             // tenant_id
        "https://in.vortexanalytics.io",      // url
        "Windows",                            // platform
        "1.0.0",                              // app_version
        enable_auto_batching,                 // auto_batching
        flush_interval_seconds,               // flush_interval_sec
    );
}
```

> ⚠️ **Important:** `init()` must be called before sending any events.

### Verbose Logging

Enable verbose logging to see internal debug output:

```rust
AnalyticsManager::instance().set_verbose(true);
```

### Internal Behavior

On initialization, the system:

1. Loads or generates a persistent device identifier (stored in a local `analytics.id` file).
2. Creates a new session ID.
3. Spawns a background thread to check server health.
4. Enables or disables analytics based on server availability.
5. Automatically tracks an `app_started` event.

If the server is unreachable, events are safely queued in memory until connectivity is restored.

## Tracking Events

### Simple Event

```rust
AnalyticsManager::instance().track_event_string("app_started", "");
```

### Event with String Payload

```rust
AnalyticsManager::instance().track_event_string("menu_opened", "settings");
```

### Event with Structured Data

```rust
use std::collections::HashMap;
use serde_json::Value;

let mut props = HashMap::new();
props.insert("level".to_string(), Value::from(5));
props.insert("difficulty".to_string(), Value::from("Hard"));
props.insert("time".to_string(), Value::from(123.4));

AnalyticsManager::instance().track_event("level_completed", props);
```

## Custom Data

You can attach custom JSON data to all analytics events sent by the system.

### Setting Custom Data

```rust
use std::collections::HashMap;
use serde_json::Value;

let mut custom = HashMap::new();
custom.insert("user_id".to_string(), Value::from(123));
custom.insert("tier".to_string(), Value::from("gold"));

AnalyticsManager::instance().set_custom_data(Some(custom));
```

- Pass a `HashMap<String, Value>` with your custom properties.
- The map is automatically serialized to JSON.
- Once set, the custom data is included in every subsequent event.

### Resetting Custom Data

```rust
// Clear custom data
AnalyticsManager::instance().set_custom_data(None);
```

### Behavior

- Custom data is stored in the `custom` field of each `TrackingData` object.
- It persists across multiple event calls until explicitly cleared or changed.
- Empty custom data is not included in the request payload.

## Batching

### Manual Batching

Manual batching allows you to explicitly control when analytics events are sent (e.g., at the end of a match).

#### Add Events to Batch

```rust
use std::collections::HashMap;
use serde_json::Value;

// Simple event
AnalyticsManager::instance().batched_track_event_string("EnemyKilled", "");

// Event with structured data
let mut props = HashMap::new();
props.insert("item".to_string(), Value::from("MagicSword"));
props.insert("rarity".to_string(), Value::from("Epic"));

AnalyticsManager::instance().batched_track_event("ItemCrafted", props);
```

#### Send Batched Events

```rust
AnalyticsManager::instance().flush_manual_batch();
```

This spawns a background thread to send all queued events in a single request.

### Automatic Batching

When `auto_batching` is enabled during initialization:

- Events tracked via `track_event` / `track_event_string` are queued automatically.
- The system flushes the queue every `flush_interval_sec` seconds.
- If the server is unreachable, events remain queued until the server responds to a health check.

## Lifecycle Handling

Because `AnalyticsManager` cannot automatically detect when your application closes, you must call `shutdown()` when your application exits to ensure buffered events are flushed to the network.

### CLI / Desktop App Example

```rust
fn main() {
    AnalyticsManager::instance().init(
        "my_app",
        "https://in.vortexanalytics.io",
        "Windows",
        "1.0.0",
        false,
        10,
    );

    // ... your application logic ...

    // Ensure events are flushed before exit
    AnalyticsManager::instance().shutdown();
}
```

### Using `ctrlc` for Graceful Shutdown

```rust
ctrlc::set_handler(move || {
    AnalyticsManager::instance().shutdown();
    std::process::exit(0);
}).expect("Error setting Ctrl-C handler");
```

### Shutdown Behavior

Calling `shutdown()`:

1. Signals all background threads to stop.
2. Tracks a final `app_exit` event.
3. Merges any remaining internal queue into the manual batch.
4. Performs a **blocking** HTTP request (up to the client timeout of 5 seconds) to flush all remaining events before the process terminates.

## Data Structures

The library exposes the following serializable structures:

| Struct | Description |
|---|---|
| `TrackingData` | Contains the event name, value, identity, session, platform, version, custom data, and timestamp. |
| `Tracking` | Wraps a `TrackingData` with a `tenant_id`. |
| `BatchedTracks` | A collection of `Tracking` objects sent in a single batch request. |
| `ValueWrapper` | A simple wrapper around a string value for string-based event payloads. |

## API Reference

| Method | Description |
|---|---|
| `instance()` | Returns the global singleton `&'static AnalyticsManager`. |
| `init(tenant_id, url, platform, app_version, auto_batching, flush_interval_sec)` | Initializes the manager. Must be called once before any tracking. |
| `set_verbose(bool)` | Enables or disables verbose logging to stdout. |
| `track_event(name, HashMap<String, Value>)` | Tracks an event with structured key-value data. |
| `track_event_string(name, &str)` | Tracks an event with a plain string payload. |
| `set_custom_data(Option<HashMap<String, Value>>)` | Sets custom data attached to all future events. Pass `None` to clear. |
| `batched_track_event(name, HashMap<String, Value>)` | Adds a structured event to the manual batch queue. |
| `batched_track_event_string(name, &str)` | Adds a string event to the manual batch queue. |
| `flush_manual_batch()` | Sends all manually batched events in a single request (background thread). |
| `shutdown()` | Flushes all remaining events and stops background threads. **Blocking.** |
