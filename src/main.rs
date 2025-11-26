// Gorilla Time Series Database - Educational Implementation

// Core modules that implement Gorilla's architecture
mod compression; // Timestamp and value compression algorithms
mod storage; // In-memory data structures
mod tsdb; // Main database interface

use std::time::{SystemTime, UNIX_EPOCH};
use tsdb::Gorilla;

fn main() {
    println!("=== Gorilla Time Series Database ===\n");

    // Create a new Gorilla instance
    let mut gorilla = Gorilla::new();

    // Example 1: Store CPU metrics (simulating regular intervals)
    println!("Example 1: Storing CPU metrics at regular 60-second intervals");
    let base_time = get_current_timestamp();

    let cpu_values = vec![
        (base_time, 45.2),
        (base_time + 60, 46.1),
        (base_time + 120, 45.8),
        (base_time + 180, 47.3),
        (base_time + 240, 45.9),
    ];

    for (timestamp, value) in &cpu_values {
        gorilla.insert("server1.cpu.usage", *timestamp, *value);
    }

    // Show compression efficiency
    let stats = gorilla.get_stats("server1.cpu.usage");
    println!("Original size: {} bytes", stats.original_size);
    println!("Compressed size: {} bytes", stats.compressed_size);
    println!("Compression ratio: {:.2}x\n", stats.compression_ratio);

    // Example 2: Query the data back
    println!("Example 2: Querying data");
    if let Some(series) = gorilla.query("server1.cpu.usage", base_time, base_time + 240) {
        println!("Time series: server1.cpu.usage");
        for (ts, val) in series {
            println!("  {} -> {:.2}", format_timestamp(ts), val);
        }
    }
    println!();

    // Example 3: Store memory metrics (showing XOR compression efficiency)
    println!("Example 3: Storing similar values (shows XOR compression)");
    let memory_base = base_time;
    let memory_values = vec![
        (memory_base, 8192.0),      // Same value repeated
        (memory_base + 15, 8192.0), // 15 second intervals
        (memory_base + 30, 8192.0),
        (memory_base + 45, 8193.0), // Slight change
        (memory_base + 60, 8192.0), // Back to original
    ];

    for (timestamp, value) in &memory_values {
        gorilla.insert("server1.memory.used", *timestamp, *value);
    }

    let mem_stats = gorilla.get_stats("server1.memory.used");
    println!("Memory metrics compression:");
    println!(
        "Original: {} bytes -> Compressed: {} bytes",
        mem_stats.original_size, mem_stats.compressed_size
    );
    println!("Compression ratio: {:.2}x", mem_stats.compression_ratio);
    println!("(Notice how similar values compress extremely well!)\n");

    // Example 4: Demonstrate delta-of-delta timestamp compression
    println!("Example 4: Timestamp compression visualization");
    demonstrate_timestamp_compression();

    // Example 5: Demonstrate XOR value compression
    println!("\nExample 5: Value compression visualization");
    demonstrate_value_compression();

    // Example 6: Advanced features
    println!("\nExample 6: Advanced features");
    demonstrate_advanced_features(&mut gorilla, base_time);
}

fn get_current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

fn format_timestamp(ts: u64) -> String {
    // Simple formatting for demo
    format!("T+{}", ts % 1000)
}

fn demonstrate_timestamp_compression() {
    use compression::timestamp::compress_timestamp;

    println!("  Regular 60-second intervals:");
    let t0 = 1000u64;
    let timestamps = vec![t0, t0 + 60, t0 + 120, t0 + 180];

    let mut prev_ts = t0;
    let mut prev_delta = 0i64;

    for (i, &ts) in timestamps.iter().enumerate() {
        if i == 0 {
            println!("    T0: {} (stored as-is, 64 bits)", ts);
        } else {
            let delta = (ts as i64) - (prev_ts as i64);
            let delta_of_delta = delta - prev_delta;
            let bits = compress_timestamp(delta_of_delta);
            println!(
                "    T{}: {} | delta={}, Δ²={}, bits={}",
                i, ts, delta, delta_of_delta, bits
            );
            prev_delta = delta;
        }
        prev_ts = ts;
    }
}

fn demonstrate_value_compression() {
    println!("  Similar floating point values:");
    let values: Vec<f64> = vec![12.0, 12.0, 11.5, 12.0];

    let mut prev_value: f64 = values[0];
    println!("    V0: {} (stored as-is, 64 bits)", prev_value);

    for (i, &value) in values[1..].iter().enumerate() {
        let xor_result = value.to_bits() ^ prev_value.to_bits();
        let bits_needed = if xor_result == 0 {
            1 // Just a '0' bit
        } else {
            let leading = xor_result.leading_zeros();
            let trailing = xor_result.trailing_zeros();
            let meaningful = 64 - leading - trailing;

            if leading >= 10 && trailing >= 10 {
                14 // Control bits + compressed
            } else {
                meaningful + 13 // Control bits + length encoding
            }
        };

        println!(
            "    V{}: {} | XOR={:064b}, bits={}",
            i + 1,
            value,
            xor_result,
            bits_needed
        );
        prev_value = value;
    }
}

fn demonstrate_advanced_features(gorilla: &mut Gorilla, base_time: u64) {
    // Add some correlated metrics for demonstration
    println!("  Adding correlated metrics:");

    // CPU and response time are typically correlated
    for i in 0..10 {
        let time = base_time + i * 60;
        let cpu = 50.0 + (i as f64 * 2.0);
        let response_time = 100.0 + (i as f64 * 5.0);

        gorilla.insert("web01.cpu", time, cpu);
        gorilla.insert("web01.response_time", time, response_time);
    }

    // Find correlations
    let correlations = gorilla.find_correlated("web01.cpu", base_time, base_time + 600, 5);
    println!("  Metrics correlated with web01.cpu:");
    for (key, corr) in correlations {
        println!("    {} -> correlation: {:.3}", key, corr);
    }

    // Demonstrate scan functionality
    println!("\n  Scanning all time series:");
    let mut count = 0;
    gorilla.scan(|_key, _ts, _val| {
        count += 1;
    });
    println!("    Total data points across all series: {}", count);

    // Demonstrate delete
    gorilla.delete("server1.memory.used");
    println!("    Deleted series: server1.memory.used");
}

#[cfg(test)]
mod integration_tests {
    use super::*;

    #[test]
    fn test_all_methods_used() {
        let mut gorilla = Gorilla::new();
        let base_time = 1000u64;

        // Insert data
        gorilla.insert("test.metric", base_time, 100.0);
        gorilla.insert("test.metric2", base_time, 200.0);

        // Test scan
        let mut count = 0;
        gorilla.scan(|_key, _ts, _val| {
            count += 1;
        });
        assert!(count > 0);

        // Test delete
        gorilla.delete("test.metric");
        assert!(
            gorilla
                .query("test.metric", base_time, base_time + 100)
                .is_none()
        );

        // Test find_correlated
        let _correlations = gorilla.find_correlated("test.metric2", base_time, base_time + 100, 5);
    }
}
