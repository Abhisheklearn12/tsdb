// Main Gorilla TSDB interface
// Paper Section 4: Gorilla Architecture

use crate::storage::{DataPoint, TimeSeriesMap};

/// Design goals (from paper Section 2.2):
/// - Store billions of time series
/// - Handle 700M+ data points per minute
/// - Keep 26 hours of data in memory
/// - Sub-millisecond query latency
/// - High availability and fault tolerance
///
/// Key innovations:
/// 1. Delta-of-delta timestamp compression (Section 4.1.1)
/// 2. XOR-based floating point compression (Section 4.1.2)
/// 3. Efficient in-memory structures (Section 4.2)
/// 4. Write-through cache design (Section 1)
pub struct Gorilla {
    // The core data structure: TSmap
    // In production, this would be sharded across multiple hosts
    tsmap: TimeSeriesMap,
}

impl Gorilla {
    /// Create a new Gorilla instance
    pub fn new() -> Self {
        Gorilla {
            tsmap: TimeSeriesMap::new(),
        }
    }

    /// Insert a data point
    ///
    /// In production, this would:
    /// 1. Hash the key to determine shard
    /// 2. Stream to multiple regions for redundancy
    /// 3. Buffer writes for 1 minute on shard reassignment
    ///
    /// Paper Section 4.4: Handling failures
    pub fn insert(&mut self, key: &str, timestamp: u64, value: f64) {
        self.tsmap.insert(key.to_string(), timestamp, value);
    }

    /// Query data points within a time range
    ///
    /// Returns all points for the given key between start and end timestamps
    ///
    /// In production:
    /// - Queries hit the closest regional instance
    /// - Failed over automatically on node failure
    /// - Can return partial results marked as such
    ///
    /// Paper: Query latency reduced from ~500ms (HBase) to ~7ms (Gorilla)
    pub fn query(&self, key: &str, start: u64, end: u64) -> Option<Vec<(u64, f64)>> {
        self.tsmap.get(key).map(|series| {
            series
                .query(start, end)
                .into_iter()
                .map(|dp| (dp.timestamp, dp.value))
                .collect()
        })
    }

    /// Get storage statistics for a time series
    ///
    /// This shows the compression efficiency achieved by Gorilla
    /// Paper reports average of 1.37 bytes per data point (12x compression)
    pub fn get_stats(&self, key: &str) -> CompressionStats {
        if let Some(series) = self.tsmap.get(key) {
            let stats = series.get_stats();
            CompressionStats {
                original_size: stats.original_size,
                compressed_size: stats.compressed_size,
                compression_ratio: stats.compression_ratio(),
            }
        } else {
            CompressionStats::default()
        }
    }

    /// Scan all time series
    ///
    /// Used for:
    /// - Correlation search (Section 5.1)
    /// - Background rollup aggregations (Section 5.3)
    /// - Monitoring and debugging
    ///
    /// Paper: Gorilla can scan all data very efficiently for these operations
    /// Demonstrated in Example 6
    pub fn scan<F>(&self, mut f: F)
    where
        F: FnMut(&str, u64, f64),
    {
        self.tsmap.scan(|series| {
            for point in series.query(0, u64::MAX) {
                f(&series.key, point.timestamp, point.value);
            }
        });
    }

    /// Delete a time series
    /// Used in Example 6 to demonstrate cleanup
    pub fn delete(&mut self, key: &str) {
        self.tsmap.delete(key);
    }
}

/// Statistics about compression efficiency
#[derive(Debug, Default)]
pub struct CompressionStats {
    pub original_size: usize,
    pub compressed_size: usize,
    pub compression_ratio: f64,
}

/// Use cases enabled by Gorilla (from Section 5)
///
/// 1. Time series correlation (Section 5.1)
///    - Brute-force search across 1M+ time series
///    - Uses PPMCC (Pearson correlation)
///    - Helps answer: "What happened when my service broke?"
///
/// 2. Advanced charting (Section 5.2)
///    - Horizon charts with large datasets
///    - Visual anomaly detection
///    - Real-time dashboards
///
/// 3. Efficient aggregations (Section 5.3)
///    - Rollup operations run directly on Gorilla
///    - No longer need expensive HBase scans
///    - Reduced load on persistent storage
impl Gorilla {
    /// Example: Find correlated time series (simplified version of Section 5.1)
    ///
    /// In production, this calculates Pearson Product-Moment Correlation
    /// Coefficient (PPMCC) across all time series
    /// Demonstrated in Example 6
    pub fn find_correlated(
        &self,
        needle_key: &str,
        start: u64,
        end: u64,
        top_n: usize,
    ) -> Vec<(String, f64)> {
        // Get the needle time series
        let needle = match self.query(needle_key, start, end) {
            Some(data) => data,
            None => return Vec::new(),
        };

        if needle.is_empty() {
            return Vec::new();
        }

        let mut correlations = Vec::new();

        // Scan all time series and calculate correlation
        self.tsmap.scan(|series| {
            if series.key == needle_key {
                return; // Skip self
            }

            let data = series.query(start, end);
            if data.len() != needle.len() {
                return; // Need same length for correlation
            }

            // Simple correlation calculation (simplified)
            let correlation = calculate_correlation(&needle, &data);
            correlations.push((series.key.clone(), correlation));
        });

        // Sort by absolute correlation and take top N
        correlations.sort_by(|a, b| b.1.abs().partial_cmp(&a.1.abs()).unwrap());
        correlations.truncate(top_n);

        correlations
    }
}

/// Calculate correlation between two time series (simplified)
/// Used by find_correlated() in Example 6
fn calculate_correlation(series1: &[(u64, f64)], series2: &[DataPoint]) -> f64 {
    if series1.len() != series2.len() || series1.is_empty() {
        return 0.0;
    }

    let n = series1.len() as f64;

    // Calculate means
    let mean1: f64 = series1.iter().map(|(_, v)| v).sum::<f64>() / n;
    let mean2: f64 = series2.iter().map(|p| p.value).sum::<f64>() / n;

    // Calculate correlation
    let mut numerator = 0.0;
    let mut sum_sq1 = 0.0;
    let mut sum_sq2 = 0.0;

    for i in 0..series1.len() {
        let diff1 = series1[i].1 - mean1;
        let diff2 = series2[i].value - mean2;
        numerator += diff1 * diff2;
        sum_sq1 += diff1 * diff1;
        sum_sq2 += diff2 * diff2;
    }

    let denominator = (sum_sq1 * sum_sq2).sqrt();
    if denominator == 0.0 {
        0.0
    } else {
        numerator / denominator
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_operations() {
        let mut gorilla = Gorilla::new();

        // Use current time to ensure we're within a valid block
        let base_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Insert some data
        gorilla.insert("cpu.usage", base_time, 45.2);
        gorilla.insert("cpu.usage", base_time + 60, 46.1);
        gorilla.insert("cpu.usage", base_time + 120, 45.8);

        // Query it back
        let results = gorilla
            .query("cpu.usage", base_time, base_time + 200)
            .unwrap();
        assert_eq!(
            results.len(),
            3,
            "Expected 3 results, got {}",
            results.len()
        );
        assert_eq!(results[0].1, 45.2);

        // Check compression
        let stats = gorilla.get_stats("cpu.usage");
        println!("Compression: {}x", stats.compression_ratio);
        assert!(stats.compression_ratio > 1.0);

        // Test that key field is accessible
        gorilla.scan(|key, _ts, _val| {
            println!("Scanned series: {}", key);
        });
    }

    #[test]
    fn test_compression_efficiency() {
        let mut gorilla = Gorilla::new();

        // Use current time
        let base_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Insert identical values (should compress to ~1 bit each)
        for i in 0..100 {
            gorilla.insert("memory.used", base_time + i * 60, 8192.0);
        }

        let stats = gorilla.get_stats("memory.used");
        println!("100 identical values:");
        println!("  Original: {} bytes", stats.original_size);
        println!("  Compressed: {} bytes", stats.compressed_size);
        println!("  Ratio: {:.2}x", stats.compression_ratio);

        // Should achieve very high compression
        assert!(stats.compression_ratio > 10.0);
    }
}
