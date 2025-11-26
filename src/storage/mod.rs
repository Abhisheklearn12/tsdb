// In-memory data structures for time series storage
// Paper Section 4.2: In-memory data structures

use crate::compression::{BitWriter, timestamp::TimestampCompressor, value::ValueCompressor};
use std::collections::HashMap;

/// A single data point in a time series
#[derive(Debug, Clone, Copy)]
pub struct DataPoint {
    pub timestamp: u64,
    pub value: f64,
}

/// A time series holds all data points for a single metric
///
/// Architecture (from paper Figure 7):
/// - Open block: Currently being written (last 2 hours)
/// - Closed blocks: Completed 2-hour chunks (compressed)
///
/// Each time series has a spinlock in production, but we use
/// Rust's ownership system instead for this educational version
pub struct TimeSeries {
    // Key is public and used in scan operations for correlation analysis
    pub key: String,

    // Open block - actively being written
    open_block: TimeSeriesBlock,

    // Closed blocks - immutable compressed data
    closed_blocks: Vec<TimeSeriesBlock>,

    // Block duration in seconds (paper uses 2 hours = 7200 seconds)
    block_duration: u64,
}

impl TimeSeries {
    pub fn new(key: String) -> Self {
        let block_duration = 7200; // 2 hours
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Align to 2-hour window (as paper describes)
        let block_start = (now / block_duration) * block_duration;

        TimeSeries {
            key,
            open_block: TimeSeriesBlock::new(block_start),
            closed_blocks: Vec::new(),
            block_duration,
        }
    }

    /// Insert a data point into the time series
    pub fn insert(&mut self, timestamp: u64, value: f64) {
        // Check if we need to close the current block
        if timestamp >= self.open_block.start_time + self.block_duration {
            // Close current block and start a new one
            let old_block = std::mem::replace(
                &mut self.open_block,
                TimeSeriesBlock::new((timestamp / self.block_duration) * self.block_duration),
            );
            self.closed_blocks.push(old_block);
        }

        // Add point to open block
        self.open_block.add_point(timestamp, value);
    }

    /// Query data points within a time range
    pub fn query(&self, start: u64, end: u64) -> Vec<DataPoint> {
        let mut results = Vec::new();

        // Query closed blocks
        for block in &self.closed_blocks {
            if block.overlaps(start, end) {
                results.extend(block.get_points(start, end));
            }
        }

        // Query open block
        if self.open_block.overlaps(start, end) {
            results.extend(self.open_block.get_points(start, end));
        }

        results
    }

    /// Get storage statistics
    pub fn get_stats(&self) -> StorageStats {
        let mut stats = StorageStats::default();

        // Count points and calculate sizes
        let mut total_points = 0;

        for block in &self.closed_blocks {
            total_points += block.points.len();
            stats.compressed_size += block.compressed_size;
        }

        total_points += self.open_block.points.len();
        stats.compressed_size += self.open_block.compressed_size;

        // Original size: 16 bytes per point (8 bytes timestamp + 8 bytes value)
        stats.original_size = total_points * 16;

        stats
    }
}

/// A block represents a 2-hour chunk of compressed time series data
/// Paper describes this as the fundamental storage unit
pub struct TimeSeriesBlock {
    pub start_time: u64,

    // Uncompressed points (for demo purposes)
    // In production, only compressed data would be kept
    points: Vec<DataPoint>,

    // Compressed representation
    compressed_data: Vec<u8>,
    compressed_size: usize,
}

impl TimeSeriesBlock {
    pub fn new(start_time: u64) -> Self {
        TimeSeriesBlock {
            start_time,
            points: Vec::new(),
            compressed_data: Vec::new(),
            compressed_size: 0,
        }
    }

    /// Add a point and compress it
    pub fn add_point(&mut self, timestamp: u64, value: f64) {
        self.points.push(DataPoint { timestamp, value });

        // Recompress the entire block (simplified for demo)
        // In production, this would append to existing compressed data
        self.compress();
    }

    /// Compress all points in this block
    fn compress(&mut self) {
        if self.points.is_empty() {
            return;
        }

        let mut writer = BitWriter::new();

        // Write header: aligned start time (64 bits)
        writer.write_bits(self.start_time, 64);

        // Write first timestamp delta (14 bits, as per paper)
        let first_delta = (self.points[0].timestamp as i64) - (self.start_time as i64);
        writer.write_bits(first_delta as u64, 14);

        // Write first value (64 bits)
        writer.write_bits(self.points[0].value.to_bits(), 64);

        // Compress subsequent points
        if self.points.len() > 1 {
            let mut ts_compressor = TimestampCompressor::new(self.points[0].timestamp);
            let mut val_compressor = ValueCompressor::new(self.points[0].value);

            for point in &self.points[1..] {
                ts_compressor.add_timestamp(&mut writer, point.timestamp);
                val_compressor.add_value(&mut writer, point.value);
            }
        }

        self.compressed_data = writer.finish();
        self.compressed_size = self.compressed_data.len();
    }

    /// Check if this block overlaps with a time range
    fn overlaps(&self, start: u64, end: u64) -> bool {
        let block_end = self.start_time + 7200; // 2 hours
        !(end < self.start_time || start > block_end)
    }

    /// Get points within a time range
    fn get_points(&self, start: u64, end: u64) -> Vec<DataPoint> {
        self.points
            .iter()
            .filter(|p| p.timestamp >= start && p.timestamp <= end)
            .copied()
            .collect()
    }
}

/// Storage statistics for compression analysis
#[derive(Default, Debug)]
pub struct StorageStats {
    pub original_size: usize,   // Uncompressed size in bytes
    pub compressed_size: usize, // Compressed size in bytes
}

impl StorageStats {
    pub fn compression_ratio(&self) -> f64 {
        if self.compressed_size == 0 {
            return 0.0;
        }
        self.original_size as f64 / self.compressed_size as f64
    }
}

/// Time Series Map (TSmap) - main data structure
/// Paper Section 4.2 and Figure 7
///
/// Contains:
/// - Vector of time series for efficient scanning
/// - HashMap for O(1) lookups by key
pub struct TimeSeriesMap {
    // Vector allows efficient paged scans
    series_vector: Vec<Option<TimeSeries>>,

    // Map from key to index in vector
    key_to_index: HashMap<String, usize>,

    // Free list for reusing tombstoned entries
    free_indices: Vec<usize>,
}

impl TimeSeriesMap {
    pub fn new() -> Self {
        TimeSeriesMap {
            series_vector: Vec::new(),
            key_to_index: HashMap::new(),
            free_indices: Vec::new(),
        }
    }

    /// Insert or update a time series
    pub fn insert(&mut self, key: String, timestamp: u64, value: f64) {
        if let Some(&index) = self.key_to_index.get(&key) {
            // Time series exists, update it
            if let Some(ref mut series) = self.series_vector[index] {
                series.insert(timestamp, value);
            }
        } else {
            // Create new time series
            let mut series = TimeSeries::new(key.clone());
            series.insert(timestamp, value);

            let index = if let Some(free_idx) = self.free_indices.pop() {
                // Reuse a tombstoned slot
                self.series_vector[free_idx] = Some(series);
                free_idx
            } else {
                // Append new slot
                self.series_vector.push(Some(series));
                self.series_vector.len() - 1
            };

            self.key_to_index.insert(key, index);
        }
    }

    /// Get a time series by key
    pub fn get(&self, key: &str) -> Option<&TimeSeries> {
        self.key_to_index
            .get(key)
            .and_then(|&idx| self.series_vector[idx].as_ref())
    }

    /// Delete a time series (tombstoning)
    pub fn delete(&mut self, key: &str) {
        if let Some(&index) = self.key_to_index.get(key) {
            self.series_vector[index] = None; // Tombstone
            self.free_indices.push(index);
            self.key_to_index.remove(key);
        }
    }

    /// Scan all time series (for background jobs)
    pub fn scan<F>(&self, mut f: F)
    where
        F: FnMut(&TimeSeries),
    {
        for entry in &self.series_vector {
            if let Some(series) = entry {
                f(series);
            }
        }
    }
}
