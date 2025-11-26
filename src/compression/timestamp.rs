// Delta-of-delta timestamp compression
// Paper Section 4.1.1: Compressing time stamps

use super::BitWriter;

/// Compresses a timestamp using delta-of-delta encoding
///
/// Key insight: Time series data often arrives at regular intervals
/// - If data comes every 60 seconds, deltas are: 60, 60, 60, ...
/// - Delta-of-deltas become: 0, 0, 0, ... (highly compressible!)
///
/// Encoding scheme from the paper:
/// - If D == 0: store '0' (1 bit)
/// - If D in [-63, 64]: store '10' + 7 bits (9 bits total)
/// - If D in [-255, 256]: store '110' + 9 bits (12 bits total)
/// - If D in [-2047, 2048]: store '1110' + 12 bits (16 bits total)
/// - Otherwise: store '1111' + 32 bits (36 bits total)
pub fn compress_timestamp(delta_of_delta: i64) -> usize {
    if delta_of_delta == 0 {
        1 // Just '0'
    } else if delta_of_delta >= -63 && delta_of_delta <= 64 {
        9 // '10' + 7 bits
    } else if delta_of_delta >= -255 && delta_of_delta <= 256 {
        12 // '110' + 9 bits
    } else if delta_of_delta >= -2047 && delta_of_delta <= 2048 {
        16 // '1110' + 12 bits
    } else {
        36 // '1111' + 32 bits
    }
}

/// Encodes a timestamp delta-of-delta into a BitWriter
pub fn encode_timestamp_delta(writer: &mut BitWriter, delta_of_delta: i64) {
    if delta_of_delta == 0 {
        // Case: D == 0
        writer.write_bit(false); // '0'
    } else if delta_of_delta >= -63 && delta_of_delta <= 64 {
        // Case: D in [-63, 64]
        writer.write_bit(true); // '1'
        writer.write_bit(false); // '0' -> '10'

        // Store as 7-bit signed integer
        let value = ((delta_of_delta + 63) as u64) & 0x7F;
        writer.write_bits(value, 7);
    } else if delta_of_delta >= -255 && delta_of_delta <= 256 {
        // Case: D in [-255, 256]
        writer.write_bit(true); // '1'
        writer.write_bit(true); // '1'
        writer.write_bit(false); // '0' -> '110'

        // Store as 9-bit signed integer
        let value = ((delta_of_delta + 255) as u64) & 0x1FF;
        writer.write_bits(value, 9);
    } else if delta_of_delta >= -2047 && delta_of_delta <= 2048 {
        // Case: D in [-2047, 2048]
        writer.write_bit(true); // '1'
        writer.write_bit(true); // '1'
        writer.write_bit(true); // '1'
        writer.write_bit(false); // '0' -> '1110'

        // Store as 12-bit signed integer
        let value = ((delta_of_delta + 2047) as u64) & 0xFFF;
        writer.write_bits(value, 12);
    } else {
        // Case: Everything else
        writer.write_bit(true); // '1'
        writer.write_bit(true); // '1'
        writer.write_bit(true); // '1'
        writer.write_bit(true); // '1' -> '1111'

        // Store as 32-bit signed integer
        writer.write_bits(delta_of_delta as u64, 32);
    }
}

/// Complete timestamp compression example
pub struct TimestampCompressor {
    prev_timestamp: u64,
    prev_delta: i64,
}

impl TimestampCompressor {
    pub fn new(first_timestamp: u64) -> Self {
        TimestampCompressor {
            prev_timestamp: first_timestamp,
            prev_delta: 0,
        }
    }

    /// Add a timestamp and return bits needed to encode it
    pub fn add_timestamp(&mut self, writer: &mut BitWriter, timestamp: u64) -> usize {
        let delta = (timestamp as i64) - (self.prev_timestamp as i64);
        let delta_of_delta = delta - self.prev_delta;

        let bits_before = writer.bit_count();
        encode_timestamp_delta(writer, delta_of_delta);
        let bits_after = writer.bit_count();

        // Update state for next timestamp
        self.prev_timestamp = timestamp;
        self.prev_delta = delta;

        bits_after - bits_before
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_regular_intervals() {
        // Simulating data arriving every 60 seconds
        let timestamps = vec![1000, 1060, 1120, 1180, 1240];

        let mut writer = BitWriter::new();
        let mut compressor = TimestampCompressor::new(timestamps[0]);

        let mut total_bits = 64; // First timestamp stored in full

        for &ts in &timestamps[1..] {
            let bits = compressor.add_timestamp(&mut writer, ts);
            total_bits += bits;

            // After first timestamp, all deltas are 0, so only 1 bit each!
            if ts != timestamps[1] {
                assert_eq!(bits, 1, "Regular intervals should compress to 1 bit");
            }
        }

        println!(
            "Regular intervals: {} timestamps in {} bits",
            timestamps.len(),
            total_bits
        );
        println!(
            "Average: {:.1} bits per timestamp",
            total_bits as f64 / timestamps.len() as f64
        );
    }

    #[test]
    fn test_irregular_intervals() {
        // Simulating slightly irregular data (59, 61, 60 second intervals)
        let timestamps = vec![1000, 1059, 1120, 1180];

        let mut writer = BitWriter::new();
        let mut compressor = TimestampCompressor::new(timestamps[0]);

        for &ts in &timestamps[1..] {
            compressor.add_timestamp(&mut writer, ts);
        }

        // Small variations still compress well (9 bits)
        println!("Irregular but close: compressed successfully");
    }
}
