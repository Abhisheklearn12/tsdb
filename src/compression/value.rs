// XOR-based floating point value compression
// Paper Section 4.1.2: Compressing values

use super::BitWriter;

/// Compresses a floating point value using XOR with previous value
///
/// Key insight: Similar values have similar bit representations
/// - XOR of similar floats has many leading and trailing zeros
/// - We only need to store the "meaningful" bits in the middle
///
/// Example:
///   12.0 = 0x4028000000000000
///   24.0 = 0x4038000000000000
///   XOR  = 0x0010000000000000 (only 1 bit different!)
///
/// Encoding scheme:
/// 1. If XOR == 0: store '0' (1 bit) - value unchanged
/// 2. If XOR != 0: store '1' + either:
///    a) Control bit '0': Reuse previous leading/trailing zero counts
///    b) Control bit '1': Store new leading zeros (5 bits) +
///                        meaningful bit length (6 bits) + value
#[allow(dead_code)]
pub fn compress_value_xor(xor_result: u64) -> usize {
    if xor_result == 0 {
        1 // Just '0' bit
    } else {
        let leading = xor_result.leading_zeros();
        let trailing = xor_result.trailing_zeros();
        let meaningful = 64 - leading - trailing;

        // Control bit '0' case (optimistic)
        if leading >= 10 && trailing >= 10 {
            14 // '10' + meaningful bits (approx)
        } else {
            // Control bit '1' case
            2 + 5 + 6 + meaningful as usize // '11' + 5 + 6 + value
        }
    }
}

/// Encodes a floating point value using XOR compression
pub fn encode_value_xor(
    writer: &mut BitWriter,
    value: f64,
    prev_value: f64,
    prev_leading: &mut u32,
    prev_trailing: &mut u32,
) -> usize {
    let value_bits = value.to_bits();
    let prev_bits = prev_value.to_bits();
    let xor = value_bits ^ prev_bits;

    let bits_before = writer.bit_count();

    if xor == 0 {
        // Values are identical
        writer.write_bit(false); // '0'
    } else {
        writer.write_bit(true); // '1'

        let leading = xor.leading_zeros();
        let trailing = xor.trailing_zeros();

        // Check if we can reuse previous block position
        if leading >= *prev_leading && trailing >= *prev_trailing {
            // Case (a): Block of meaningful bits fits within previous block
            writer.write_bit(false); // '0' -> control bit

            // Calculate how many bits to store
            let meaningful_bits = 64 - *prev_leading - *prev_trailing;

            // Extract the meaningful bits
            let shift = *prev_trailing;
            // Prevent overflow: if meaningful_bits is 64, mask is all 1s (u64::MAX)
            let mask = if meaningful_bits == 64 {
                u64::MAX
            } else {
                (1u64 << meaningful_bits) - 1
            };
            let meaningful_value = (xor >> shift) & mask;

            writer.write_bits(meaningful_value, meaningful_bits as u8);
        } else {
            // Case (b): Need to store new block position
            writer.write_bit(true); // '1' -> control bit

            // Store leading zeros count (5 bits, max 31)
            writer.write_bits(leading as u64, 5);

            // Calculate and store meaningful bits length (6 bits, max 63)
            let meaningful_bits = 64 - leading - trailing;
            writer.write_bits(meaningful_bits as u64, 6);

            // Store the meaningful bits
            let shift = trailing;
            // Prevent overflow: if meaningful_bits is 64, mask is all 1s (u64::MAX)
            let mask = if meaningful_bits == 64 {
                u64::MAX
            } else {
                (1u64 << meaningful_bits) - 1
            };
            let meaningful_value = (xor >> shift) & mask;
            writer.write_bits(meaningful_value, meaningful_bits as u8);

            // Update for next value
            *prev_leading = leading;
            *prev_trailing = trailing;
        }
    }

    writer.bit_count() - bits_before
}

/// Complete value compression helper
pub struct ValueCompressor {
    prev_value: f64,
    prev_leading: u32,
    prev_trailing: u32,
}

impl ValueCompressor {
    pub fn new(first_value: f64) -> Self {
        ValueCompressor {
            prev_value: first_value,
            prev_leading: 0,
            prev_trailing: 0,
        }
    }

    pub fn add_value(&mut self, writer: &mut BitWriter, value: f64) -> usize {
        let bits = encode_value_xor(
            writer,
            value,
            self.prev_value,
            &mut self.prev_leading,
            &mut self.prev_trailing,
        );

        self.prev_value = value;
        bits
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_identical_values() {
        // Identical values compress to just 1 bit each
        let values = vec![42.0, 42.0, 42.0, 42.0];

        let mut writer = BitWriter::new();
        let mut compressor = ValueCompressor::new(values[0]);

        let mut total_bits = 64; // First value stored in full

        for &val in &values[1..] {
            let bits = compressor.add_value(&mut writer, val);
            total_bits += bits;
            assert_eq!(bits, 1, "Identical values should be 1 bit");
        }

        println!(
            "Identical values: {} values in {} bits",
            values.len(),
            total_bits
        );
    }

    #[test]
    fn test_similar_values() {
        // Similar values compress well
        let values = vec![100.0, 100.5, 100.2, 100.8];

        let mut writer = BitWriter::new();
        let mut compressor = ValueCompressor::new(values[0]);

        let mut total_bits = 64;

        for &val in &values[1..] {
            let bits = compressor.add_value(&mut writer, val);
            total_bits += bits;
            println!("Value {}: {} bits", val, bits);
        }

        println!(
            "Similar values: {} values in {} bits",
            values.len(),
            total_bits
        );
        println!(
            "Average: {:.1} bits per value",
            total_bits as f64 / values.len() as f64
        );
    }

    #[test]
    fn test_integer_values() {
        // Integer values stored as floats compress extremely well
        // because only the mantissa changes in predictable patterns
        let values = vec![8192.0, 8192.0, 8192.0, 8193.0, 8192.0];

        let mut writer = BitWriter::new();
        let mut compressor = ValueCompressor::new(values[0]);

        let mut total_bits = 64;

        for &val in &values[1..] {
            let bits = compressor.add_value(&mut writer, val);
            total_bits += bits;
        }

        println!(
            "Integer-like values: {} values in {} bits",
            values.len(),
            total_bits
        );
        println!(
            "Average: {:.1} bits per value",
            total_bits as f64 / values.len() as f64
        );
    }
}
