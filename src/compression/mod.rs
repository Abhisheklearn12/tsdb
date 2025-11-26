// Implements Gorilla's innovative compression algorithms
// Paper Section 4.1: Time series compression

pub mod timestamp;
pub mod value;

/// BitWriter allows writing individual bits to a byte buffer
/// This is essential for Gorilla's variable-length encoding
pub struct BitWriter {
    buffer: Vec<u8>,
    current_byte: u8,
    bit_position: u8, // 0-7, position within current byte
}

impl BitWriter {
    pub fn new() -> Self {
        BitWriter {
            buffer: Vec::new(),
            current_byte: 0,
            bit_position: 0,
        }
    }

    /// Write a single bit (0 or 1)
    pub fn write_bit(&mut self, bit: bool) {
        if bit {
            self.current_byte |= 1 << (7 - self.bit_position);
        }

        self.bit_position += 1;

        if self.bit_position == 8 {
            self.buffer.push(self.current_byte);
            self.current_byte = 0;
            self.bit_position = 0;
        }
    }

    /// Write multiple bits from a u64 value
    /// bits: number of bits to write (from least significant)
    pub fn write_bits(&mut self, value: u64, bits: u8) {
        for i in (0..bits).rev() {
            let bit = (value >> i) & 1 == 1;
            self.write_bit(bit);
        }
    }

    /// Flush remaining bits and return the buffer
    pub fn finish(mut self) -> Vec<u8> {
        if self.bit_position > 0 {
            self.buffer.push(self.current_byte);
        }
        self.buffer
    }

    /// Get current size in bits
    pub fn bit_count(&self) -> usize {
        self.buffer.len() * 8 + self.bit_position as usize
    }
}

/// BitReader allows reading individual bits from a byte buffer
/// Used for decompression (not shown in this demo, but needed for production)
#[allow(dead_code)]
pub struct BitReader {
    buffer: Vec<u8>,
    byte_position: usize,
    bit_position: u8, // 0-7
}

#[allow(dead_code)]
impl BitReader {
    pub fn new(buffer: Vec<u8>) -> Self {
        BitReader {
            buffer,
            byte_position: 0,
            bit_position: 0,
        }
    }

    /// Read a single bit
    pub fn read_bit(&mut self) -> Option<bool> {
        if self.byte_position >= self.buffer.len() {
            return None;
        }

        let byte = self.buffer[self.byte_position];
        let bit = (byte >> (7 - self.bit_position)) & 1 == 1;

        self.bit_position += 1;
        if self.bit_position == 8 {
            self.byte_position += 1;
            self.bit_position = 0;
        }

        Some(bit)
    }

    /// Read multiple bits into a u64
    pub fn read_bits(&mut self, bits: u8) -> Option<u64> {
        let mut value = 0u64;
        for _ in 0..bits {
            let bit = self.read_bit()?;
            value = (value << 1) | (bit as u64);
        }
        Some(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bit_writer_reader() {
        let mut writer = BitWriter::new();

        // Write some bits
        writer.write_bit(true);
        writer.write_bit(false);
        writer.write_bit(true);
        writer.write_bits(0b1010, 4);

        let buffer = writer.finish();
        let mut reader = BitReader::new(buffer);

        assert_eq!(reader.read_bit(), Some(true));
        assert_eq!(reader.read_bit(), Some(false));
        assert_eq!(reader.read_bit(), Some(true));
        assert_eq!(reader.read_bits(4), Some(0b1010));
    }
}
