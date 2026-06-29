use bitvec::prelude::*;
use sha1::{Digest, Sha1};
use std::collections::HashMap;

pub type PieceIndex = u32;
pub type BlockIndex = u32;

/// A wrapper around BitVec representing the piece availability.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Bitfield {
    bits: BitVec<u8, Msb0>,
}

impl Bitfield {
    pub fn new(len: usize) -> Self {
        Self {
            bits: BitVec::repeat(false, len),
        }
    }

    pub fn from_bytes(bytes: Vec<u8>, bit_len: usize) -> Self {
        let mut bits = BitVec::<u8, Msb0>::from_vec(bytes);
        bits.truncate(bit_len);
        Self { bits }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        self.bits.clone().into_vec()
    }

    pub fn get(&self, index: usize) -> Option<bool> {
        self.bits.get(index).as_deref().copied()
    }

    pub fn set(&mut self, index: usize, value: bool) {
        if index < self.bits.len() {
            self.bits.set(index, value);
        }
    }

    pub fn len(&self) -> usize {
        self.bits.len()
    }

    pub fn is_empty(&self) -> bool {
        self.bits.is_empty()
    }

    pub fn count_ones(&self) -> usize {
        self.bits.count_ones()
    }
}

/// PiecePicker tracks piece availability from peers and selects which pieces to download.
pub struct PiecePicker {
    num_pieces: usize,
    piece_availability: Vec<usize>, // number of peers offering each piece index
    downloaded: Bitfield,
    requested: Bitfield,
}

impl PiecePicker {
    pub fn new(num_pieces: usize) -> Self {
        Self {
            num_pieces,
            piece_availability: vec![0; num_pieces],
            downloaded: Bitfield::new(num_pieces),
            requested: Bitfield::new(num_pieces),
        }
    }

    pub fn peer_has_piece(&mut self, index: usize) {
        if index < self.num_pieces {
            self.piece_availability[index] += 1;
        }
    }

    pub fn peer_lost_piece(&mut self, index: usize) {
        if index < self.num_pieces && self.piece_availability[index] > 0 {
            self.piece_availability[index] -= 1;
        }
    }

    pub fn mark_downloaded(&mut self, index: usize) {
        self.downloaded.set(index, true);
    }

    pub fn mark_requested(&mut self, index: usize, requested: bool) {
        self.requested.set(index, requested);
    }

    /// Rarest-first piece selection algorithm.
    /// Returns the index of the best piece to request next.
    pub fn pick_next_piece(&self, peer_bitfield: &Bitfield) -> Option<usize> {
        let mut candidates = Vec::new();

        for i in 0..self.num_pieces {
            // Only consider pieces we don't have and haven't requested yet,
            // and that the peer actually has.
            if !self.downloaded.get(i).unwrap_or(true)
                && !self.requested.get(i).unwrap_or(true)
                && peer_bitfield.get(i).unwrap_or(false)
            {
                candidates.push((i, self.piece_availability[i]));
            }
        }

        // Sort candidates by availability (rarest first)
        candidates.sort_by_key(|&(_, count)| count);
        candidates.first().map(|&(index, _)| index)
    }
}

/// Verifies if the piece data matches the expected SHA-1 infohash.
pub fn verify_piece(data: &[u8], expected_hash: &[u8; 20]) -> bool {
    let mut hasher = Sha1::new();
    hasher.update(data);
    let result = hasher.finalize();
    result.as_slice() == expected_hash
}
