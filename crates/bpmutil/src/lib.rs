use std::io::Read;

/// Read all bytes from a [Read] and return a blake3 hash 
pub fn blake3_hash_reader<R: Read>(mut read: R) -> std::io::Result<String> {
    let mut hasher = blake3::Hasher::new();
    let _ = std::io::copy(&mut read, &mut hasher)?;
    let hash = hasher.finalize().to_hex().to_string();
    Ok(hash)
}
