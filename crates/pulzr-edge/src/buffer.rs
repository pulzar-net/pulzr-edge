//! Store-and-forward on disk: a bounded queue of MQTT messages, replayed oldest
//! first, and the per-device `seq` counters that survive restarts.
//!
//! The queue is a few append-only segment files of `topic<TAB>json` lines. Messages
//! are acknowledged in order; a segment is deleted once all its messages are. The
//! read position is not persisted (flash wear), so after a restart the first
//! segment is replayed from its start: duplicates are harmless because the platform
//! drops a `seq` it already took. When the disk budget is exceeded the oldest
//! segment is dropped whole.

use anyhow::{Context, Result};
use std::{
    collections::{BTreeMap, VecDeque},
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    pub topic: String,
    pub payload: Vec<u8>,
    /// Bytes the entry occupies on disk, passed back to [`Buffer::ack`].
    pub len: u64,
}

#[derive(Debug)]
struct Seg {
    id: u64,
    bytes: u64,
    /// Entries not yet acknowledged.
    left: u64,
}

pub struct Buffer {
    dir: PathBuf,
    max_bytes: u64,
    seg_target: u64,
    segs: VecDeque<Seg>,
    /// Read offset in the first segment.
    head: u64,
    writer: Option<File>,
    dropped: u64,
}

fn seg_path(dir: &Path, id: u64) -> PathBuf {
    dir.join(format!("seg-{id:016}.log"))
}

impl Buffer {
    /// Open (or create) the queue in `dir`, bounded to roughly `max_bytes` on disk.
    pub fn open(dir: &Path, max_bytes: u64) -> Result<Self> {
        fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
        let mut ids = Vec::new();
        for e in fs::read_dir(dir)? {
            let name = e?.file_name().to_string_lossy().into_owned();
            if let Some(n) = name
                .strip_prefix("seg-")
                .and_then(|n| n.strip_suffix(".log"))
            {
                if let Ok(id) = n.parse::<u64>() {
                    ids.push(id);
                }
            }
        }
        ids.sort_unstable();
        let mut segs = VecDeque::new();
        for id in ids {
            let path = seg_path(dir, id);
            let data = fs::read(&path)?;
            // a crash can leave a half-written last line: cut it off
            let good = data.iter().rposition(|&b| b == b'\n').map_or(0, |p| p + 1);
            if good < data.len() {
                OpenOptions::new()
                    .write(true)
                    .open(&path)?
                    .set_len(good as u64)?;
            }
            let left = data[..good].iter().filter(|&&b| b == b'\n').count() as u64;
            if left == 0 {
                fs::remove_file(&path)?;
                continue;
            }
            segs.push_back(Seg {
                id,
                bytes: good as u64,
                left,
            });
        }
        let seg_target = (max_bytes / 8).clamp(1024, 64 * 1024);
        Ok(Self {
            dir: dir.to_path_buf(),
            max_bytes,
            seg_target,
            segs,
            head: 0,
            writer: None,
            dropped: 0,
        })
    }

    /// Test hook: smaller segments make rotation and dropping easy to provoke.
    pub fn with_segment_size(mut self, bytes: u64) -> Self {
        self.seg_target = bytes;
        self
    }

    /// Messages waiting.
    pub fn len(&self) -> u64 {
        self.segs.iter().map(|s| s.left).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Messages discarded so far because the disk budget ran out.
    pub fn dropped(&self) -> u64 {
        self.dropped
    }

    fn total_bytes(&self) -> u64 {
        self.segs.iter().map(|s| s.bytes).sum()
    }

    /// Queue a message. Returns how many old messages were dropped to make room.
    pub fn push(&mut self, topic: &str, payload: &[u8]) -> Result<u64> {
        debug_assert!(!payload.contains(&b'\n') && !topic.contains(['\t', '\n']));
        let mut line = Vec::with_capacity(topic.len() + payload.len() + 2);
        line.extend_from_slice(topic.as_bytes());
        line.push(b'\t');
        line.extend_from_slice(payload);
        line.push(b'\n');
        let len = line.len() as u64;

        let mut dropped = 0;
        while self.total_bytes() + len > self.max_bytes && self.segs.len() > 1 {
            let old = self.segs.pop_front().unwrap();
            let _ = fs::remove_file(seg_path(&self.dir, old.id));
            // the unread part of the first segment is what is lost
            dropped += old.left;
            self.head = 0;
        }
        self.dropped += dropped;

        let rotate = self
            .segs
            .back()
            .is_none_or(|s| s.bytes + len > self.seg_target && s.bytes > 0);
        if rotate {
            let id = self.segs.back().map_or(0, |s| s.id + 1);
            let f = OpenOptions::new()
                .create(true)
                .append(true)
                .open(seg_path(&self.dir, id))?;
            self.writer = Some(f);
            self.segs.push_back(Seg {
                id,
                bytes: 0,
                left: 0,
            });
            self.gc();
        }
        if self.writer.is_none() {
            let id = self.segs.back().unwrap().id;
            self.writer = Some(
                OpenOptions::new()
                    .append(true)
                    .open(seg_path(&self.dir, id))?,
            );
        }
        self.writer.as_mut().unwrap().write_all(&line)?;
        let s = self.segs.back_mut().unwrap();
        s.bytes += len;
        s.left += 1;
        Ok(dropped)
    }

    /// The oldest unacknowledged message, if any.
    pub fn peek(&mut self) -> Result<Option<Entry>> {
        let Some(first) = self.segs.front() else {
            return Ok(None);
        };
        if first.left == 0 {
            return Ok(None);
        }
        let mut f = BufReader::new(File::open(seg_path(&self.dir, first.id))?);
        f.seek(SeekFrom::Start(self.head))?;
        let mut line = Vec::new();
        let n = f.read_until(b'\n', &mut line)?;
        if n == 0 {
            return Ok(None);
        }
        line.pop();
        let tab = line
            .iter()
            .position(|&b| b == b'\t')
            .context("corrupt queue line")?;
        Ok(Some(Entry {
            topic: String::from_utf8_lossy(&line[..tab]).into_owned(),
            payload: line[tab + 1..].to_vec(),
            len: n as u64,
        }))
    }

    /// Mark the entry returned by [`peek`](Self::peek) as delivered.
    pub fn ack(&mut self, entry: &Entry) {
        if let Some(first) = self.segs.front_mut() {
            first.left = first.left.saturating_sub(1);
            self.head += entry.len;
        }
        self.gc();
        // a drained queue leaves no file behind, so a restart does not replay it
        if self.segs.len() == 1 && self.segs[0].left == 0 {
            let old = self.segs.pop_front().unwrap();
            let _ = fs::remove_file(seg_path(&self.dir, old.id));
            self.writer = None;
            self.head = 0;
        }
    }

    /// Remove fully acknowledged segments, keeping the one being written.
    fn gc(&mut self) {
        while self.segs.len() > 1 && self.segs.front().is_some_and(|s| s.left == 0) {
            let old = self.segs.pop_front().unwrap();
            let _ = fs::remove_file(seg_path(&self.dir, old.id));
            self.head = 0;
        }
    }
}

/// Per-device `seq` counters. A block of numbers is reserved on disk ahead of use, so
/// the file is written once per block, not per message. After a crash numbers skip
/// forward (never back); after [`flush`](Self::flush) on a clean stop the next number
/// continues exactly.
pub struct SeqStore {
    path: PathBuf,
    /// key -> (next to hand out, reserved up to but excluding)
    keys: BTreeMap<String, (u64, u64)>,
}

const SEQ_BLOCK: u64 = 1000;

impl SeqStore {
    pub fn open(dir: &Path) -> Result<Self> {
        fs::create_dir_all(dir)?;
        let path = dir.join("seq.json");
        let mut keys = BTreeMap::new();
        if let Ok(text) = fs::read_to_string(&path) {
            let saved: BTreeMap<String, u64> = serde_json::from_str(&text).context("seq.json")?;
            for (k, reserved) in saved {
                keys.insert(k, (reserved, reserved));
            }
        }
        Ok(Self { path, keys })
    }

    fn save(&self) -> Result<()> {
        let map: BTreeMap<&String, u64> = self.keys.iter().map(|(k, v)| (k, v.1)).collect();
        let tmp = self.path.with_extension("tmp");
        fs::write(&tmp, serde_json::to_vec(&map)?)?;
        fs::rename(&tmp, &self.path)?;
        Ok(())
    }

    /// The next `seq` for `key`, wrapping at 2^32 as the protocol's range requires.
    pub fn next(&mut self, key: &str) -> Result<u32> {
        let (next, reserved) = self.keys.entry(key.to_string()).or_insert((0, 0));
        let n = *next;
        *next += 1;
        if n >= *reserved {
            *reserved = n + SEQ_BLOCK;
            self.save()?;
        }
        Ok((n & 0xFFFF_FFFF) as u32)
    }

    /// Write the exact positions, for a clean shutdown.
    pub fn flush(&mut self) -> Result<()> {
        for v in self.keys.values_mut() {
            v.1 = v.0;
        }
        self.save()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn drain(b: &mut Buffer) -> Vec<String> {
        let mut out = vec![];
        while let Some(e) = b.peek().unwrap() {
            out.push(String::from_utf8(e.payload.clone()).unwrap());
            b.ack(&e);
        }
        out
    }

    #[test]
    fn replays_oldest_first_and_empties() {
        let d = tempfile::tempdir().unwrap();
        let mut b = Buffer::open(d.path(), 1 << 20).unwrap();
        for i in 0..5 {
            b.push("t", format!("{{\"n\":{i}}}").as_bytes()).unwrap();
        }
        assert_eq!(b.len(), 5);
        let e = b.peek().unwrap().unwrap();
        assert_eq!(e.topic, "t");
        assert_eq!(b.peek().unwrap().unwrap(), e, "peek does not consume");
        assert_eq!(
            drain(&mut b),
            (0..5).map(|i| format!("{{\"n\":{i}}}")).collect::<Vec<_>>()
        );
        assert!(b.is_empty());
    }

    #[test]
    fn survives_restart_and_replays_unacked() {
        let d = tempfile::tempdir().unwrap();
        {
            let mut b = Buffer::open(d.path(), 1 << 20).unwrap();
            for i in 0..3 {
                b.push("t", format!("m{i}").as_bytes()).unwrap();
            }
            let e = b.peek().unwrap().unwrap();
            b.ack(&e); // m0 delivered, m1 and m2 not
        }
        let mut b = Buffer::open(d.path(), 1 << 20).unwrap();
        // the read position is not persisted: the segment replays from its start
        let got = drain(&mut b);
        assert_eq!(got, vec!["m0", "m1", "m2"]);
        let mut b = Buffer::open(d.path(), 1 << 20).unwrap();
        assert!(
            b.peek().unwrap().is_none(),
            "a drained queue leaves nothing behind"
        );
    }

    #[test]
    fn acked_segments_are_deleted() {
        let d = tempfile::tempdir().unwrap();
        let mut b = Buffer::open(d.path(), 1 << 20)
            .unwrap()
            .with_segment_size(64);
        for i in 0..20 {
            b.push("t", format!("message-{i:03}").as_bytes()).unwrap();
        }
        assert!(fs::read_dir(d.path()).unwrap().count() > 3);
        drain(&mut b);
        assert_eq!(
            fs::read_dir(d.path()).unwrap().count(),
            0,
            "a drained queue leaves no file"
        );
    }

    #[test]
    fn full_disk_budget_drops_the_oldest() {
        let d = tempfile::tempdir().unwrap();
        let mut b = Buffer::open(d.path(), 400).unwrap().with_segment_size(100);
        for i in 0..40 {
            b.push("t", format!("message-{i:03}").as_bytes()).unwrap();
        }
        assert!(b.dropped() > 0);
        let got = drain(&mut b);
        assert!(got.len() < 40);
        assert_eq!(got.last().unwrap(), "message-039", "newest survives");
        let nums: Vec<u32> = got.iter().map(|s| s[8..].parse().unwrap()).collect();
        assert!(
            nums.windows(2).all(|w| w[1] == w[0] + 1),
            "what is left is a contiguous, ordered tail"
        );
        assert_eq!(b.dropped() + got.len() as u64, 40);
    }

    #[test]
    fn a_torn_last_line_is_cut_off() {
        let d = tempfile::tempdir().unwrap();
        {
            let mut b = Buffer::open(d.path(), 1 << 20).unwrap();
            b.push("t", b"whole").unwrap();
        }
        let p = fs::read_dir(d.path())
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        OpenOptions::new()
            .append(true)
            .open(&p)
            .unwrap()
            .write_all(b"t\thal")
            .unwrap();
        let mut b = Buffer::open(d.path(), 1 << 20).unwrap();
        assert_eq!(drain(&mut b), vec!["whole"]);
    }

    #[test]
    fn seq_is_monotonic_across_unclean_restarts() {
        let d = tempfile::tempdir().unwrap();
        let mut last = None;
        for _ in 0..3 {
            let mut s = SeqStore::open(d.path()).unwrap();
            for _ in 0..7 {
                let n = s.next("modbus:x").unwrap();
                assert!(last.is_none_or(|l| n > l), "{n} after {last:?}");
                last = Some(n);
            }
            // dropped without flush: a crash
        }
    }

    #[test]
    fn seq_continues_exactly_after_a_clean_stop() {
        let d = tempfile::tempdir().unwrap();
        let mut s = SeqStore::open(d.path()).unwrap();
        assert_eq!(
            (
                s.next("a").unwrap(),
                s.next("a").unwrap(),
                s.next("b").unwrap()
            ),
            (0, 1, 0)
        );
        s.flush().unwrap();
        let mut s = SeqStore::open(d.path()).unwrap();
        assert_eq!(
            (
                s.next("a").unwrap(),
                s.next("b").unwrap(),
                s.next("a").unwrap()
            ),
            (2, 1, 3)
        );
    }
}
