use std::fmt;
use std::time::Duration;

#[derive(Clone, Serialize)]
pub struct Stats {
    latency: Vec<Duration>,
    bytes: Vec<usize>,
}

impl Stats {
    pub fn new() -> Stats {
        Stats {
            latency: Vec::new(),
            bytes: Vec::new(),
        }
    }

    pub fn total_latency(&self) -> Duration {
        return self.latency.iter().fold(Duration::new(0, 0), |acc, s| acc + *s);
    }

    pub fn total_bytes(&self) -> usize {
        return self.bytes.iter().fold(0, |acc, s| acc + s);
    }

    pub fn record(&mut self, latency: Duration, bytes: usize) {
        self.latency.push(latency);
        self.bytes.push(bytes);
    }

    pub fn num_ops(&self) -> usize {
        self.bytes.len()
    }

    pub fn ops_per_second(&self) -> f64 {
        let total_latency = self.total_latency();
        (self.num_ops() as f64) / (total_latency.as_secs() as f64 + (total_latency.subsec_nanos() as f64 / 1_000_000_000 as f64))
    }
}

impl fmt::Display for Stats {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let total_latency = self.total_latency();
        let total_bytes = self.total_bytes();
        let num_ops = self.bytes.len();
        let avg_latency = match total_latency.checked_div(num_ops as u32) {
            Some(quotient) => format!("{}.{:09}", quotient.as_secs(), quotient.subsec_nanos()),
            None => String::from("(inf)"),
        };
        write!(
            f,
            "Completed {} operations ({} bytes) in {}.{:09} s\n",
            num_ops,
            total_bytes,
            total_latency.as_secs(),
            total_latency.subsec_nanos()
        )?;
        write!(f, " - Average latency = {}\n", avg_latency)?;
        write!(f, " - Bytes/Operation = {}\n", (total_bytes as f64) / (num_ops as f64))?;

        let ops_per_second =
            (num_ops as f64) / (total_latency.as_secs() as f64 + (total_latency.subsec_nanos() as f64 / 1_000_000_000 as f64));
        write!(f, " - Operations/Second = {}\n", ops_per_second)?;
        Ok(())
    }
}

impl ::std::ops::Add for Stats {
    type Output = Stats;
    fn add(self, rhs: Stats) -> Self::Output {
        Stats {
            latency: [&self.latency[..], &rhs.latency[..]].concat(),
            bytes: [&self.bytes[..], &rhs.bytes[..]].concat(),
        }
    }
}
