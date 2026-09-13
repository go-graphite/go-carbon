//! Go process collectors absent from the Rust prometheus crate.
use prometheus::core::{Collector, Desc};
use prometheus::proto::{Counter, Gauge, Metric, MetricFamily, MetricType};

pub(crate) struct ExtraProcessMetrics {
    descs: [Desc; 3],
}

impl ExtraProcessMetrics {
    pub(crate) fn new() -> prometheus::Result<Self> {
        let desc = |name: &str, help: &str| {
            Desc::new(name.into(), help.into(), vec![], Default::default())
        };
        Ok(Self {
            descs: [
                desc(
                    "process_virtual_memory_max_bytes",
                    "Maximum amount of virtual memory available in bytes.",
                )?,
                desc(
                    "process_network_receive_bytes_total",
                    "Number of bytes received by the process over the network.",
                )?,
                desc(
                    "process_network_transmit_bytes_total",
                    "Number of bytes sent by the process over the network.",
                )?,
            ],
        })
    }

    fn samples(&self, limits: &str, netstat: &str) -> Vec<MetricFamily> {
        [
            address_space(limits),
            octets(netstat, "InOctets"),
            octets(netstat, "OutOctets"),
        ]
        .into_iter()
        .enumerate()
        .filter_map(|(i, value)| {
            let value = value? as f64;
            let mut metric = Metric::new();
            let mut family = MetricFamily::new();
            family.set_name(self.descs[i].fq_name.clone());
            family.set_help(self.descs[i].help.clone());
            if i == 0 {
                let mut gauge = Gauge::new();
                gauge.set_value(value);
                metric.set_gauge(gauge);
                family.set_field_type(MetricType::GAUGE);
            } else {
                let mut counter = Counter::new();
                counter.set_value(value);
                metric.set_counter(counter);
                family.set_field_type(MetricType::COUNTER);
            }
            family.mut_metric().push(metric);
            Some(family)
        })
        .collect()
    }
}

impl Collector for ExtraProcessMetrics {
    fn desc(&self) -> Vec<&Desc> {
        self.descs.iter().collect()
    }
    fn collect(&self) -> Vec<MetricFamily> {
        // Match Go's procfs source: IpExt is network-namespace traffic, not socket attribution.
        self.samples(
            &std::fs::read_to_string("/proc/self/limits").unwrap_or_default(),
            &std::fs::read_to_string("/proc/self/net/netstat").unwrap_or_default(),
        )
    }
}

fn address_space(limits: &str) -> Option<u64> {
    let value = limits
        .lines()
        .find_map(|line| line.strip_prefix("Max address space"))?
        .split_whitespace()
        .next()?;
    if value == "unlimited" {
        Some(u64::MAX)
    } else {
        value.parse().ok()
    }
}

fn octets(netstat: &str, field: &str) -> Option<u64> {
    let mut lines = netstat.lines();
    while let Some(header) = lines.next() {
        let values = lines.next()?;
        if header.starts_with("IpExt:") && values.starts_with("IpExt:") {
            return header
                .split_whitespace()
                .zip(values.split_whitespace())
                .find_map(|(key, value)| (key == field).then(|| value.parse().ok()))
                .flatten();
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn procfs_values_keep_go_names_types_and_unlimited_semantics() {
        let collector = ExtraProcessMetrics::new().unwrap();
        assert_eq!(collector.desc().len(), 3);
        let netstat =
            "TcpExt: A B\nTcpExt: 1 2\nIpExt: OutOctets Other InOctets\nIpExt: 123 0 456\n";
        let samples = collector.samples(
            "Max address space         unlimited            unlimited            bytes\n",
            netstat,
        );
        assert_eq!(samples.len(), 3);
        assert_eq!(samples[0].get_field_type(), MetricType::GAUGE);
        assert_eq!(
            samples[0].get_metric()[0].get_gauge().value(),
            u64::MAX as f64
        );
        assert_eq!(samples[1].name(), "process_network_receive_bytes_total");
        assert_eq!(samples[1].get_field_type(), MetricType::COUNTER);
        assert_eq!(samples[1].get_metric()[0].get_counter().value(), 456.0);
        assert_eq!(samples[2].get_metric()[0].get_counter().value(), 123.0);
        assert_eq!(
            address_space("Max address space 4096 8192 bytes"),
            Some(4096)
        );
        assert!(collector.samples("", "").is_empty());
        assert_eq!(octets("IpExt: InOctets\nIpExt: broken\n", "InOctets"), None);
    }
}
