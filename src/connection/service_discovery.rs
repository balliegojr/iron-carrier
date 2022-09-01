use simple_mdns::ServiceDiscovery;
use std::net::IpAddr;

use crate::{config::Config, constants::VERSION};

pub fn get_service_discovery(config: &Config) -> crate::Result<Option<ServiceDiscovery>> {
    if !config.enable_service_discovery {
        return Ok(None);
    }

    let node_id = config.node_id.clone();
    let mut sd = ServiceDiscovery::new(&node_id, "_ironcarrier._tcp.local", 600)?;

    let mut service_info = simple_mdns::InstanceInformation::default();
    service_info.ports.push(config.port);
    service_info.ip_addresses = get_my_ips()?;
    service_info
        .attributes
        .insert("v".into(), Some(VERSION.into()));
    service_info.attributes.insert("id".into(), Some(node_id));

    if config.group.is_some() {
        service_info
            .attributes
            .insert("g".into(), config.group.clone());
    }

    sd.add_service_info(service_info)?;

    Ok(Some(sd))
}
fn get_my_ips() -> crate::Result<Vec<IpAddr>> {
    let addrs = if_addrs::get_if_addrs()?
        .iter()
        .filter_map(|iface| {
            if iface.addr.is_loopback() {
                None
            } else {
                Some(iface.addr.ip())
            }
        })
        .collect();

    Ok(addrs)
}
