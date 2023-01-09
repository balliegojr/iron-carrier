use simple_mdns::async_discovery::ServiceDiscovery;
use std::{
    collections::HashMap,
    net::{IpAddr, SocketAddr},
};

use crate::{config::Config, constants::VERSION, hash_helper};

pub async fn get_service_discovery(config: &Config) -> crate::Result<Option<ServiceDiscovery>> {
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
            .insert("g".into(), hashed_group(config.group.as_ref()));
    }

    sd.add_service_info(service_info).await?;

    Ok(Some(sd))
}

fn hashed_group(group: Option<&String>) -> Option<String> {
    group.map(|g| hash_helper::calculate_checksum(g.as_bytes()).to_string())
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

pub async fn get_peers(
    service_discovery: &ServiceDiscovery,
    group: Option<&String>,
) -> HashMap<SocketAddr, Option<String>> {
    let mut addresses = HashMap::new();
    let h_group = hashed_group(group);

    let services = service_discovery
        .get_known_services()
        .await
        .into_iter()
        .filter(|service| match group {
            group @ Some(_) => service
                .attributes
                .get("g")
                .map(|g| g.eq(&h_group))
                .unwrap_or_default(),
            None => !service.attributes.contains_key("g"),
        });

    for instance_info in services {
        let id = &instance_info.attributes["id"];
        for addr in instance_info.get_socket_addresses() {
            addresses.insert(addr, id.clone());
        }
    }

    addresses
}