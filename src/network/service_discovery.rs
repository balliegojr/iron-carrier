use simple_mdns::{async_discovery::ServiceDiscovery, InstanceInformation};
use std::{
    collections::HashMap,
    net::{IpAddr, SocketAddr},
};

use crate::{config::Config, constants::VERSION, hash_helper};
use tokio::sync::OnceCell;

static SERVICE_DISCOVERY: OnceCell<ServiceDiscovery> = OnceCell::const_new();

pub async fn get_service_discovery(config: &Config) -> crate::Result<Option<&ServiceDiscovery>> {
    if !config.enable_service_discovery {
        return Ok(None);
    }

    let sd: crate::Result<&ServiceDiscovery> = SERVICE_DISCOVERY
        .get_or_try_init(|| async {
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
                    .insert("g".into(), config.group.as_ref().map(hashed_group));
            }

            sd.add_service_info(service_info).await?;

            Ok(sd)
        })
        .await;

    sd.map(Some)
}

pub fn get_my_ips() -> crate::Result<Vec<IpAddr>> {
    let addrs = if_addrs::get_if_addrs()?
        .iter()
        .filter_map(|iface| {
            let should_add = match iface.ip() {
                IpAddr::V4(ip) => !ip.is_loopback() && ip.is_private(),
                IpAddr::V6(ip) => !ip.is_loopback(),
            };

            if should_add {
                Some(iface.addr.ip())
            } else {
                None
            }
        })
        .collect();

    Ok(addrs)
}

pub async fn get_peers(
    service_discovery: &ServiceDiscovery,
    group: Option<&String>,
) -> HashMap<SocketAddr, Option<u64>> {
    let mut addresses = HashMap::new();
    let h_group = group.map(hashed_group);

    let services = service_discovery
        .get_known_services()
        .await
        .into_iter()
        .filter(|service| same_version(service) && same_group(service, &h_group));

    for mut instance_info in services {
        let id = instance_info
            .attributes
            .remove("id")
            .flatten()
            .map(hash_helper::hashed_str);

        for addr in instance_info.get_socket_addresses() {
            addresses.insert(addr, id);
        }
    }

    addresses
}

fn hashed_group(group: &String) -> String {
    hash_helper::hashed_str(group).to_string()
}

fn same_version(service: &InstanceInformation) -> bool {
    service
        .attributes
        .get("v")
        .and_then(|v| v.as_ref().map(|v| v == VERSION))
        .unwrap_or_default()
}

fn same_group(service: &InstanceInformation, h_group: &Option<String>) -> bool {
    match h_group {
        Some(_) => service
            .attributes
            .get("g")
            .map(|g| g.eq(h_group))
            .unwrap_or_default(),
        None => !service.attributes.contains_key("g"),
    }
}
