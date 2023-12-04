use simple_mdns::{async_discovery::ServiceDiscovery, InstanceInformation};
use std::{
    collections::HashMap,
    net::{IpAddr, SocketAddr},
    time::Duration,
};

use crate::{config::Config, constants::VERSION, context::Context, hash_helper, node_id::NodeId};
use tokio::sync::OnceCell;

static SERVICE_DISCOVERY: OnceCell<ServiceDiscovery> = OnceCell::const_new();

pub async fn init_service_discovery(config: &Config) {
    let _ = get_service_discovery(config).await;
}

async fn get_service_discovery(config: &Config) -> anyhow::Result<Option<&ServiceDiscovery>> {
    if !config.enable_service_discovery {
        return Ok(None);
    }

    let sd: anyhow::Result<&ServiceDiscovery> = SERVICE_DISCOVERY
        .get_or_try_init(|| async {
            let backoff = backoff::ExponentialBackoffBuilder::new()
                .with_max_elapsed_time(Some(Duration::from_secs(30)))
                .build();

            // this retry is here in case the network card isn't ready when initializing the daemon
            let mut sd = backoff::future::retry(backoff, || async {
                ServiceDiscovery::new(
                    config.node_id_hashed.to_string().as_str(),
                    "_ironcarrier._tcp.local",
                    600,
                )
                .map_err(backoff::Error::from)
            })
            .await?;

            let mut service_info = simple_mdns::InstanceInformation::default();
            service_info.ports.push(config.port);
            service_info.ip_addresses = get_my_ips(config)?;
            service_info
                .attributes
                .insert("v".into(), Some(VERSION.into()));

            if config.group.is_some() {
                service_info
                    .attributes
                    .insert("g".into(), config.group.as_deref().map(hashed_group));
            }

            sd.add_service_info(service_info).await?;

            Ok(sd)
        })
        .await;

    sd.map(Some)
}

pub fn get_my_ips(config: &Config) -> anyhow::Result<Vec<IpAddr>> {
    let bind_addr: IpAddr = config.bind.parse()?;

    let addrs = if_addrs::get_if_addrs()?
        .iter()
        .filter_map(|iface| {
            let should_add = match iface.ip() {
                IpAddr::V4(ip) => bind_addr.is_ipv4() && !ip.is_loopback() && ip.is_private(),
                IpAddr::V6(ip) => bind_addr.is_ipv6() && !ip.is_loopback(),
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

pub async fn get_nodes(context: &Context) -> anyhow::Result<HashMap<SocketAddr, Option<NodeId>>> {
    let bind_addr: IpAddr = context.config.bind.parse()?;
    let mut addresses = HashMap::new();

    if let Some(service_discovery) = get_service_discovery(context.config).await? {
        let h_group = context.config.group.as_deref().map(hashed_group);

        let services = get_known_services(service_discovery)
            .await
            .into_iter()
            .filter(|(node_id, service)| {
                *node_id != context.config.node_id_hashed.to_string()
                    && same_version(service)
                    && same_group(service, &h_group)
            });

        for (node_id, instance_info) in services {
            if let Ok(id) = node_id.parse::<u64>() {
                for addr in instance_info
                    .get_socket_addresses()
                    .filter(|addr| addr.is_ipv4() == bind_addr.is_ipv4())
                {
                    addresses.insert(addr, Some(id.into()));
                }
            }
        }
    }

    Ok(addresses)
}

/// Try to get known services, if no services are returned, wait 2 seconds and then try again
async fn get_known_services(
    service_discovery: &ServiceDiscovery,
) -> HashMap<String, InstanceInformation> {
    let services = service_discovery.get_known_services().await;
    if !services.is_empty() {
        return services;
    }

    tokio::time::sleep(Duration::from_secs(1)).await;
    service_discovery.get_known_services().await
}

fn hashed_group(group: &str) -> String {
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
