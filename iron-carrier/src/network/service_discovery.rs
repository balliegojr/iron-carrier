use simple_mdns::{InstanceInformation, async_discovery::ServiceDiscovery};
use std::{
    collections::{HashMap, HashSet},
    net::{IpAddr, SocketAddr},
    time::Duration,
};

use crate::{config::Config, constants::VERSION, context::Context, node_id::NodeId};
use tokio::sync::OnceCell;

use super::backoff_retry;

static SERVICE_DISCOVERY: OnceCell<ServiceDiscovery> = OnceCell::const_new();
const SERVICE_DISCOVERY_TIMEOUT: u64 = 30;

pub async fn init_service_discovery(config: &Config) {
    let _ = get_service_discovery(config).await;
}

async fn get_service_discovery(config: &Config) -> anyhow::Result<Option<&ServiceDiscovery>> {
    if !config.enable_service_discovery {
        return Ok(None);
    }

    let sd: anyhow::Result<&ServiceDiscovery> = SERVICE_DISCOVERY
        .get_or_try_init(|| async {
            let mut service_info = simple_mdns::InstanceInformation::new(config.node_id.clone());
            service_info.ports.insert(config.port);
            service_info.ip_addresses = get_my_ips(config)?;
            service_info
                .attributes
                .insert("v".into(), Some(VERSION.into()));

            if config.group.is_some() {
                service_info
                    .attributes
                    .insert("g".into(), config.group.clone());
            }
            // this retry is here in case the network card isn't ready when initializing the daemon
            let sd = backoff_retry(SERVICE_DISCOVERY_TIMEOUT, || async {
                ServiceDiscovery::new(service_info.clone(), "_ironcarrier._tcp.local", 600)
                    .map_err(backoff::Error::from)
            })
            .await?;

            Ok(sd)
        })
        .await;

    sd.map(Some)
}

pub fn get_my_ips(config: &Config) -> anyhow::Result<HashSet<IpAddr>> {
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
        let services = get_known_services(service_discovery)
            .await
            .into_iter()
            .filter(|service| {
                service.unescaped_instance_name() != context.config.node_id
                    && same_version(service)
                    && same_group(service, &context.config.group)
            });

        for instance_info in services {
            if let Ok(id) = instance_info.unescaped_instance_name().parse::<u64>() {
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
async fn get_known_services(service_discovery: &ServiceDiscovery) -> HashSet<InstanceInformation> {
    let services = service_discovery.get_known_services().await;
    if !services.is_empty() {
        return services;
    }

    tokio::time::sleep(Duration::from_secs(1)).await;
    service_discovery.get_known_services().await
}

fn same_version(service: &InstanceInformation) -> bool {
    service
        .attributes
        .get("v")
        .and_then(|v| v.as_ref().map(|v| v == VERSION))
        .unwrap_or_default()
}

fn same_group(service: &InstanceInformation, group: &Option<String>) -> bool {
    match group {
        Some(_) => service
            .attributes
            .get("g")
            .map(|g| g.eq(group))
            .unwrap_or_default(),
        None => !service.attributes.contains_key("g"),
    }
}
