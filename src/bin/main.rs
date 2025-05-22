use std::collections::HashMap;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;

use clap::Parser;

use tokio::signal;
use tokio::sync::mpsc;

use tracing::info;
use tracing_subscriber::filter::LevelFilter;

use uuid::Uuid;

use raft::network::Network;
use raft::server::{NodeId, Server};

#[derive(thiserror::Error, Debug)]
enum PeerError {
  #[error("failed to parse peer string, expected format <peer_id>:<peer_ip>:<peer_port>")]
  StringParseFailure,
}

#[derive(Debug, Clone)]
struct Peer {
  id: NodeId,
  addr: SocketAddr,
}

impl FromStr for Peer {
  type Err = PeerError;

  fn from_str(s: &str) -> Result<Self, Self::Err> {
    let parts: Vec<&str> = s.splitn(2, ':').collect();
    if parts.len() != 2 {
      return Err(PeerError::StringParseFailure);
    }
    let id = Uuid::parse_str(parts[0]).map_err(|_| PeerError::StringParseFailure)?;
    let addr = parts[1]
      .parse()
      .map_err(|_| PeerError::StringParseFailure)?;
    Ok(Peer { id, addr })
  }
}

/// Command-line arguments for the Raft node.
#[derive(Parser)]
#[command(name = "raft-node", version = "1.0", about = "A Raft consensus node")]
struct CliArgs {
  /// Node ID (UUID)
  #[arg(short = 'i', long = "id")]
  id: NodeId,

  /// Node address (e.g., 127.0.0.1:8080)
  #[arg(short = 'a', long = "addr")]
  addr: SocketAddr,

  /// Peer nodes in the format <peer_id>:<peer_ip>:<peer_port>
  #[arg(short = 'p', long = "peer", value_parser = str::parse::<Peer>)]
  peers: Vec<Peer>,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), anyhow::Error> {
  tracing_subscriber::fmt()
    .with_max_level(LevelFilter::INFO)
    .with_target(false)
    .with_thread_ids(true)
    .with_ansi(true)
    .init();

  log_panics::init();

  let args = CliArgs::parse();

  let mut peer_map = HashMap::new();
  let mut peer_ids = Vec::new();
  for peer in &args.peers {
    peer_map.insert(peer.id, peer.addr);
    peer_ids.push(peer.id);
  }

  let (tx, rx) = mpsc::channel(1024);

  let network = Arc::new(Network::new(args.addr, peer_map, tx).await?);
  let node = Server::new(args.id, network, peer_ids, rx);

  info!("server spinning up...");
  tokio::select! {
    _ = signal::ctrl_c() => {},
    _ = node.spin() => {},
  }
  info!("server spinning down...");

  Ok(())
}
