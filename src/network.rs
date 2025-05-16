use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinSet;
use tokio::time::{Duration, sleep};

use tracing::{Instrument, Span, instrument};
use tracing::{error, info, warn};

use crate::server::{Message, NodeId};

#[derive(thiserror::Error, Debug)]
pub enum NetworkError {
  #[error("failed to listen on address '{0}'")]
  AddressListenFailure(SocketAddr),

  #[error("peer not connected")]
  PeerNotConnected,

  #[error("failed to send, removing peer '{0}' connection")]
  PeerConnectionAborted(NodeId),

  #[error("failed to decode message from bytes")]
  MessageDecodeFailure,
}

pub struct Network {
  connections: Arc<Mutex<HashMap<NodeId, TcpStream>>>,
  _tasks: JoinSet<()>,
}

impl Network {
  #[instrument(name = "NETWORK", skip(addr, peers, tx))]
  pub async fn new(
    addr: SocketAddr,
    peers: HashMap<NodeId, SocketAddr>,
    tx: mpsc::Sender<Message>,
  ) -> Result<Self, NetworkError> {
    let listener = TcpListener::bind(addr)
      .await
      .map_err(|_| NetworkError::AddressListenFailure(addr))?;
    let task_tx = tx.clone();

    let mut tasks = JoinSet::new();
    tasks.spawn(
      async move {
        let mut subtasks = JoinSet::new();
        loop {
          let (socket, _) = match listener.accept().await {
            Ok(s) => s,
            Err(e) => {
              warn!("failed to accept connection: {}", e);
              continue;
            }
          };

          let subtask_tx = task_tx.clone();
          subtasks.spawn(handle_connection(socket, subtask_tx).instrument(Span::current()));
        }
      }
      .instrument(Span::current()),
    );

    let connections = Arc::new(Mutex::new(HashMap::new()));

    for (&peer_id, &peer_addr) in &peers {
      let task_connections = Arc::clone(&connections);
      tasks
        .spawn(connect_to_peer(task_connections, peer_id, peer_addr).instrument(Span::current()));
    }

    Ok(Self {
      connections,
      _tasks: tasks,
    })
  }

  pub async fn send_message(&self, peer_id: NodeId, message: Message) -> Result<(), NetworkError> {
    let encoded = bincode::serialize(&message).map_err(|_| NetworkError::MessageDecodeFailure)?;
    let mut connections = self.connections.lock().await;
    if let Some(stream) = connections.get_mut(&peer_id) {
      if stream.write_all(&encoded).await.is_err() {
        connections.remove(&peer_id); // causes reconnect task to kick in
        Err(NetworkError::PeerConnectionAborted(peer_id))
      } else {
        Ok(())
      }
    } else {
      Err(NetworkError::PeerNotConnected)
    }
  }
}

async fn connect_to_peer(
  connections: Arc<Mutex<HashMap<NodeId, TcpStream>>>,
  peer_id: NodeId,
  addr: SocketAddr,
) {
  loop {
    let connected = {
      let conns = connections.lock().await;
      conns.contains_key(&peer_id)
    };

    if !connected {
      match TcpStream::connect(addr).await {
        Ok(stream) => {
          info!("connected to peer {}", peer_id);
          connections.lock().await.insert(peer_id, stream);
        }
        Err(e) => {
          warn!("failed to connect to peer {}: {}", peer_id, e);
        }
      }
    }

    sleep(Duration::from_secs(5)).await;
  }
}

async fn handle_connection(mut socket: TcpStream, tx: mpsc::Sender<Message>) {
  let mut buffer = vec![0; 1024];
  loop {
    match socket.read(&mut buffer).await {
      Ok(0) => {
        // connection closed
        break;
      }
      Ok(n) => {
        if let Ok(message) = bincode::deserialize::<Message>(&buffer[..n]) {
          if let Err(e) = tx.send(message).await {
            error!("failed to forward message: {}", e);
            break;
          }
        }
      }
      Err(e) => {
        error!("failed to read from socket: {}", e);
        break;
      }
    }
  }
}
