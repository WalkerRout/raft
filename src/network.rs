use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinSet;
use tokio::time::{Duration, sleep};

use crate::server::{Message, NodeId};

pub struct Network {
  connections: Arc<Mutex<HashMap<NodeId, TcpStream>>>,
  _tasks: JoinSet<()>,
}

impl Network {
  pub async fn new(
    addr: SocketAddr,
    peers: HashMap<NodeId, SocketAddr>,
    tx: mpsc::Sender<Message>,
  ) -> io::Result<Self> {
    let listener = TcpListener::bind(addr).await?;
    let task_tx = tx.clone();

    let mut tasks = JoinSet::new();
    tasks.spawn(async move {
      let mut subtasks = JoinSet::new();
      loop {
        let (socket, _) = match listener.accept().await {
          Ok(s) => s,
          Err(e) => {
            eprintln!("Failed to accept connection: {}", e);
            continue;
          }
        };

        let subtask_tx = task_tx.clone();
        subtasks.spawn(handle_connection(socket, subtask_tx));
      }
    });

    let connections = Arc::new(Mutex::new(HashMap::new()));

    for (&peer_id, &peer_addr) in &peers {
      let task_connections = Arc::clone(&connections);
      tasks.spawn(connect_to_peer(task_connections, peer_id, peer_addr));
    }

    Ok(Self {
      connections,
      _tasks: tasks,
    })
  }

  pub async fn send_message(&self, peer_id: NodeId, message: Message) -> io::Result<()> {
    let mut connections = self.connections.lock().await;

    if let Some(stream) = connections.get_mut(&peer_id) {
      let encoded = bincode::serialize(&message).unwrap();
      if let Err(e) = stream.write_all(&encoded).await {
        eprintln!("Failed to send to {}: {}, removing connection", peer_id, e);
        connections.remove(&peer_id); // causes reconnect task to kick in
        return Err(io::Error::new(
          io::ErrorKind::ConnectionAborted,
          "Send failed",
        ));
      }
      return Ok(());
    }

    Err(io::Error::new(
      io::ErrorKind::NotConnected,
      "Peer not connected",
    ))
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
          println!("Connected to peer {}", peer_id);
          connections.lock().await.insert(peer_id, stream);
        }
        Err(e) => {
          eprintln!("Failed to connect to peer {}: {}", peer_id, e);
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
            eprintln!("Failed to forward message: {}", e);
            break;
          }
        }
      }
      Err(e) => {
        eprintln!("Failed to read from socket: {}", e);
        break;
      }
    }
  }
}