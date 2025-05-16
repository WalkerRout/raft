use std::pin::Pin;
use std::sync::Arc;

use tokio::sync::mpsc;
use tokio::time::{Duration, Instant, interval, sleep};

use serde::{Deserialize, Serialize};

use rand::Rng;

use uuid::Uuid;

use crate::network::Network;

pub type NodeId = Uuid;

pub struct Follower;

pub struct Candidate;

pub struct Leader;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntries {
  term: u64,
  leader_id: NodeId,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Message {
  AppendEntries(AppendEntries),
  RequestVote { term: u64, candidate_id: NodeId },
  Vote { term: u64, granted: bool },
}

pub trait Run {
  type Conclusion;

  fn run(self) -> Pin<Box<dyn Future<Output = Self::Conclusion>>>;
}

pub struct StateMachine<S> {
  id: NodeId,
  current_term: u64,
  voted_for: Option<NodeId>,
  peer_ids: Vec<NodeId>,
  network: Arc<Network>,
  rx: mpsc::Receiver<Message>,
  _state: S,
}

impl<S> StateMachine<S> {
  fn transfer_to<T>(self, new_state: T) -> StateMachine<T> {
    StateMachine {
      id: self.id,
      current_term: self.current_term,
      voted_for: self.voted_for,
      peer_ids: self.peer_ids,
      network: self.network,
      rx: self.rx,
      _state: new_state,
    }
  }
}

impl StateMachine<Follower> {
  fn into_candidate(self) -> StateMachine<Candidate> {
    self.transfer_to(Candidate)
  }
}

impl StateMachine<Candidate> {
  fn into_follower(self) -> StateMachine<Follower> {
    self.transfer_to(Follower)
  }

  fn into_leader(self) -> StateMachine<Leader> {
    self.transfer_to(Leader)
  }
}

impl StateMachine<Leader> {
  fn into_follower(self) -> StateMachine<Follower> {
    self.transfer_to(Follower)
  }
}

impl Run for StateMachine<Follower> {
  type Conclusion = DynamicServer;

  fn run(mut self) -> Pin<Box<dyn Future<Output = Self::Conclusion>>> {
    Box::pin(async move {
      println!(
        "Node {}: running as Follower (term {})",
        self.id, self.current_term
      );
      loop {
        let timeout = rand::rng().random_range(150..300);
        let timer = sleep(Duration::from_millis(timeout));
        tokio::select! {
          _ = timer => {
            println!("Node {}: election timeout", self.id);
            return self.into_candidate().into();
          },
          Some(msg) = self.rx.recv() => {
            match msg {
              Message::AppendEntries(ae) => {
                if ae.term >= self.current_term {
                  self.current_term = ae.term;
                  self.voted_for = None;
                  println!("Node {}: got heartbeat from leader {}", self.id, ae.leader_id);
                  // Restart the follower timer
                  continue; // re-enters loop with a fresh timer
                }
              },
              Message::RequestVote { term, candidate_id } => {
                if term > self.current_term {
                  self.current_term = term;
                  self.voted_for = Some(candidate_id);
                  let _ = self.network
                    .send_message(candidate_id, Message::Vote {
                      term,
                      granted: true
                    })
                    .await;
                  println!("Node {}: voted for {}", self.id, candidate_id);
                }
              },
              _ => {}
            }
          }
        }
      }
    }) // end of pinned future
  }
}

impl Run for StateMachine<Candidate> {
  type Conclusion = DynamicServer;

  fn run(mut self) -> Pin<Box<dyn Future<Output = Self::Conclusion>>> {
    Box::pin(async move {
      self.current_term += 1;
      self.voted_for = Some(self.id);
      let mut votes = 1; // voted for self
      println!(
        "Node {}: starting election for term {}",
        self.id, self.current_term
      );

      for peer_id in &self.peer_ids {
        let _ = self
          .network
          .send_message(
            *peer_id,
            Message::RequestVote {
              term: self.current_term,
              candidate_id: self.id,
            },
          )
          .await;
      }

      let election_timeout =
        Instant::now() + Duration::from_millis(rand::rng().random_range(150..300));

      while Instant::now() < election_timeout {
        tokio::select! {
          Some(msg) = self.rx.recv() => {
            match msg {
              Message::Vote { term, granted } => {
                if term == self.current_term && granted {
                  votes += 1;
                  if votes > 1 {
                    println!("Node {}: became leader!", self.id);
                    return self.into_leader().into();
                  }
                }
              }
              Message::AppendEntries(ae) => {
                if ae.term >= self.current_term {
                  println!("Node {}: stepping down to follower", self.id);
                  self.current_term = ae.term;
                  return self.into_follower().into();
                }
              }
              Message::RequestVote { term, candidate_id } => {
                if term > self.current_term {
                  println!("Node {}: stepping down due to higher term {} from {}", self.id, term, candidate_id);
                  self.current_term = term;
                  return self.into_follower().into();
                }
              }
            }
          },
          _ = sleep(Duration::from_millis(10)) => {}
        }
      }

      println!("Node {}: election failed, retrying...", self.id);

      self.into()
    })
  }
}

impl Run for StateMachine<Leader> {
  type Conclusion = DynamicServer;

  fn run(mut self) -> Pin<Box<dyn Future<Output = Self::Conclusion>>> {
    Box::pin(async move {
      let mut ticker = interval(Duration::from_millis(100));
      loop {
        ticker.tick().await;
        // heartbeat
        for &peer_id in &self.peer_ids {
          let _ = self
            .network
            .send_message(
              peer_id,
              Message::AppendEntries(AppendEntries {
                term: self.current_term,
                leader_id: self.id,
              }),
            )
            .await;
        }
        println!("Node {}: sent heartbeat", self.id);

        //let timeout = rand::thread_rng().gen_range(150..300);
        //let mut timer = sleep(Duration::from_millis(timeout)).await;

        while let Ok(Some(msg)) =
          tokio::time::timeout(Duration::from_millis(5), self.rx.recv()).await
        {
          if let Message::AppendEntries(ae) = msg {
            if ae.term > self.current_term {
              println!(
                "Node {}: stepping down, new leader {}",
                self.id, ae.leader_id
              );
              self.current_term = ae.term;
              return self.into_follower().into();
            }
          }
        }
      }
    }) // end of pinned future
  }
}

pub enum DynamicServer {
  Follower(StateMachine<Follower>),
  Candidate(StateMachine<Candidate>),
  Leader(StateMachine<Leader>),
}

impl DynamicServer {
  fn new(
    id: NodeId,
    network: Arc<Network>,
    peer_ids: Vec<NodeId>,
    rx: mpsc::Receiver<Message>,
  ) -> Self {
    Self::Follower(StateMachine {
      id,
      current_term: 0,
      voted_for: None,
      network,
      peer_ids,
      rx,
      _state: Follower,
    })
  }
}

impl Run for DynamicServer {
  type Conclusion = Self;

  fn run(self) -> Pin<Box<dyn Future<Output = Self::Conclusion>>> {
    Box::pin(async move {
      match self {
        Self::Follower(srv) => srv.run().await,
        Self::Candidate(srv) => srv.run().await,
        Self::Leader(srv) => srv.run().await,
      }
    })
  }
}

impl From<StateMachine<Follower>> for DynamicServer {
  fn from(follower: StateMachine<Follower>) -> Self {
    Self::Follower(follower)
  }
}

impl From<StateMachine<Candidate>> for DynamicServer {
  fn from(candidate: StateMachine<Candidate>) -> Self {
    Self::Candidate(candidate)
  }
}

impl From<StateMachine<Leader>> for DynamicServer {
  fn from(leader: StateMachine<Leader>) -> Self {
    Self::Leader(leader)
  }
}

pub struct Server {
  inner: Option<DynamicServer>,
}

impl Server {
  pub fn new(
    id: NodeId,
    network: Arc<Network>,
    peer_ids: Vec<NodeId>,
    rx: mpsc::Receiver<Message>,
  ) -> Self {
    Self {
      inner: Some(DynamicServer::new(id, network, peer_ids, rx)),
    }
  }

  pub async fn spin(mut self) {
    loop {
      let inner = self.inner.take().unwrap();
      self.inner = Some(inner.run().await);
    }
  }
}
