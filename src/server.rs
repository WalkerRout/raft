use std::pin::Pin;
use std::sync::Arc;

use rand::Rng;

use serde::{Deserialize, Serialize};

use tokio::sync::mpsc;
use tokio::time::{Duration, Instant, interval, sleep, timeout};

use tracing::instrument;
use tracing::{info, warn};

use uuid::Uuid;

use crate::network::Network;

pub type NodeId = Uuid;

pub struct Follower;

#[derive(Default)]
pub struct Candidate {
  vote_count: usize,
}

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

pub trait Step {
  type Conclusion;

  fn step(self) -> Pin<Box<dyn Future<Output = Self::Conclusion>>>;
}

#[allow(dead_code)]
enum TransitionTo {
  Follower,
  Candidate,
  Leader,
}

fn random_election_timeout() -> Duration {
  let millis = rand::rng().random_range(150..300);
  Duration::from_millis(millis)
}

pub struct StateMachine<S> {
  id: NodeId,
  current_term: u64,
  voted_for: Option<NodeId>,
  peer_ids: Vec<NodeId>,
  network: Arc<Network>,
  rx: mpsc::Receiver<Message>,
  state: S,
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
      state: new_state,
    }
  }
}

impl StateMachine<Follower> {
  fn into_candidate(self) -> StateMachine<Candidate> {
    self.transfer_to(Candidate::default())
  }
}

impl StateMachine<Follower> {
  async fn handle_follower_message(&mut self, msg: Message) -> Option<TransitionTo> {
    match msg {
      Message::AppendEntries(ae) => {
        if ae.term >= self.current_term {
          self.current_term = ae.term;
          self.voted_for = None;
          info!("got heartbeat from leader {}", ae.leader_id);
        }
        None // restart timer
      }

      Message::RequestVote { term, candidate_id } => {
        if term > self.current_term {
          self.current_term = term;
          self.voted_for = Some(candidate_id);
          let _ = self
            .network
            .send_message(
              candidate_id,
              Message::Vote {
                term,
                granted: true,
              },
            )
            .await;
          info!("voted for {}", candidate_id);
        } else if term == self.current_term && self.voted_for.is_none() {
          self.voted_for = Some(candidate_id);
          let _ = self
            .network
            .send_message(
              candidate_id,
              Message::Vote {
                term,
                granted: true,
              },
            )
            .await;
          info!("voted for {} (same term)", candidate_id);
        } else {
          let _ = self
            .network
            .send_message(
              candidate_id,
              Message::Vote {
                term,
                granted: false,
              },
            )
            .await;
          info!("rejected vote for {}", candidate_id);
        }
        None
      }

      _ => None,
    }
  }
}

impl Step for StateMachine<Follower> {
  type Conclusion = DynamicServer;

  fn step(mut self) -> Pin<Box<dyn Future<Output = Self::Conclusion>>> {
    Box::pin(async move {
      info!("running as Follower (term {})", self.current_term);

      loop {
        let timeout = random_election_timeout();
        let timer = sleep(timeout);

        tokio::select! {
          _ = timer => {
            info!("election timeout");
            return self.into_candidate().into();
          },
          Some(msg) = self.rx.recv() => {
            // followers dont transition through message handling as of yet
            let _ = self.handle_follower_message(msg).await;
          }
        }
      }
    })
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

impl StateMachine<Candidate> {
  async fn start_election(&mut self) {
    self.current_term += 1;
    self.voted_for = Some(self.id);
    self.state.vote_count = 1; // voted for self
    info!("starting election for term {}", self.current_term);

    for peer_id in &self.peer_ids {
      if let Err(e) = self
        .network
        .send_message(
          *peer_id,
          Message::RequestVote {
            term: self.current_term,
            candidate_id: self.id,
          },
        )
        .await
      {
        let _ = e; // warn!("failed to send message to {}: {}", peer_id, e);
      }
    }
  }

  async fn handle_candidate_message(&mut self, msg: Message) -> Option<TransitionTo> {
    match msg {
      Message::Vote { term, granted } => {
        if term == self.current_term && granted {
          self.state.vote_count += 1;
          if self.state.vote_count > self.peer_ids.len().div_ceil(2) {
            info!("became leader!");
            Some(TransitionTo::Leader)
          } else {
            None
          }
        } else {
          None
        }
      }

      Message::AppendEntries(ae) => {
        if ae.term >= self.current_term {
          info!("stepping down to follower");
          self.current_term = ae.term;
          Some(TransitionTo::Follower)
        } else {
          None
        }
      }

      Message::RequestVote { term, candidate_id } => {
        if term > self.current_term {
          info!(
            "stepping down due to higher term {} from {}",
            term, candidate_id
          );
          self.current_term = term;
          Some(TransitionTo::Follower)
        } else {
          None
        }
      }
    }
  }
}

impl Step for StateMachine<Candidate> {
  type Conclusion = DynamicServer;

  fn step(mut self) -> Pin<Box<dyn Future<Output = Self::Conclusion>>> {
    Box::pin(async move {
      self.start_election().await;
      let election_deadline = Instant::now() + random_election_timeout();

      while Instant::now() < election_deadline {
        tokio::select! {
          Some(msg) = self.rx.recv() => {
            return match self.handle_candidate_message(msg).await {
              Some(TransitionTo::Leader) => self.into_leader().into(),
              Some(TransitionTo::Follower) => self.into_follower().into(),
              // this is kinda hacky, we avoid returning by continuing...
              _ => continue,
            };
          }
          _ = sleep(Duration::from_millis(10)) => {}
        }
      }

      warn!("election failed, retrying...");

      self.into()
    })
  }
}

impl StateMachine<Leader> {
  fn into_follower(self) -> StateMachine<Follower> {
    self.transfer_to(Follower)
  }
}

impl StateMachine<Leader> {
  async fn handle_leader_message(&mut self, msg: Message) -> Option<TransitionTo> {
    match msg {
      Message::AppendEntries(ae) if ae.term > self.current_term => {
        info!(
          "stepping down, new leader {} (term {})",
          ae.leader_id, ae.term
        );
        self.current_term = ae.term;
        Some(TransitionTo::Follower)
      }
      _ => None,
    }
  }

  async fn beat_heart(&self) {
    for peer_id in &self.peer_ids {
      if let Err(e) = self
        .network
        .send_message(
          *peer_id,
          Message::AppendEntries(AppendEntries {
            term: self.current_term,
            leader_id: self.id,
          }),
        )
        .await
      {
        let _ = e; // warn!("failed to send message to {}: {}", peer_id, e);
      }
    }
    info!("sent heartbeat");
  }
}

impl Step for StateMachine<Leader> {
  type Conclusion = DynamicServer;

  fn step(mut self) -> Pin<Box<dyn Future<Output = Self::Conclusion>>> {
    Box::pin(async move {
      let mut ticker = interval(Duration::from_millis(100));
      loop {
        ticker.tick().await;
        // send out heartbeat to all connected nodes
        self.beat_heart().await;
        while let Ok(Some(msg)) = timeout(Duration::from_millis(5), self.rx.recv()).await {
          return match self.handle_leader_message(msg).await {
            Some(TransitionTo::Follower) => self.into_follower().into(),
            _ => continue,
          };
        }
      }
    })
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
      state: Follower,
    })
  }
}

impl Step for DynamicServer {
  type Conclusion = Self;

  fn step(self) -> Pin<Box<dyn Future<Output = Self::Conclusion>>> {
    Box::pin(async move {
      match self {
        Self::Follower(srv) => srv.step().await,
        Self::Candidate(srv) => srv.step().await,
        Self::Leader(srv) => srv.step().await,
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
  id: NodeId,
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
      id,
      inner: Some(DynamicServer::new(id, network, peer_ids, rx)),
    }
  }

  #[instrument(skip(self), fields(node = %self.id))]
  pub async fn spin(mut self) {
    loop {
      let inner = self.inner.take().expect("inner should always be present");
      self.inner = Some(inner.step().await);
    }
  }
}
