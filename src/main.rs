use std::collections::HashMap;
use std::net::{SocketAddr, TcpListener};
use std::sync::{Arc, RwLock};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::protocol::CloseCode;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;
use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};

use hyper_hyper_space_core::identity::Identity;
use hyper_hyper_space_core::linkup::LinkupAddress;
use hyper_hyper_space_core::util::multimap::MultiMap;
use hyper_hyper_space_core::HashedObject;

type Tx = Sender<Message>;
type PeerMap = Arc<RwLock<MultiMap<String, Tx>>>;
type ConnectionMap = Arc<RwLock<HashMap<SocketAddr, HashSet<String>>>>;

#[derive(Serialize, Deserialize)]
#[serde(tag = "action")]
enum Message {
    Pong,
    Ping,
    Listen { linkup_id: String },
    Send { linkup_id: String, limit: Option<usize> },
    Query { linkup_ids: Vec<String>, query_id: String },
    QueryReply { query_id: String, hits: Vec<String> },
    UpdateChallenge { challenge: String },
}

#[tokio::main]
async fn main() {
    let addr: SocketAddr = "0.0.0.0:3002".parse().unwrap();
    let state: PeerMap = Arc::new(RwLock::new(MultiMap::new()));
    let connections: ConnectionMap = Arc::new(RwLock::new(HashMap::new()));

    let listener = TcpListener::bind(&addr).await.expect("Failed to bind");
    println!("Started Hyper Hyper Space's signaling server on {:?}", addr);

    while let Ok((stream, _)) = listener.accept().await {
        let addr = stream.peer_addr().expect("Failed to get peer address");

        let state = Arc::clone(&state);

        tokio::spawn(async move {
            let ws_stream = tokio_tungstenite::accept_async(stream)
                .await
                .expect("Error during the websocket handshake");

            process_ws(ws_stream, addr, state).await;
        });
    }
}

use tokio_tungstenite::tungstenite::Error;

async fn process_ws(
    mut ws_stream: WebSocketStream<TcpStream>,
    addr: SocketAddr,
    state: PeerMap,
    connections: ConnectionMap,
) {
    let (mut tx, mut rx) = ws_stream.split();

    // Generate UUID for the connection
    let uuid = uuid::Uuid::new_v4().to_string();

    // Send the initial UpdateChallenge message
    let _ = tx
        .send(Message::text(
            serde_json::to_string(&Message::UpdateChallenge {
                challenge: uuid.clone(),
            })
            .unwrap(),
        ))
        .await;

    // Process incoming messages
    while let Some(msg) = rx.next().await {
        match msg {
            Ok(msg) => {
                if msg.is_text() {
                    match serde_json::from_str::<Message>(&msg.into_text().unwrap()) {
                        Ok(message) => match message {
                            Message::Ping => {
                                let _ = tx.send(Message::text(serde_json::to_string(&Message::Pong).unwrap())).await;
                            }
                            Message::Listen { linkup_id } => {
                                let mut conns = connections.write().unwrap();
                                let entry = conns.entry(addr).or_insert_with(HashSet::new);
                                entry.insert(linkup_id.clone());

                                let mut peers = state.write().unwrap();
                                peers.insert(linkup_id, tx.clone());
                            }
                            Message::Send { linkup_id, limit } => {
                                let peers = state.read().unwrap();
                                if let Some(targets) = peers.get(&linkup_id) {
                                    let targets = if let Some(limit) = limit {
                                        targets.iter().take(limit).cloned().collect::<Vec<_>>()
                                    } else {
                                        targets.iter().cloned().collect::<Vec<_>>()
                                    };

                                    for target in targets {
                                        let _ = target.send(msg.clone()).await;
                                    }
                                }
                            }
                            Message::Query { linkup_ids, query_id } => {
                                let hits = {
                                    let peers = state.read().unwrap();
                                    linkup_ids
                                        .into_iter()
                                        .filter(|id| peers.contains_key(id))
                                        .collect::<Vec<_>>()
                                };
                                let _ = tx
                                    .send(Message::text(
                                        serde_json::to_string(&Message::QueryReply {
                                            query_id,
                                            hits,
                                        })
                                        .unwrap(),
                                    ))
                                    .await;
                            }
                            _ => {}
                        },
                        Err(_) => {
                            // Ignore the malformed message
                        }
                    }
                }
            }
            Err(_) => {
                // Ignore the malformed message
            }
        }
    }

    // Perform cleanup when the connection is closed
    {
        let mut peers = state.write().unwrap();
        if let Some(connection_linkup_ids) = connections.write().unwrap().remove(&addr) {
            for linkup_id in connection_linkup_ids {
                peers.remove(&linkup_id, &tx);
            }
        }
    }
}
