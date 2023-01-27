use libp2p::{
    core::upgrade,
    floodsub::{Floodsub, FloodsubEvent, Topic},
    futures::StreamExt,
    identity,
    mdns::{Mdns, MdnsEvent},
    mplex,
    noise::{Keypair, NoiseConfig, X25519Spec},
    swarm::{NetworkBehaviourEventProcess, Swarm, SwarmBuilder},
    tcp::TokioTcpConfig,
    NetworkBehaviour, PeerId, Transport,
};
use log::{error, info};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use tokio::{fs, io::AsyncBufReadExt, sync::mpsc};

use rand::Rng;
use std::env;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;

use std::net::SocketAddrV4;
use std::time::{Duration, Instant};

const STORAGE_FILE_PATH: &str = "./KeyValuePairs.json";

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync + 'static>>;
type KeyValuePairs = Vec<KeyValuePair>;
static KEYS: Lazy<identity::Keypair> = Lazy::new(|| identity::Keypair::generate_ed25519());
static PEER_ID: Lazy<PeerId> = Lazy::new(|| PeerId::from(KEYS.public()));
static TOPIC: Lazy<Topic> = Lazy::new(|| Topic::new("KeyValuePairs"));

#[derive(Debug, Serialize, Deserialize)]
struct KeyValuePair {
    id: usize,
    key: String,
    value: String,
    public: bool,
}

#[derive(Debug, Serialize, Deserialize)]
enum ListMode {
    ALL,
    One(String),
}

#[derive(Debug, Serialize, Deserialize)]
struct ListRequest {
    mode: ListMode,
}

#[derive(Debug, Serialize, Deserialize)]
struct ListResponse {
    mode: ListMode,
    data: KeyValuePairs,
    receiver: String,
}

// client -----------------------------
#[derive(Debug, Serialize, Deserialize)]
pub struct ClientRequest {
    request_id: i128,
    sender_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ClientResponse {
    request_id: i128,
    sender_id: String,
    server_id: String,
    done: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ServerDeclaration {
    sender_id: String,
}

//-------------------------------

enum EventType {
    Response(ClientResponse),
    Input(String),
}

#[derive(NetworkBehaviour)]
struct KeyValuePairBehaviour {
    floodsub: Floodsub,
    mdns: Mdns,
    #[behaviour(ignore)]
    response_sender: mpsc::UnboundedSender<ClientResponse>,
}

// Types.rs
#[derive(Debug, PartialEq)]
pub enum State {
    FOLLOWER,
    LEADER,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Heartbeat {
    leader_id: String,
}

#[derive(Debug)]
pub struct Server {
    pub id: String,
    pub state: State,
    pub timeout_threshold: i32,
    pub timeout: i32,
    pub leader: Option<String>,
    pub isElection: bool,
    pub past_leader: String,
    pub is_server: i32,
    pub peers: Vec<String>, // vector of server ids
    pub requests_cnt: i32,
    // pub client_request_start: Instant,
}

impl Server {
    pub fn new(id: String, timeout_threshold: i32) -> Self {
        Server {
            id: id,
            state: State::FOLLOWER,
            timeout: 1, // config
            timeout_threshold: timeout_threshold,
            leader: None,
            isElection: false,
            past_leader: "".to_string(),
            is_server: 1,
            peers: Vec::new(),
            requests_cnt: 0,
            // client_request_start: Instant::now(),
        }
    }
    pub fn refresh_timeout(self: &mut Self) {
        self.timeout = 1;
    }
    pub fn become_leader(self: &mut Self) {
        info!("Server {} has won the election!", self.id);
        self.state = State::LEADER;
    }
    pub fn has_timed_out(self: &mut Self) -> bool {
        if self.timeout == 0 {
            return true;
        }
        self.timeout = (self.timeout + 1) % self.timeout_threshold;
        return false;
    }
    pub fn start(self: &mut Self) {
        self.refresh_timeout();
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct ElectionResult {
    pub initiator_id: String,
    pub leader: String,
}

static server: Lazy<Arc<Mutex<Server>>> = Lazy::new(|| {
    Arc::new(Mutex::new(Server::new(
        PEER_ID.to_string(),
        rand::thread_rng().gen_range(50..100),
    )))
});

// ----------------------------------------------------

impl NetworkBehaviourEventProcess<FloodsubEvent> for KeyValuePairBehaviour {
    fn inject_event(&mut self, event: FloodsubEvent) {
        match event {
            FloodsubEvent::Message(msg) => {
                if let Ok(req) = serde_json::from_slice::<ElectionResult>(&msg.data) {
                    let mut server_tmp = server.lock().unwrap();

                    if server_tmp.is_server == 1 {
                        println!("Received election result!!");

                        server_tmp.refresh_timeout();
                        if server_tmp.id.to_string() == req.leader {
                            server_tmp.state = State::LEADER;
                            println!("I am the winner, Current State : {:?}", server_tmp.state);
                        } else {
                            server_tmp.state = State::FOLLOWER;
                            println!(
                                "Election resutlt by {}: Node {} is the leader",
                                req.initiator_id,
                                req.leader.to_string()
                            );
                        }
                        server_tmp.leader = Some(req.leader.to_string());
                        server_tmp.past_leader = req.leader.to_string();
                    }
                } else if let Ok(req) = serde_json::from_slice::<ClientResponse>(&msg.data) {
                    let mut server_tmp = server.lock().unwrap();

                    if req.done && server_tmp.is_server == 0 && req.sender_id == server_tmp.id {
                        // let server_response_to_client_duration =
                        // server.lock().unwrap().client_request_start.elapsed();
                        println!("Received Response From Server {}", req.server_id);
                    } else if !req.done
                        && server_tmp.is_server == 1
                        && req.server_id == server_tmp.id
                    {
                        println!("Recieved undone request {}!", req.request_id);
                        server_tmp.refresh_timeout();
                        handle_client_response(self.response_sender.clone(), req);
                    }
                } else if let Ok(req) = serde_json::from_slice::<Heartbeat>(&msg.data) {
                    let mut server_tmp = server.lock().unwrap();

                    if server_tmp.is_server == 1 {
                        server_tmp.refresh_timeout();
                        println!("Recieved heartbeat from leader {}.", req.leader_id);
                        server_tmp.leader = Some(req.leader_id.to_string());
                        server_tmp.past_leader = req.leader_id.to_string();
                    }
                } else if let Ok(req) = serde_json::from_slice::<ClientRequest>(&msg.data) {
                    let mut server_tmp = server.lock().unwrap();
                    if server_tmp.state == State::LEADER {
                        let handler_id = server_tmp.requests_cnt % (server_tmp.peers.len() as i32);
                        let handler = server_tmp.peers[(handler_id as usize)].clone();
                        server_tmp.requests_cnt = server_tmp.requests_cnt.clone() + 1;
                        println!(
                            " Received request id {} from {}",
                            req.request_id,
                            req.sender_id.to_string()
                        );
                        forward_client_request(
                            self.response_sender.clone(),
                            req,
                            handler.to_string(),
                        );
                    }
                } else if let Ok(req) = serde_json::from_slice::<ServerDeclaration>(&msg.data) {
                    let mut server_tmp = server.lock().unwrap();

                    if server_tmp.is_server == 1 && !server_tmp.peers.contains(&req.sender_id) {
                        println!("Added Server : {:?}", req.sender_id);
                        server_tmp.peers.push(req.sender_id);
                    }
                } else if true {
                    println!("Unhandled request!-----");
                }
            }
            _ => (),
        }
    }
}

fn handle_client_response(sender: mpsc::UnboundedSender<ClientResponse>, req: ClientResponse) {
    tokio::spawn(async move {
        let resp = ClientResponse {
            request_id: req.request_id,
            sender_id: req.sender_id,
            server_id: req.server_id,
            done: true,
        };
        if let Err(e) = sender.send(resp) {
            error!("error sending response via channel, {}", e);
        }
    });
}

fn forward_client_request(
    sender: mpsc::UnboundedSender<ClientResponse>,
    req: ClientRequest,
    handler: String,
) {
    tokio::spawn(async move {
        let resp = ClientResponse {
            request_id: req.request_id,
            sender_id: req.sender_id,
            server_id: handler,
            done: false,
        };
        if let Err(e) = sender.send(resp) {
            error!("error sending response via channel, {}", e);
        }
    });
}

impl NetworkBehaviourEventProcess<MdnsEvent> for KeyValuePairBehaviour {
    fn inject_event(&mut self, event: MdnsEvent) {
        match event {
            MdnsEvent::Discovered(discovered_list) => {
                for (peer, _addr) in discovered_list {
                    self.floodsub.add_node_to_partial_view(peer);
                }
            }
            MdnsEvent::Expired(expired_list) => {
                for (peer, _addr) in expired_list {
                    if !self.mdns.has_node(&peer) {
                        self.floodsub.remove_node_from_partial_view(&peer);
                    }
                }
            }
        }
    }
}

async fn leader_from_peers(
    swarm: &mut Swarm<KeyValuePairBehaviour>,
    server_tmp: &mut Server,
) -> String {
    let nodes = swarm.behaviour().mdns.discovered_nodes();
    let mut unique_peers = Vec::new();

    for peer in nodes {
        if peer.to_string() != server_tmp.past_leader {
            unique_peers.push(peer.to_string());
        }
    }
    unique_peers.push(server_tmp.id.clone());
    unique_peers.sort();

    return unique_peers.last().unwrap().to_string();
}

async fn elect_leader(swarm: &mut Swarm<KeyValuePairBehaviour>) {
    let start_election_time = Instant::now();
    let mut server_tmp = server.lock().unwrap();
    let mut i = 0;
    while i < server_tmp.peers.len() {
        if server_tmp.peers[i] == server_tmp.past_leader {
            server_tmp.peers[i] = "0".to_string();
        }
        i = i + 1;
    }
    server_tmp.peers.sort();
    let mut leader = server_tmp.peers.last().unwrap().to_string();

    server_tmp.refresh_timeout();
    let server_id = server_tmp.id.to_string();
    if server_tmp.id > leader {
        leader = server_id.clone();
    }
    if leader == server_id {
        println!("I elected myself haha");
        server_tmp.past_leader = server_id.clone();
        server_tmp.leader = Some(server_id.clone());

        server_tmp.state = State::LEADER;
    } else {
        let election_duration = start_election_time.elapsed();
        println!(
            "I elected {} as leader. Election ends!, Duration: {:?}",
            leader, election_duration
        );
    }

    let req = ElectionResult {
        initiator_id: server_id.to_string(),
        leader: leader,
    };

    let json = serde_json::to_string(&req).expect("can jsonify request");
    swarm
        .behaviour_mut()
        .floodsub
        .publish(TOPIC.clone(), json.as_bytes());
}

fn handle_timeout() -> bool {
    let server_id = server.lock().unwrap().id.to_string();
    let has_timed_out = server.lock().unwrap().has_timed_out();

    return has_timed_out;
}

async fn respond_to_client(swarm: &mut Swarm<KeyValuePairBehaviour>, resp: ClientResponse) {
    println!("Responding to client!");
    let json = serde_json::to_string(&resp).expect("can jsonify request");
    swarm
        .behaviour_mut()
        .floodsub
        .publish(TOPIC.clone(), json.as_bytes());
}

async fn broadcast_heartbeat(swarm: &mut Swarm<KeyValuePairBehaviour>, resp: ClientResponse) {
    if resp.sender_id != "None" {
        println!("Sending data");
        let json = serde_json::to_string(&resp).expect("can jsonify request");
        swarm
            .behaviour_mut()
            .floodsub
            .publish(TOPIC.clone(), json.as_bytes());
    } else {
        println!("Sending heartbeat!");
        let id = server.lock().unwrap().id.to_string();
        let req = Heartbeat { leader_id: id };

        let json2 = serde_json::to_string(&req).expect("can jsonify request");
        swarm
            .behaviour_mut()
            .floodsub
            .publish(TOPIC.clone(), json2.as_bytes());
    }
}

async fn broadcast_client(swarm: &mut Swarm<KeyValuePairBehaviour>, request_id: i128) {
    println!("Client Request is working!!!");
    let req = ClientRequest {
        request_id: request_id,
        sender_id: PEER_ID.clone().to_string(),
    };

    let json = serde_json::to_string(&req).expect("can jsonify request");
    swarm
        .behaviour_mut()
        .floodsub
        .publish(TOPIC.clone(), json.as_bytes());
}

#[tokio::main]
async fn main() {
    pretty_env_logger::init();

    println!("Peer Id: {}", PEER_ID.clone());

    // ---------------------------------------------------

    let (response_sender, mut response_rcv) = mpsc::unbounded_channel();

    let auth_keys = Keypair::<X25519Spec>::new()
        .into_authentic(&KEYS)
        .expect("can create auth keys");

    let transp = TokioTcpConfig::new()
        .upgrade(upgrade::Version::V1)
        .authenticate(NoiseConfig::xx(auth_keys).into_authenticated()) // XX Handshake pattern, IX exists as well and IK - only XX currently provides interop with other libp2p impls
        .multiplex(mplex::MplexConfig::new())
        .boxed();

    let mut behaviour = KeyValuePairBehaviour {
        floodsub: Floodsub::new(PEER_ID.clone()),
        mdns: Mdns::new(Default::default())
            .await
            .expect("can create mdns"),
        response_sender,
    };

    behaviour.floodsub.subscribe(TOPIC.clone());

    let mut swarm = SwarmBuilder::new(transp, behaviour, PEER_ID.clone())
        .executor(Box::new(|fut| {
            tokio::spawn(fut);
        }))
        .build();

    Swarm::listen_on(
        &mut swarm,
        "/ip4/0.0.0.0/tcp/0"
            .parse()
            .expect("can get a local socket"),
    )
    .expect("swarm can be started");

    let args: Vec<String> = env::args().collect();
    let thres: i32 = args[1].parse::<i32>().unwrap();
    server.lock().unwrap().timeout_threshold = thres;

    let is_client: i32 = args[2].parse::<i32>().unwrap();

    // Start server
    let start_server_time = Instant::now();
    server.lock().unwrap().start();
    println!(
        "Start server with timeout {}!",
        server.lock().unwrap().timeout_threshold
    );

    let mut heart_beat_count = 1;

    let mut request_id: i128 = 0;

    if is_client == 1 {
        server.lock().unwrap().is_server = 0;
    }

    let ten_millis = Duration::from_millis(3000);
    let now = Instant::now();
    thread::sleep(ten_millis);
    assert!(now.elapsed() >= ten_millis);

    for n in 1..30 {
        if is_client != 1 {
            let req = ServerDeclaration {
                sender_id: PEER_ID.clone().to_string(),
            };
            let json = serde_json::to_string(&req).expect("can jsonify request");
            swarm
                .behaviour_mut()
                .floodsub
                .publish(TOPIC.clone(), json.as_bytes());

            let evt = {
                tokio::select! {
                    response = response_rcv.recv() => Some(EventType::Response(response.expect("response exists"))),
                    event = swarm.select_next_some() => {
                        info!("Unhandled Swarm Event: {:?}", event);
                        None
                    },
                }
            };

            if let Some(event) = evt {
                match event {
                    EventType::Response(resp) => {
                        let json = serde_json::to_string(&resp).expect("can jsonify response");
                        swarm
                            .behaviour_mut()
                            .floodsub
                            .publish(TOPIC.clone(), json.as_bytes());
                    }
                    EventType::Input(line) => match line.as_str() {
                        _ => error!("unknown command"),
                    },
                }
            }
        }
    }
    println!(
        "Length of discovered peers : {:?}",
        server.lock().unwrap().peers.len()
    );

    loop {
        let evt = {
            tokio::select! {
                response = response_rcv.recv() => Some(EventType::Response(response.expect("response exists"))),
                event = swarm.select_next_some() => {
                    info!("Unhandled Swarm Event: {:?}", event);
                    None
                },
            }
        };

        let mut recv_resp = ClientResponse {
            request_id: 0,
            sender_id: "None".to_string(),
            server_id: "None".to_string(),
            done: false,
        };

        if let Some(event) = evt {
            match event {
                EventType::Response(resp) => {
                    recv_resp = resp;
                    if server.lock().unwrap().state == State::LEADER {
                        println!("Forwarding Request !!");
                    } else {
                        println!("Responding to Request!!");
                    }
                }
                EventType::Input(line) => match line.as_str() {
                    _ => error!("unknown command"),
                },
            }
        }

        if is_client == 1 {
            // server.lock().unwrap().client_request_start = Instant::now();
            broadcast_client(&mut swarm, request_id).await;
            request_id = request_id + 1;
        } else {
            let is_leader = server.lock().unwrap().state == State::LEADER;
            if is_leader {
                if recv_resp.sender_id != "None" {
                    broadcast_heartbeat(&mut swarm, recv_resp).await;
                } else if heart_beat_count == 0 {
                    broadcast_heartbeat(&mut swarm, recv_resp).await;
                } else {
                    heart_beat_count = heart_beat_count + 1;
                    heart_beat_count = heart_beat_count % 20;
                }
            } else {
                let server_id = server.lock().unwrap().id.to_string();
                let has_timed_out = handle_timeout();
                if has_timed_out {
                    println!("Server {} has timed out.", server_id);
                    elect_leader(&mut swarm).await;
                }
                if recv_resp.sender_id != "None" {
                    respond_to_client(&mut swarm, recv_resp).await;
                }
            }
        }
    }
}
