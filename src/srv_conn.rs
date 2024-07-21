// This module contains the server-side connection handler.
//
// Server connections are initiated when a ReadReq or a WriteReq is received. The server handles
// this by creating a ServerConnectionHandler instance. The ServerConnectionHandler works in
// roughly the following stages:
//
// 1. Check whether the request can be serviced by interacting with the filesystem.
// 2. If the request can be serviced, enter a phase of work and wait cycles. The work cycles
//    primarily involve reading or writing to files and sending packets, and the wait cycles
//    involve waiting for the client to respond to the output of the work cycles with packets of
//    its own.
// 3. Eventually the request enters a terminal phase where it is waiting for a final packet or
//    timeout. After timeout or receipt of this packet, the connection is closed. The connection
//    may also terminate on receipt of or sending of an error packet.
//
// There are two ServerConnectionHandlers: one for read requests, and one for write requests. The
// read request handler works to send data packets, reads files, and waits for ack packets. The
// write request handler sends ack packets, writes files, and waits for data packets. These
// operations are similar enough that they can share the same data structure to maintain state and
// expose the same management API to the connection management system, but the implementations of
// what happens during the work, wait, and termination phases are distinct.

use crate::tftp;
use rand::Rng;
use tokio::time::Instant;
use std::error;
use std::fmt;
use std::net::{IpAddr, SocketAddr};
use std::time::Duration;
use tokio::fs::File;
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};

/// An object responsible for handling a request.
pub struct ServerRequestHandler {
    /// The TFTP socket used to send and receive connections.
    sock: tftp::TftpSocket,

    /// The address to send packets to.
    dst: SocketAddr,

    /// The packet processor.
    processor: PacketProcessor,
}

/// Attempts to bind to a random UDP socket until one succeeds.
fn bind_random_socket(peer_ip: IpAddr) -> tftp::TftpSocket {
    let mut rng = rand::thread_rng();
    let mut sock = tftp::TftpSocket::bind((peer_ip, rng.gen_range(1024..65535)).into());
    while !sock.is_ok() {
        sock = tftp::TftpSocket::bind((peer_ip, rand::thread_rng().gen_range(1024..65535)).into());
    }
    sock.unwrap()
}

async fn send_error_packet(
    sock: &mut tftp::TftpSocket,
    dst: SocketAddr,
    code: tftp::ErrorCode,
    message: String,
) {
    // Error packet is sent as a courtesy, we don't care how it goes.
    let _ = sock.send(&tftp::Packet::Error { code, message }, dst).await;
}

impl ServerRequestHandler {
    pub async fn new(
        initial_request: &tftp::Packet,
        src: SocketAddr,
    ) -> Result<ServerRequestHandler, ServerConnectionError> {
        let mut sock = bind_random_socket(src.ip());
        match initial_request {
            tftp::Packet::ReadReq { path, mode } => {
                if *mode == tftp::FileMode::Mail {
                    send_error_packet(
                        &mut sock,
                        src,
                        tftp::ErrorCode::Undefined,
                        "Support for mail mode is unimplemented".to_string(),
                    )
                    .await;
                    return Err(ServerConnectionError::BadRequest(
                        "Peer requested mail mode, which is unsupported".to_string(),
                    ));
                }

                let file = File::open(path).await;
                match file {
                    Err(e) => {
                        send_error_packet(&mut sock, src, e.kind().into(), format!("{:#?}", e))
                            .await;
                        Err(e.into())
                    }
                    Ok(f) => Ok(ServerRequestHandler {
                        sock,
                        dst: src,
                        processor: PacketProcessor::Read(ReadProcessor::new(f)),
                    }),
                }
            }
            tftp::Packet::WriteReq { path, mode } => {
                if *mode == tftp::FileMode::Mail {
                    send_error_packet(
                        &mut sock,
                        src,
                        tftp::ErrorCode::Undefined,
                        "Support for mail mode is unimplemented".to_string(),
                    )
                    .await;
                    return Err(ServerConnectionError::BadRequest(
                        "Peer requested mail mode, which is unsupported".to_string(),
                    ));
                }

                let file = File::options()
                    .read(true)
                    .write(true)
                    .create(true)
                    .open(path)
                    .await;
                match file {
                    Err(e) => {
                        send_error_packet(&mut sock, src, e.kind().into(), format!("{:#?}", e))
                            .await;
                        Err(e.into())
                    }
                    Ok(f) => Ok(ServerRequestHandler {
                        sock,
                        dst: src,
                        processor: PacketProcessor::Write(WriteProcessor::new(f)),
                    }),
                }
            }
            &_ => Err(ServerConnectionError::BadRequest(
                "Only read and write requests are valid initial requests.".to_string(),
            )),
        }
    }

    /// Does the work of sending and receiving data over the connection until the connection closes
    pub async fn handle(&mut self, connection_timeout: Duration) {
        let mut out_packet = match self.processor.first_packet().await {
            ResultAction::CloseConnection(_) | ResultAction::RetryRecv => {
                log::error!("Packet processor said we should close the connection or retry receipt before the first message was sent. This should never happen!");
                send_error_packet(
                    &mut self.sock,
                    self.dst,
                    tftp::ErrorCode::Undefined,
                    "Internal error, please retry".to_string(),
                )
                .await;
                return;
            }
            ResultAction::SendPacketAndAwait(p) => p,
            ResultAction::TerminateWithPacket(p) => {
                let _ = self.sock.send(&p, self.dst);
                return;
            }
        };

        // This loop is used for message sending as well as retries, depending on whether
        // |out_packet| has been overwritten.
        loop {
            if let Err(e) = self.sock.send(&out_packet, self.dst).await {
                log::warn!("Unable to send packet: {e}");
                return;
            }

            let started_waiting = Instant::now();
            while started_waiting.elapsed() < connection_timeout {
                match self
                    .sock
                    .recv_with_timeout(Duration::from_millis(500))
                    .await
                {
                    Ok((packet, src)) => {
                        // First, check whether the data came from the src we were expecting.
                        if src != self.dst {
                            // Terminate the connection the peer may be trying to start on this
                            // port.
                            send_error_packet(
                                &mut self.sock,
                                src,
                                tftp::ErrorCode::Illegal,
                                "This connection has already been initiated with a different client, cannot send packets over this socket.".to_string()
                            ).await;
                            // Continue waiting for a good packet from the expected peer.
                            continue;
                        }

                        // Next, figure out how to respond.
                        match self.processor.process_packet(&packet).await {
                            ResultAction::SendPacketAndAwait(p) => {
                                out_packet = p;
                                break;
                            }
                            ResultAction::CloseConnection(maybe_warn) => {
                                if let Some(msg) = maybe_warn {
                                    log::warn!("Terminating connection, but note: {}", msg);
                                }
                                // No more packets to send, our work here is done!
                                return;
                            }
                            ResultAction::RetryRecv => {
                                continue;
                            }
                            ResultAction::TerminateWithPacket(p) => {
                                let _ = self.sock.send(&p, src).await;
                                return;
                            }
                        }
                    }
                    // If we timed out, rebroadcast the last sent packet
                    Err(tftp::SocketError::Timeout(_)) => {
                        break;
                    }
                    // If we couldn't parse the incoming packet, send and error and kill the
                    // connection.
                    Err(tftp::SocketError::PacketParse(msg)) => {
                        send_error_packet(
                            &mut self.sock,
                            self.dst,
                            tftp::ErrorCode::Illegal,
                            format!("Error parsing incoming packet: {msg}"),
                        )
                        .await;
                        return;
                    }
                    // If we had an I/O error involving the socket, send and error and kill the
                    // connection.
                    Err(tftp::SocketError::IO(e)) => {
                        send_error_packet(
                            &mut self.sock,
                            self.dst,
                            tftp::ErrorCode::Undefined,
                            format!("I/O error: {:#?}", e),
                        )
                        .await;
                        return;
                    }
                }
            }
        }
    }
}

enum PacketProcessor {
    Read(ReadProcessor),
    Write(WriteProcessor),
}

/// Represents an action that the caller of PacketProcessor should take in
/// response to processing a packet.
enum ResultAction {
    /// Caller should send the packet and await a response.
    SendPacketAndAwait(tftp::Packet),

    /// Caller should close the connection without sending a message, optionally logging a string.
    CloseConnection(Option<String>),

    /// Caller should try receiving the last packet again.
    RetryRecv,

    /// Caller should terminate the connection by sending the packet.
    TerminateWithPacket(tftp::Packet),
}

struct ReadProcessor {
    f: File,
    curr_block: u16,
    awaiting_final_ack: bool,
}

impl ReadProcessor {
    fn new(f: File) -> ReadProcessor {
        ReadProcessor {
            f,
            curr_block: 0,
            awaiting_final_ack: false,
        }
    }

    async fn process_ack(&mut self, packet: &tftp::Packet) -> ResultAction {
        match packet {
            &tftp::Packet::Ack { block } => {
                match block {
                    block if block == self.curr_block => {
                        if self.awaiting_final_ack {
                            return ResultAction::CloseConnection(None)
                        }
                        match read_block_from_file(&mut self.f).await {
                            Ok(data) => {
                                self.curr_block += 1;
                                if data.len() < tftp::DATA_BUFFER_SIZE {
                                    self.awaiting_final_ack = true;
                                }
                                ResultAction::SendPacketAndAwait(
                                    tftp::Packet::Data { block: self.curr_block , data }
                                )
                            },
                            Err(e) => ResultAction::TerminateWithPacket(
                                tftp::Packet::Error { code: e.kind().into(), message: format!("Failed to read from file: {:#?}", e) }
                            ),
                        }
                    },
                    block if block < self.curr_block => {
                        // Ignore acks for blocks we know have already been acknowledged.
                        ResultAction::RetryRecv
                    },
                    _ => {
                        ResultAction::TerminateWithPacket(
                            tftp::Packet::Error {
                                code: tftp::ErrorCode::Illegal,
                                message: format!(
                                    "Cannot acknowledge a block which was not yet sent. Server's current block is {cb}, but received an ack for {block}",
                                    cb = self.curr_block
                                )
                            }
                        )
                    },
                }
            }
            _ => ResultAction::TerminateWithPacket(tftp::Packet::Error {
                code: tftp::ErrorCode::Illegal,
                message: format!(
                    "Expected to receive an Ack message on this connection, but got {:#?} instead",
                    packet
                ),
            }),
        }
    }
}

async fn read_block_from_file(f: &mut File) -> Result<Vec<u8>, io::Error> {
    let mut buf = vec![0_u8; tftp::DATA_BUFFER_SIZE];
    let mut cursor = 0;

    // Reading works this way because we have no guarantee that a particular call to read will
    // actually fill the buffer all the way. To compensate for this, if we don't fully fill the
    // buffer on the call to read, we pass a progressively smaller slice of the buffer that we
    // haven't yet populated into the read function until we reach the end of the file or fully
    // populate the buffer.
    loop {
        match f.read(&mut buf[cursor..]).await {
            Ok(s) => {
                if cursor + s == buf.len() {
                    return Ok(buf);
                } else if s == 0 {
                    buf.truncate(cursor + s);
                    return Ok(buf);
                } else {
                    cursor += s;
                    continue;
                };
            }
            Err(e) => return Err(e),
        };
    }
}

struct WriteProcessor {
    f: File,
    curr_block: u16,
}

impl WriteProcessor {
    fn new(f: File) -> WriteProcessor {
        WriteProcessor { f, curr_block: 0 }
    }

    async fn process_data(&mut self, packet: &tftp::Packet) -> ResultAction {
        match packet {
            tftp::Packet::Data { block, data } => {
                match block {
                    block if *block == 0 && self.curr_block == 0 => {
                        ResultAction::SendPacketAndAwait(
                            tftp::Packet::Ack { block: 0 }
                        )
                    },
                    block if *block == self.curr_block + 1 => {
                        match write_block_to_file(&mut self.f, &data).await {
                            None => {
                                self.curr_block += 1;
                                let packet = tftp::Packet::Ack { block: self.curr_block };

                                if data.len() < tftp::DATA_BUFFER_SIZE {
                                    ResultAction::TerminateWithPacket(packet)
                                } else {
                                    ResultAction::SendPacketAndAwait(packet)
                                }
                            },
                            Some(e) => ResultAction::TerminateWithPacket(
                                tftp::Packet::Error {
                                    code: e.kind().into(),
                                    message: format!("Error writing to file: {:#?}", e)
                                }
                            ),
                        }
                    },
                    block if *block < self.curr_block + 1 => {
                        // Ignore data packets from previous requests that e.g. may have been
                        // duplicated in transit
                        ResultAction::RetryRecv
                    },
                    _ => ResultAction::TerminateWithPacket(
                        tftp::Packet::Error {
                            code: tftp::ErrorCode::Illegal,
                            message: format!(
                                "Data blocks must be received by the server in sequence. Server received data for block {block}, \
                                but the server has only received up to block {cb}.", cb = self.curr_block),
                        }
                    ),
                }
            }
            _ => ResultAction::TerminateWithPacket(tftp::Packet::Error {
                code: tftp::ErrorCode::Illegal,
                message: format!(
                    "Expected to receive a Data packet on this connection, but got {:#?} instead",
                    packet
                ),
            }),
        }
    }
}

async fn write_block_to_file(f: &mut File, buf: &[u8]) -> Option<io::Error> {
    let mut cursor = 0;
    loop {
        match f.write(&buf[cursor..]).await {
            Ok(s) => {
                if cursor + s == buf.len() {
                    return None;
                }
                cursor += s;
                continue;
            }
            Err(e) => return Some(e),
        }
    }
}

/// An entity that can process packets and produce a response.
impl PacketProcessor {
    async fn first_packet(&mut self) -> ResultAction {
        let first_packet = match self {
            PacketProcessor::Read(_) => tftp::Packet::Ack { block: 0 },
            PacketProcessor::Write(_) => tftp::Packet::Data {
                block: 0,
                data: vec![],
            },
        };
        self.process_packet(&first_packet).await
    }

    /// Given an incoming packet, processes it and describes the action the caller should take.
    async fn process_packet(&mut self, packet: &tftp::Packet) -> ResultAction {
        match self {
            PacketProcessor::Read(p) => p.process_ack(packet).await,
            PacketProcessor::Write(p) => p.process_data(packet).await,
        }
    }
}

#[derive(Debug)]
pub enum ServerConnectionError {
    BadRequest(String),
    File(std::io::Error),
    Internal(String),
}

impl error::Error for ServerConnectionError {}

impl fmt::Display for ServerConnectionError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::BadRequest(msg) => write!(f, "Invalid request: {:#?}", msg),
            Self::File(e) => write!(f, "File IO error: {:#?}", e),
            Self::Internal(msg) => write!(f, "Internal error: {:#?}", msg),
        }
    }
}

impl From<io::Error> for ServerConnectionError {
    fn from(e: io::Error) -> ServerConnectionError {
        ServerConnectionError::File(e)
    }
}
