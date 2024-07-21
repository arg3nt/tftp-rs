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

use crate::processor::{PacketProcessor, ResultAction};
use crate::tftp;
use rand::Rng;
use std::error;
use std::fmt;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::Path;
use std::time::Duration;
use tokio::io;
use tokio::time::Instant;

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
fn bind_random_socket() -> tftp::TftpSocket {
    let mut rng = rand::thread_rng();
    let mut sock = tftp::TftpSocket::bind((Ipv4Addr::UNSPECIFIED, rng.gen_range(1024..65535)).into());
    while !sock.is_ok() {
        log::warn!("Couldn't bind socket: {:#?}", sock);
        sock = tftp::TftpSocket::bind((Ipv4Addr::UNSPECIFIED, rand::thread_rng().gen_range(1024..65535)).into());
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
        path_prefix: &Path,
        initial_request: &tftp::Packet,
        src: SocketAddr,
    ) -> Result<ServerRequestHandler, ServerConnectionError> {
        log::info!("Binding socket");
        let mut sock = bind_random_socket();
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

                let open_path = if path.starts_with("/") { &path[1..] } else { &path[..] };
                let processor = match PacketProcessor::new_for_reading(&path_prefix.join(open_path)).await {
                    Ok(p) => p,
                    Err(e) => {
                        send_error_packet(&mut sock, src, e.kind().into(), format!("{:#?}", e)).await;
                        return Err(e.into());
                    },
                };

                Ok(ServerRequestHandler { sock, dst: src, processor})
            },
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

                let open_path = if path.starts_with("/") { &path[1..] } else { &path[..] };
                let processor = match PacketProcessor::new_for_writing(&path_prefix.join(open_path)).await {
                    Ok(p) => p,
                    Err(e) => {
                        send_error_packet(&mut sock, src, e.kind().into(), format!("{:#?}", e)).await;
                        return Err(e.into());
                    },
                };

                Ok(ServerRequestHandler { sock, dst: src, processor })
            },
            _ => Err(ServerConnectionError::BadRequest(
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
                log::warn!("Terminating request to {:#?} and sending packet {:#?}", p, self.dst);
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
                        log::info!("Got packet from {:#?}: {:#?}", src, packet);
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
                                    log::warn!("{}", msg);
                                }
                                log::info!("Closing connection with {:#?}", src);
                                // No more packets to send, our work here is done!
                                return;
                            }
                            ResultAction::RetryRecv => {
                                continue;
                            }
                            ResultAction::TerminateWithPacket(p) => {
                                let _ = self.sock.send(&p, src).await;
                                log::info!("Closing connection with {:#?}", src);
                                return;
                            }
                        }
                    }
                    // If we timed out, rebroadcast the last sent packet
                    Err(tftp::SocketError::Timeout(_)) => {
                        log::info!("Timed out, trying again");
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


#[derive(Debug)]
pub enum ServerConnectionError {
    BadRequest(String),
    File(io::Error),
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
