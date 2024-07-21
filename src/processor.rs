use crate::tftp;
use std::path::Path;
use tokio::fs::File;
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};

#[derive(Debug)]
pub enum PacketProcessor {
    Read(ReadProcessor),
    Write(WriteProcessor),
}

/// An entity that can process packets and produce a response.
impl PacketProcessor {
    pub async fn new_for_reading(path: &Path) -> Result<PacketProcessor, io::Error> {
        match File::open(path).await {
            Ok(f) => Ok(PacketProcessor::Read(ReadProcessor::new(f))),
            Err(e) => Err(e),
        }
    }

    pub async fn new_for_writing(path: &Path) -> Result<PacketProcessor, io::Error> {
        log::info!("Writing to {:#?}", path);
        match File::create_new(path).await
        {
            Ok(f) => Ok(PacketProcessor::Write(WriteProcessor::new(f))),
            Err(e) => Err(e),
        }
    }

    pub async fn first_packet(&mut self) -> ResultAction {
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
    pub async fn process_packet(&mut self, packet: &tftp::Packet) -> ResultAction {
        match self {
            PacketProcessor::Read(p) => p.process_ack(packet).await,
            PacketProcessor::Write(p) => p.process_data(packet).await,
        }
    }
}

/// Represents an action that the caller of PacketProcessor should take in
/// response to processing a packet.
#[derive(Debug, PartialEq)]
pub enum ResultAction {
    /// Caller should send the packet and await a response.
    SendPacketAndAwait(tftp::Packet),

    /// Caller should close the connection without sending a message, optionally logging a string.
    CloseConnection(Option<String>),

    /// Caller should try receiving the last packet again.
    RetryRecv,

    /// Caller should terminate the connection by sending the packet.
    TerminateWithPacket(tftp::Packet),
}

#[derive(Debug)]
pub struct ReadProcessor {
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
            },
            tftp::Packet::Error { code, message } => ResultAction::CloseConnection(Some(format!(
                "Client sent error packet: code: {:#?}, message: '{}'",
                code, message
            ))),
            _ => ResultAction::TerminateWithPacket(tftp::Packet::Error {
                code: tftp::ErrorCode::Illegal,
                message: format!(
                    "Expected to receive an Ack packet, but got {:#?} instead",
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

#[derive(Debug)]
pub struct WriteProcessor {
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
            },
            tftp::Packet::Error { code, message } => ResultAction::CloseConnection(Some(format!(
                "Client sent error packet: code: {:#?}, message: '{}'",
                code, message
            ))),
            _ => ResultAction::TerminateWithPacket(tftp::Packet::Error {
                code: tftp::ErrorCode::Illegal,
                message: format!(
                    "Expected to receive a Data packet, but got {:#?} instead",
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

#[cfg(test)]
mod tests {
    use super::*;
    use tempdir::TempDir;

    #[tokio::test]
    async fn test_new_for_reading_invalid_path() {
        assert_eq!(
            PacketProcessor::new_for_reading(&Path::new("/some/invalid/file.txt"))
                .await
                .err()
                .unwrap()
                .kind(),
            io::ErrorKind::NotFound
        );
    }

    #[tokio::test]
    async fn test_new_for_reading_valid_path() {
        let tmpdir = TempDir::new("scratch").unwrap();
        let path = tmpdir.path().join("test.txt");
        let _ = File::create(path.clone()).await.unwrap();

        let processor = PacketProcessor::new_for_reading(&path).await;
        println!("{:#?}", processor);
        assert!(processor.is_ok());
    }

    #[tokio::test]
    async fn test_read_first_packet_succeeds() {
        let tmpdir = TempDir::new("scratch").unwrap();
        let path = tmpdir.path().join("test.txt");
        let mut file = File::create(path.clone()).await.unwrap();
        let contents = "testing".to_string();
        assert_eq!(
            file.write(contents.as_bytes()).await.unwrap(),
            contents.as_bytes().len()
        );

        let mut processor = PacketProcessor::new_for_reading(&path)
            .await
            .inspect(|p| println!("{:#?}", p))
            .unwrap();

        assert_eq!(
            processor.first_packet().await,
            ResultAction::SendPacketAndAwait(tftp::Packet::Data {
                block: 1,
                data: vec![0x74, 0x65, 0x73, 0x74, 0x69, 0x6E, 0x67]
            })
        );
    }

    #[tokio::test]
    async fn test_read_multiple_packets_succeeds() {
        let tmpdir = TempDir::new("scratch").unwrap();
        let path = tmpdir.path().join("test.txt");
        let mut file = File::create(path.clone()).await.unwrap();
        let mut contents = String::new();
        for _ in 0..1024 {
            contents.push('x');
        }
        contents.push_str("testing");
        assert_eq!(
            file.write(contents.as_bytes()).await.unwrap(),
            contents.as_bytes().len()
        );

        let mut processor = PacketProcessor::new_for_reading(&path)
            .await
            .inspect(|p| println!("{:#?}", p))
            .unwrap();

        assert_eq!(
            processor.first_packet().await,
            ResultAction::SendPacketAndAwait(tftp::Packet::Data {
                block: 1,
                data: vec![0x78; 512]
            })
        );

        assert_eq!(
            processor
                .process_packet(&tftp::Packet::Ack { block: 1 })
                .await,
            ResultAction::SendPacketAndAwait(tftp::Packet::Data {
                block: 2,
                data: vec![0x78; 512]
            })
        );

        assert_eq!(
            processor
                .process_packet(&tftp::Packet::Ack { block: 2 })
                .await,
            ResultAction::SendPacketAndAwait(tftp::Packet::Data {
                block: 3,
                data: vec![0x74, 0x65, 0x73, 0x74, 0x69, 0x6E, 0x67]
            })
        );

        assert_eq!(
            processor
                .process_packet(&tftp::Packet::Ack { block: 3 })
                .await,
            ResultAction::CloseConnection(None)
        );
    }

    #[tokio::test]
    async fn test_process_recv_error() {
        let tmpdir = TempDir::new("scratch").unwrap();
        let path = tmpdir.path().join("test.txt");
        let mut file = File::create(path.clone()).await.unwrap();
        let mut contents = String::new();
        for _ in 0..1024 {
            contents.push('x');
        }
        contents.push_str("testing");
        assert_eq!(
            file.write(contents.as_bytes()).await.unwrap(),
            contents.as_bytes().len()
        );

        let mut processor = PacketProcessor::new_for_reading(&path)
            .await
            .inspect(|p| println!("{:#?}", p))
            .unwrap();

        assert_eq!(
            processor.first_packet().await,
            ResultAction::SendPacketAndAwait(tftp::Packet::Data {
                block: 1,
                data: vec![0x78; 512]
            })
        );

        assert_eq!(
            processor
                .process_packet(&tftp::Packet::Error {
                    code: tftp::ErrorCode::Undefined,
                    message: "whoops".to_string()
                })
                .await,
            ResultAction::CloseConnection(Some("Client sent error packet: code: Undefined, message: 'whoops'".to_string()))
        );
    }


    #[tokio::test]
    async fn test_process_read_invalid_packet() {
        let tmpdir = TempDir::new("scratch").unwrap();
        let path = tmpdir.path().join("test.txt");
        let mut file = File::create(path.clone()).await.unwrap();
        let mut contents = String::new();
        for _ in 0..1024 {
            contents.push('x');
        }
        contents.push_str("testing");
        assert_eq!(
            file.write(contents.as_bytes()).await.unwrap(),
            contents.as_bytes().len()
        );

        let mut processor = PacketProcessor::new_for_reading(&path)
            .await
            .inspect(|p| println!("{:#?}", p))
            .unwrap();

        assert_eq!(
            processor.first_packet().await,
            ResultAction::SendPacketAndAwait(tftp::Packet::Data {
                block: 1,
                data: vec![0x78; 512]
            })
        );

        assert_eq!(
            processor
                .process_packet(&tftp::Packet::Data {
                    block: 1,
                    data: vec![0x01],
                })
                .await,
            ResultAction::TerminateWithPacket(
                tftp::Packet::Error {
                    code: tftp::ErrorCode::Illegal,
                    message: "Expected to receive an Ack packet, but got Data {
    block: 1,
    data: [
        1,
    ],
} instead".to_string(),
                }
            )
        );
    }

    #[tokio::test]
    async fn test_process_ack_too_large() {
        let tmpdir = TempDir::new("scratch").unwrap();
        let path = tmpdir.path().join("test.txt");
        let mut file = File::create(path.clone()).await.unwrap();
        let mut contents = String::new();
        for _ in 0..1024 {
            contents.push('x');
        }
        contents.push_str("testing");
        assert_eq!(
            file.write(contents.as_bytes()).await.unwrap(),
            contents.as_bytes().len()
        );

        let mut processor = PacketProcessor::new_for_reading(&path)
            .await
            .inspect(|p| println!("{:#?}", p))
            .unwrap();

        assert_eq!(
            processor.first_packet().await,
            ResultAction::SendPacketAndAwait(tftp::Packet::Data {
                block: 1,
                data: vec![0x78; 512]
            })
        );

        assert_eq!(
            processor
                .process_packet(&tftp::Packet::Ack { block: 2 })
                .await,
            ResultAction::TerminateWithPacket(tftp::Packet::Error {
                code: tftp::ErrorCode::Illegal,
                message: "Cannot acknowledge a block which was not yet sent. Server's current block is 1, but received an ack for 2".to_string(),
            })
        );
    }


    #[tokio::test]
    async fn test_new_for_writing_invalid_path() {
        assert_eq!(
            PacketProcessor::new_for_writing(&Path::new("/some/invalid/path.txt"))
                .await
                .err()
                .unwrap()
                .kind(),
            io::ErrorKind::NotFound
        );
    }

    #[tokio::test]
    async fn test_new_for_writing_valid_path() {
        let tmpdir = TempDir::new("scratch").unwrap();
        let path = tmpdir.path().join("test.txt");

        let processor = PacketProcessor::new_for_writing(&path).await;
        println!("{:#?}", processor);
        assert!(processor.is_ok());
    }

    #[tokio::test]
    async fn test_write_first_packet_succeeds() {
        let tmpdir = TempDir::new("scratch").unwrap();
        let path = tmpdir.path().join("test.txt");

        let mut processor = PacketProcessor::new_for_writing(&path)
            .await
            .inspect(|p| println!("{:#?}", p))
            .unwrap();

        assert_eq!(
            processor.first_packet().await,
            ResultAction::SendPacketAndAwait(tftp::Packet::Ack { block: 0 })
        );
    }

    #[tokio::test]
    async fn test_write_multiple_packets_succeeds() {
        let tmpdir = TempDir::new("scratch").unwrap();
        let path = tmpdir.path().join("test.txt");

        let mut processor = PacketProcessor::new_for_writing(&path)
            .await
            .inspect(|p| println!("{:#?}", p))
            .unwrap();

        assert_eq!(
            processor.first_packet().await,
            ResultAction::SendPacketAndAwait(tftp::Packet::Ack { block: 0 })
        );

        assert_eq!(
            processor
                .process_packet(&tftp::Packet::Data {
                    block: 1,
                    data: vec![0x78; 512],
                })
                .await,
            ResultAction::SendPacketAndAwait(tftp::Packet::Ack { block: 1 })
        );

        assert_eq!(
            processor
                .process_packet(&tftp::Packet::Data {
                    block: 2,
                    data: vec![0x74, 0x65, 0x73, 0x74, 0x69, 0x6E, 0x67]
                })
                .await,
            ResultAction::TerminateWithPacket(tftp::Packet::Ack { block: 2 })
        );
    }

    #[tokio::test]
    async fn test_process_write_recv_error() {
        let tmpdir = TempDir::new("scratch").unwrap();
        let path = tmpdir.path().join("test.txt");

        let mut processor = PacketProcessor::new_for_writing(&path)
            .await
            .inspect(|p| println!("{:#?}", p))
            .unwrap();

        let _ = processor.first_packet().await;

        assert_eq!(
            processor
                .process_packet(&tftp::Packet::Error {
                    code: tftp::ErrorCode::Undefined,
                    message: "whoops".to_string()
                })
                .await,
            ResultAction::CloseConnection(Some("Client sent error packet: code: Undefined, message: 'whoops'".to_string()))
        );
    }


    #[tokio::test]
    async fn test_process_write_invalid_packet() {
        let tmpdir = TempDir::new("scratch").unwrap();
        let path = tmpdir.path().join("test.txt");

        let mut processor = PacketProcessor::new_for_writing(&path)
            .await
            .inspect(|p| println!("{:#?}", p))
            .unwrap();

        let _ = processor.first_packet().await;

        assert_eq!(
            processor
                .process_packet(&tftp::Packet::Ack {
                    block: 1,
                })
                .await,
            ResultAction::TerminateWithPacket(
                tftp::Packet::Error {
                    code: tftp::ErrorCode::Illegal,
                    message: "Expected to receive a Data packet, but got Ack {
    block: 1,
} instead".to_string(),
                }
            )
        );
    }

    #[tokio::test]
    async fn test_process_data_too_large() {
        let tmpdir = TempDir::new("scratch").unwrap();
        let path = tmpdir.path().join("test.txt");

        let mut processor = PacketProcessor::new_for_writing(&path)
            .await
            .inspect(|p| println!("{:#?}", p))
            .unwrap();

        let _ = processor.first_packet().await;

        assert_eq!(
            processor
                .process_packet(&tftp::Packet::Data { block: 2, data: vec![0x01] })
                .await,
            ResultAction::TerminateWithPacket(tftp::Packet::Error {
                code: tftp::ErrorCode::Illegal,
                message: "Data blocks must be received by the server in sequence. Server received data \
                    for block 2, but the server has only received up to block 0.".to_string(),
            })
        );
    }
}
