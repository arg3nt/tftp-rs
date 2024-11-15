use async_io::Async;
use std::error;
use std::fmt;
use std::io;
use std::net::{UdpSocket, SocketAddr};
use std::time::Duration;
use tokio::time::timeout;
use tokio::time::error::Elapsed;

pub const READ_DATA_BUFFER_SIZE: usize = 512;
pub const WRITE_DATA_BUFFER_SIZE: usize = READ_DATA_BUFFER_SIZE - 2;

///////////////////////////////////////////////////////////////
// Error-handling objects

/// Represents an error returned from the TFTP Socket handler
#[derive(Debug)]
pub enum SocketError {
    IO(io::Error),
    PacketParse(String),
    Timeout(Elapsed),
}

impl error::Error for SocketError {}

impl fmt::Display for SocketError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SocketError::IO(e) => write!(f, "Socket IO error: {:#?}", e),
            SocketError::PacketParse(e) => write!(f, "Packet parsing error: {:#?}", e),
            SocketError::Timeout(e) => write!(f, "Socket IO timeout: {:#?}", e),
        }
    }
}

impl From<io::Error> for SocketError {
    fn from(e: io::Error) -> Self {
        SocketError::IO(e)
    }
}

impl From<Elapsed> for SocketError {
    fn from(e: Elapsed) -> Self {
        SocketError::Timeout(e)
    }
}


type TftpResult<T> = Result<T, SocketError>;

/// Represents the mode for a file the client wishes to read or write.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum FileMode { NetAscii, Octet, Mail }

impl FileMode {
    pub fn from_str(s: &String) -> TftpResult<FileMode> {
        match s.as_str() {
            "netascii" => Ok(FileMode::NetAscii),
            "octet" => Ok(FileMode::Octet),
            "mail" => Ok(FileMode::Mail),
            _ => Err(SocketError::PacketParse(format!("Invalid file mode: '{s}'")))
        }
    }

    pub fn to_string(&self) -> String {
        match self {
            Self::NetAscii => "netascii",
            Self::Octet => "octet",
            Self::Mail => "mail",
        }.to_string()
    }
}

/// Represents a TFTP Error code surfaced by a TFTP Error packet
#[derive(Debug, PartialEq)]
pub enum ErrorCode { Undefined, FileNotFound, AccessViolation, DiskFull, Illegal, UnknownTid, FileAlreadyExists, NoSuchUser }

impl ErrorCode {
    pub fn to_u16(&self) -> u16 {
        match self {
            Self::Undefined => 0,
            Self::FileNotFound => 1,
            Self::AccessViolation => 2,
            Self::DiskFull => 3,
            Self::Illegal => 4,
            Self::UnknownTid => 5,
            Self::FileAlreadyExists => 6,
            Self::NoSuchUser => 7,
        }
    }
}

impl From<u16> for ErrorCode {
    fn from(i: u16) -> ErrorCode {
        match i {
            0 => ErrorCode::Undefined,
            1 => ErrorCode::FileNotFound,
            2 => ErrorCode::AccessViolation,
            3 => ErrorCode::DiskFull,
            4 => ErrorCode::Illegal,
            5 => ErrorCode::UnknownTid,
            6 => ErrorCode::FileAlreadyExists,
            7 => ErrorCode::NoSuchUser,
            _ => ErrorCode::Undefined,
        }
    }
}

impl From<io::ErrorKind> for ErrorCode {
    fn from(c: io::ErrorKind) -> ErrorCode {
        match c {
            io::ErrorKind::NotFound => ErrorCode::FileNotFound,
            io::ErrorKind::PermissionDenied => ErrorCode::AccessViolation,
            io::ErrorKind::AlreadyExists => ErrorCode::FileAlreadyExists,
            io::ErrorKind::OutOfMemory => ErrorCode::DiskFull,
            _ => ErrorCode::Undefined,
        }
    }
}


#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ReqOptions {
    /// How many bytes should be transferred in each block. Must be between 8 and 65,464.
    pub block_size: Option<u16>,

    /// How many seconds to wait before retransmitting. Must be between 1 and 255.
    pub timeout: Option<u8>,

    /// How many bytes the total transfer size will be.
    pub tsize: Option<usize>,
}

impl ReqOptions {
    pub fn none() -> ReqOptions {
        ReqOptions {
            block_size: None,
            timeout: None,
            tsize: None,
        }
    }

    fn parse_from_buf(buf: &[u8]) -> ReqOptions {
        println!("Parsing from buffer: {}", String::from_utf8(buf.into()).unwrap());
        let mut cursor = 0;
        let mut opt = ReqOptions::none();

        loop {
            let (key, key_size) = string_from_buffer(&buf[cursor..]);
            if key_size == 0 || cursor + key_size >= buf.len() { return opt; }

            cursor += key_size + 1;
            let (raw_val, val_size) = string_from_buffer(&buf[cursor..]);
            if val_size == 0 || cursor + val_size >= buf.len() { return opt; }
            cursor += val_size + 1;

            match key.as_str() {
                "blksize" => {
                    if let Ok(val) = raw_val.parse::<u16>() {
                        opt.block_size = Some(val);
                    }
                },
                // "timeout" => {
                //     if let Ok(val) = raw_val.parse::<u8>() {
                //         opt.timeout = Some(val);
                //     }
                // },
                // "tsize" => {
                //     if let Ok(val) = raw_val.parse::<usize>() {
                //         opt.tsize = Some(val);
                //     }
                // },
                _ => { continue; },
            }

        }
    }

    fn add_options(&self, buf: &mut Vec<u8>) {
        if let Some(block_size) = self.block_size {
            buf.extend("blksize".to_string().as_bytes());
            buf.push(0x00);
            buf.extend(block_size.to_string().as_bytes());
            buf.push(0x00);
        }

        if let Some(timeout) = self.timeout {
            buf.extend("timeout".to_string().as_bytes());
            buf.push(0x00);
            buf.extend(timeout.to_string().as_bytes());
            buf.push(0x00);
        }

        if let Some(tsize) = self.tsize {
            buf.extend("tsize".to_string().as_bytes());
            buf.push(0x00);
            buf.extend(tsize.to_string().as_bytes());
            buf.push(0x00);
        }
    }
}

impl fmt::Display for ReqOptions {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(bs) = self.block_size {
            write!(f, "block_size: {} ", bs)?;
        }
        if let Some(to) = self.timeout {
            write!(f, "timeout: {}", to)?;
        }
        if let Some(ts) = self.tsize {
            write!(f, "tsize: {} ", ts)?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct InitData {
    /////////////
    // Mandatory

    /// The path of the file being worked with.
    pub path: String,

    /// The file mode.
    pub mode: FileMode,

    /// Options accompanying the request.
    pub opt: ReqOptions,
}

/// An enum representing a TFTP packet and its associated data.
#[derive(Debug, PartialEq)]
pub enum Packet {
    /// A read request packet
    ReadReq(InitData),

    /// A write request packet
    WriteReq(InitData),

    /// A data packet
    Data {
        /// The block number for this data packet.
        block: u16,

        /// The contents of the data itself.
        data: Vec<u8>,
    },

    /// An acknowledgment packet
    Ack {
        /// The block being acknowledged.
        block: u16,
    },

    /// An error packet.
    Error {
        /// The error code.
        code: ErrorCode,

        /// A human-readable message associated with the error.
        message: String,
    },

    /// An acknowledgement of options packet.
    OptionsAck(ReqOptions),
}

impl fmt::Display for Packet {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::ReadReq(d) => write!(
                f, "ReadReq < path: '{}', mode: '{:#?}', {} >",
                d.path, d.mode, d.opt
            ),
            Self::WriteReq(d) => write!(
                f, "WriteReq < path: '{}', mode: '{:#?}', {} >",
                d.path, d.mode, d.opt
            ),
            Self::Data { block, data } => write!(f, "Data < block: {block}, data_len: {len} >", len = data.len()),
            Self::Ack { block } => write!(f, "Ack < block: {block} >"),
            Self::Error { code, message } => write!(f, "Error < code: {:#?}, message: {message} >", code),
            Self::OptionsAck(opt) => write!(f, "OptionsAck < {} >", opt),
        }
    }
}

fn u16_from_buffer(buf: &[u8]) -> u16 {
    (u16::from(buf[0]) << 8) + u16::from(buf[1])
}

/// Given a buffer, assumes the string begins at the beginning of the buffer and concatenates until
/// it finds a 0 byte, which it assumes terminates the string.
///
/// Returns the string extracted from the buffer as well as the position of the 0 byte in the
/// buffer it was given (or the size of the buffer, if no 0 byte was encountered)
fn string_from_buffer(buf: &[u8]) -> (String, usize) {
    let mut s = String::new();
    for i in 0..buf.len() {
        let c = buf[i];
        if c == 0x00 {
            return (s, i)
        }
        s.push(char::from(c));
    }

    (s, buf.len())
}

/// Utility function for obtaining the TFTP OpCode from a buffer
#[derive(Debug, PartialEq)]
pub enum OpCode { Rrq, Wrq, Data, Ack, Error, OAck }

fn retrieve_op_code(buf: &[u8]) -> TftpResult<OpCode> {
    let rawcode = u16_from_buffer(&buf[..2]);
    match rawcode {
        1 => Ok(OpCode::Rrq),
        2 => Ok(OpCode::Wrq),
        3 => Ok(OpCode::Data),
        4 => Ok(OpCode::Ack),
        5 => Ok(OpCode::Error),
        6 => Ok(OpCode::OAck),
        _ => return Err(SocketError::PacketParse(
            format!("Uknown opcode retrieved: {rawcode}").to_string())),
    }
}

fn parse_init_data(buf: &[u8]) -> TftpResult<InitData> {
    let mut cursor = 0;
    let (path, path_size) = string_from_buffer(&buf);
    cursor += path_size;

    if cursor == buf.len() {
        return Err(SocketError::PacketParse("Read request does not contain a mode, but it needs to!".to_string()));
    }

    // Each string is terminated with a null byte.
    cursor += 1;

    let (raw_mode, mode_size) = string_from_buffer(&buf[cursor..]);
    cursor += mode_size;

    if cursor >= buf.len() {
        return Err(SocketError::PacketParse("Mode must be terminated with a null byte!".to_string()));
    }

    cursor += 1;

    let mode = match raw_mode.to_lowercase().as_str() {
        "netascii" => FileMode::NetAscii,
        "octet" => FileMode::Octet,
        "mail" => FileMode::Mail,
        _ => return Err(SocketError::PacketParse(format!("Unknown file mode: '{raw_mode}'")))
    };

    Ok(InitData { path, mode, opt: ReqOptions::parse_from_buf(&buf[cursor..]) })
}

fn parse_read_req(buf: &[u8]) -> TftpResult<Packet> {
    Ok(Packet::ReadReq(parse_init_data(&buf[2..])?))
}

fn parse_write_req(buf: &[u8]) -> TftpResult<Packet> {
    Ok(Packet::WriteReq(parse_init_data(&buf[2..])?))
}

fn parse_data(buf: &[u8]) -> TftpResult<Packet> {
    let block = u16_from_buffer(&buf[2..4]);
    Ok(Packet::Data { block, data: Vec::from(&buf[4..]) })
}

fn parse_ack(buf: &[u8]) -> TftpResult<Packet> {
    let block = u16_from_buffer(&buf[2..4]);
    Ok(Packet::Ack { block })
}

fn parse_error(buf: &[u8]) -> TftpResult<Packet> {
    let raw_err = u16_from_buffer(&buf[2..4]);

    let (message, _) = string_from_buffer(&buf[4..]);
    Ok(Packet::Error { code: raw_err.into(), message })
}

fn parse_oack(buf: &[u8]) -> TftpResult<Packet> {
    Ok(Packet::OptionsAck(ReqOptions::parse_from_buf(&buf[2..])))
}

impl Packet {
    fn parse_from_buf(buf: &[u8]) -> TftpResult<Packet> {
        if buf.len() < 4 {
            return Err(SocketError::PacketParse("Packet too short!".to_string()));
        }

        match retrieve_op_code(&buf[..2])? {
            OpCode::Rrq => parse_read_req(&buf),
            OpCode::Wrq => parse_write_req(&buf),
            OpCode::Data => parse_data(&buf),
            OpCode::Ack => parse_ack(&buf),
            OpCode::Error => parse_error(&buf),
            OpCode::OAck => parse_oack(&buf),
        }
    }

    fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        match self {
            Self::ReadReq(data) => {
                buf.extend(0x0001_u16.to_be_bytes());
                buf.extend(data.path.as_bytes());
                buf.push(0x00);
                buf.extend(data.mode.to_string().as_bytes());
                buf.push(0x00);
                data.opt.add_options(&mut buf);
            },
            Self::WriteReq(data) => {
                buf.extend(0x0002_u16.to_be_bytes());
                buf.extend(data.path.as_bytes());
                buf.push(0x00);
                buf.extend(data.mode.to_string().as_bytes());
                buf.push(0x00);
                data.opt.add_options(&mut buf);
            },
            Self::Data { block, data } => {
                buf.extend(0x0003_u16.to_be_bytes());
                buf.extend(block.to_be_bytes());
                buf.extend(data);
            },
            Self::Ack { block } => {
                buf.extend(0x0004_u16.to_be_bytes());
                buf.extend(block.to_be_bytes());
            },
            Self::Error { code, message } => {
                buf.extend(0x0005_u16.to_be_bytes());
                buf.extend(code.to_u16().to_be_bytes());
                buf.extend(message.as_bytes());
                buf.push(0x00);
            },
            Self::OptionsAck(opt) => {
                buf.extend(0x0006_u16.to_be_bytes());
                opt.add_options(&mut buf);
            },
        }
        buf
    }
}

///////////////////////////////////////////////////////////////
/// Wrapper around a UDP socket that parses TFTP headers and
/// returns the packets in a more structured format.
#[derive(Debug)]
pub struct TftpSocket {
    sock: Async<UdpSocket>,
}

impl TftpSocket {
    pub fn bind(addr: SocketAddr) -> TftpResult<TftpSocket> {
        log::info!("Binding to {:#?}", addr);
        Ok(TftpSocket {
            sock: Async::<UdpSocket>::bind(addr)?,
        })
    }

    pub async fn recv_with_timeout(&mut self, ttl: Duration) -> TftpResult<(Packet, SocketAddr)> {
        let mut buf = [0; 1 << 16];
        let (total_written, src) = timeout(ttl, self.sock.recv_from(&mut buf)).await??;

        let packet = Packet::parse_from_buf(&buf[..total_written])?;
        Ok((packet, src))
    }

    pub async fn send(&mut self, packet: &Packet, dst: SocketAddr) -> TftpResult<usize> {
        let buf = packet.serialize();
        match self.sock.send_to(&buf, dst).await {
            Ok(i) => Ok(i),
            Err(e) => Err(e.into()),
        }
    }
}

impl From<Async<UdpSocket>> for TftpSocket {
    fn from(s: Async<UdpSocket>) -> TftpSocket {
        TftpSocket { sock: s }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_packet_read_req() {
        let buf = vec![
            // opcode
            0x00, 0x01,
            // path: /path/to/data.txt with terminating nullchar
            0x2F, 0x70, 0x61, 0x74, 0x68, 0x2F, 0x74, 0x6F, 0x2F, 0x64, 0x61, 0x74, 0x61, 0x2E, 0x74, 0x78, 0x74, 0x00,
            // mode: mail
            0x6D, 0x61, 0x69, 0x6C, 0x00];

        let packet = Packet::parse_from_buf(&buf);
        assert!(packet.is_ok());
        assert_eq!(packet.unwrap(), Packet::ReadReq(InitData { path: "/path/to/data.txt".to_string(), mode: FileMode::Mail, opt: ReqOptions::none() }));
    }

    // TODO: add some opts
    fn test_packet_read_req_with_blksize() {
        let buf = vec![
            // opcode
            0x00, 0x01,
            // path: /path/to/data.txt with terminating nullchar
            0x2F, 0x70, 0x61, 0x74, 0x68, 0x2F, 0x74, 0x6F, 0x2F, 0x64, 0x61, 0x74, 0x61, 0x2E, 0x74, 0x78, 0x74, 0x00,
            // mode: mail
            0x6D, 0x61, 0x69, 0x6C, 0x00];

        let packet = Packet::parse_from_buf(&buf);
        assert!(packet.is_ok());
        assert_eq!(packet.unwrap(), Packet::ReadReq(InitData { path: "/path/to/data.txt".to_string(), mode: FileMode::Mail, opt: ReqOptions::none() }));
    }

    #[test]
    fn test_packet_write_req() {
        let buf = vec![
            // opcode
            0x00, 0x02,
            // path: /path/to/data.txt with terminating nullchar
            0x2F, 0x70, 0x61, 0x74, 0x68, 0x2F, 0x74, 0x6F, 0x2F, 0x64, 0x61, 0x74, 0x61, 0x2E, 0x74, 0x78, 0x74, 0x00,
            // mode: mail
            0x6D, 0x61, 0x69, 0x6C, 0x00];

        let packet = Packet::parse_from_buf(&buf);
        assert!(packet.is_ok());
        assert_eq!(packet.unwrap(), Packet::WriteReq(InitData { path: "/path/to/data.txt".to_string(), mode: FileMode::Mail, opt: ReqOptions::none() }));
    }


    #[test]
    fn test_packet_parses_data() {
        let buf = vec![
            // opcode
            0x00, 0x03,
            // block number
            0x12, 0x34,
            // data
            0xDE, 0xAD, 0xBE, 0xEF];

        let packet = Packet::parse_from_buf(&buf);
        assert!(packet.is_ok());
        assert_eq!(packet.unwrap(), Packet::Data { block: 0x1234, data: vec![0xDE, 0xAD, 0xBE, 0xEF] });
    }

    #[test]
    fn test_packet_parses_ack() {
        let buf = vec![0x00, 0x04, 0x10, 0x2f];
        let packet = Packet::parse_from_buf(&buf);
        assert!(packet.is_ok());
        assert_eq!(packet.unwrap(), Packet::Ack { block: 0x102f } );
    }

    #[test]
    fn test_packet_parses_error() {
        let buf = vec![
            // opcode
            0x00, 0x05,
            // Error code
            0x00, 0x04,
            // Error message: Illegal!
            0x49, 0x6C, 0x6C, 0x65, 0x67, 0x61, 0x6C, 0x21];

        let packet = Packet::parse_from_buf(&buf);
        assert!(packet.is_ok());
        assert_eq!(packet.unwrap(), Packet::Error { code: ErrorCode::Illegal, message: "Illegal!".to_string() });
    }

    #[test]
    fn test_packet_parse_failures() {
        // Invalid opcodes
        assert!(!Packet::parse_from_buf(&vec![0x10]).is_ok());
        assert!(!Packet::parse_from_buf(&vec![0x10, 0x00]).is_ok());
        assert!(!Packet::parse_from_buf(&vec![0x00, 0x09]).is_ok());
        // Invalid read path
        assert!(!Packet::parse_from_buf(&vec![0x00, 0x01, 0x68, 0x69]).is_ok());
        // Missing mode string
        assert!(!Packet::parse_from_buf(&vec![0x00, 0x01, 0x68, 0x69, 0x00]).is_ok());
        // Invalid mode string
        assert!(!Packet::parse_from_buf(&vec![0x00, 0x01, 0x68, 0x69, 0x00, 0x62, 0x61, 0x64, 0x00 ]).is_ok());
    }

    #[tokio::test]
    async fn test_udp() {
        let ip_addr = std::net::Ipv4Addr::new(127,0,0,1);
        let addr: SocketAddr = (ip_addr, 12345).into();

        let sock = Async::<UdpSocket>::bind(addr).unwrap();
        let buf = vec![0x00, 0x04, 0x12, 0x34];
        assert!(sock.send_to(&buf, addr).await.is_ok());

        let mut rcv_sock = TftpSocket::from(sock);

        assert_eq!(rcv_sock.recv_with_timeout(Duration::from_millis(1000)).await.unwrap(), (Packet::Ack{ block: 0x1234 }, addr));
    }
}

