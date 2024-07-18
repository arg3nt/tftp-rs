use async_io::Async;
use std::error;
use std::fmt;
use std::io;
use std::net::{UdpSocket, SocketAddr};
use std::time::Duration;
use tokio::time::timeout;
use tokio::time::error::Elapsed;

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
#[derive(Debug, PartialEq)]
pub enum FileMode { NetAscii, Octet, Mail }

/// Represents a TFTP Error code surfaced by a TFTP Error packet
#[derive(Debug, PartialEq)]
pub enum ErrorCode { Undefined, FileNotFound, AccessViolation, DiskFull, Illegal, UnknownTid, FileAlreadyExists, NoSuchUser }

/// An enum representing a TFTP packet and its associated data.
#[derive(Debug, PartialEq)]
pub enum Packet {
    /// A read request packet
    ReadReq {
        /// The file path the client wants to read.
        path: String,

        /// The file mode.
        mode: FileMode,
    },

    /// A write request packet
    WriteReq {
        path: String,
        mode: FileMode,
    },

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
        code: ErrorCode,
        message: String,
    },
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
pub enum OpCode { Rrq, Wrq, Data, Ack, Error }

fn retrieve_op_code(buf: &[u8]) -> TftpResult<OpCode> {
    let rawcode = u16_from_buffer(&buf[..2]);
    match rawcode {
        1 => Ok(OpCode::Rrq),
        2 => Ok(OpCode::Wrq),
        3 => Ok(OpCode::Data),
        4 => Ok(OpCode::Ack),
        5 => Ok(OpCode::Error),
        _ => return Err(SocketError::PacketParse(
            format!("Uknown opcode retrieved: {rawcode}").to_string())),
    }
}


fn parse_path_and_mode(buf: &[u8]) -> TftpResult<(String, FileMode)> {
    let (path, path_end) = string_from_buffer(&buf);

    if path_end == buf.len() {
        return Err(SocketError::PacketParse("Read request does not contain a mode, but it needs to!".to_string()));
    }

    let (raw_mode, mode_end) = string_from_buffer(&buf[path_end+1..]);

    if path_end + mode_end >= buf.len() {
        return Err(SocketError::PacketParse("Mode must be terminated with a null byte!".to_string()));
    }

    let mode = match raw_mode.to_lowercase().as_str() {
        "netascii" => FileMode::NetAscii,
        "octet" => FileMode::Octet,
        "mail" => FileMode::Mail,
        _ => return Err(SocketError::PacketParse(format!("Unknown file mode: '{raw_mode}'")))
    };

    Ok((path, mode))
}

fn parse_read_req(buf: &[u8]) -> TftpResult<Packet> {
    let (path, mode) = parse_path_and_mode(&buf[2..])?;
    Ok(Packet::ReadReq { path, mode })
}

fn parse_write_req(buf: &[u8]) -> TftpResult<Packet> {
    let (path, mode) = parse_path_and_mode(&buf[2..])?;
    Ok(Packet::WriteReq { path, mode })
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
    let code = match raw_err {
        0 => ErrorCode::Undefined,
        1 => ErrorCode::FileNotFound,
        2 => ErrorCode::AccessViolation,
        3 => ErrorCode::DiskFull,
        4 => ErrorCode::Illegal,
        5 => ErrorCode::UnknownTid,
        6 => ErrorCode::FileAlreadyExists,
        7 => ErrorCode::NoSuchUser,
        _ => ErrorCode::Undefined,
    };

    let (message, _) = string_from_buffer(&buf[4..]);
    Ok(Packet::Error { code, message })
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
        }
    }
}

///////////////////////////////////////////////////////////////
/// Wrapper around a UDP socket that parses TFTP headers and
/// returns the packets in a more structured format.
pub struct TftpSocket {
    sock: Async<UdpSocket>,
}

impl TftpSocket {
    pub fn bind(addr: SocketAddr) -> TftpResult<TftpSocket> {
        Ok(TftpSocket {
            sock: Async::<UdpSocket>::bind(addr)?,
        })
    }

    pub async fn recv_with_timeout(&mut self, ttl: Duration) -> TftpResult<(Packet, SocketAddr)> {
        let mut buf = [0; 514];
        let (total_written, src) = timeout(ttl, self.sock.recv_from(&mut buf)).await??;

        let packet = Packet::parse_from_buf(&buf[..total_written])?;
        Ok((packet, src))
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
        assert_eq!(packet.unwrap(), Packet::ReadReq { path: "/path/to/data.txt".to_string(), mode: FileMode::Mail });
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
        assert_eq!(packet.unwrap(), Packet::WriteReq { path: "/path/to/data.txt".to_string(), mode: FileMode::Mail });
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

}

