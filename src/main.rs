// This is an implementation of a TFTP server compliant with RFC 1350


// Transfer begins with request to read or write a file (also req for connection)
// If server grants request, connection is opened, file is sent in blocks of 512 bytes.
// Each data packet contains one block and must be acked before the next one is sent.
//
// On data loss, intended recipient times out and may retransmit last packet (data or ack),
// causing sender to retransmit the lost packet.
// Sender has to keep one packet for retransmission.
//
// Most errors cause connection to terminate. Err is signaled w/ error packet
// This packet is unacked and never retransmitted.
//
// Timeouts are used to detect the case where the error packet gets lost.
//
// Errors caused by 3 types of events:
// - unable to satisfy req (e.g. file not found, access violation)
// - invalid packet
// - loss of access to dependency (e.g. hard disk)
//
// Only one error condition that does not cause termination: when
// source port of a received packet is incorrect.
//
// Protocol is implemented on top of UDP.
//
// Packet:
//  - IP Header
//      - UDP Header
//          - TFTP header
//              - remainder of packet (contents depend on packet type)
//
// TFTP uses src and destination port fields in UDP header, and length field represents
// size of TFTP packet.
//
// Transfer identifiers (TIDs) used by TFTP are passed to Datagram layer and used as ports. Must be
// between 0 and 65,535
//
// TFTP header is a 2-byte opcode field.
//
// ------------------------------
// Initial connection protocol:
//
// transfer est by sending req and receiving a reply.
// For read: send RRQ, receive first data packet
// For write: send WRQ, recv ack
//
// Ack packet contains block number of the packet being acked. For write acks, block number is 0,
// but ordinarily starts w/ 1 and increases.
//
// To create a conn, each end chooses a TID for itself randomly.
//
// Every packet is associated w/ src and dst TID. These become UDP ports.
//
// Requesting host chooses src TID and sends initial request to TID 69 (decimal)
// Response uses a TID chosen by server as src and TID from requester as dst.
//
// Initial connection req can be duplicated by network, causing server to send 2 replies on
// different TIDs. When this happens, client can send error packet for the second response
// without disrupting its connection on the first response.
//
// -------------------------------
// TFTP Packets
//
// opcode   operation
// 1        Read req (RRQ)
// 2        Write req (WRQ)
// 3        Data (DATA)
// 4        ACK
// 5        ERROR
//
// RRQ/WRQ packet format:
// 0x01/0x02 | Filename | 0x00 | Mode | 0x00
//
// Filename is a sequence of netascii bytes. Mode field contains "netascii", "octet", or "mail" (in
// any combination of case).
//
// In netascii, host must translate data into its own format
// In octet mode, file transfers in data format of the machine from which file is transferred.
// If host receives octet file and then returns it, it must be identical to the original.
//
// DATA packet:
// 0x03 | Block $ (2 bytes) | Data (0-512 bytes)
//
// - If block is 0-511 blocks long, it signals end of transfer.
//
// Sending a DATA packet is an acknowledgement of the ACK received for the previous data packet.
//
// WRQ and DATA packets acked by ACK or ERROR packets.
// RRQ and ACK packets acked via DATA or ERROR packets.
//
// ACK packet format:
// 0x04 | Block # (2 bytes)
//
// ERROR packet format:
// 0x05 | ErrorCode (2 bytes) | ErrMsg | 0x00
//
// Error message is intended for human consumption and should be in netascii.
//
// ----------------------------
// Normal termination
//
// End of transfer marked by DATA packet <512 bytes long. Packet should be acknowledged w/ ACK
// packet. Whoever sends the final ACK is encouraged to wait for a while before terminating to
// retransmit final ACK if it gets lost. Know it was lost if recv final DATA packet again.
//
// ----------------------------
// Premature termination
// An ERROR packet is sent as a courtesy, not retransmitted or ACKed.
// Timeouts must also be used to detect errors.
//
// ----------------------------
// Error codes
//
// 0    Not defined, see msg
// 1    File not found
// 2    Access violation
// 3    Disk full / alloc exceeded
// 4    Illegal TFTP op
// 5    Unknown TID
// 6    File already exists
// 7    No such user
//
//
// Server options:
// - Disable writes
// - Acceptable port range
// - Bind IP address
// - Timeout
// - Filesystem prefix
// - Max connections
//

use std::net::Ipv4Addr;

pub mod tftp;
pub mod srv_conn;

use tftp::TftpSocket;
use srv_conn::ServerRequestHandler;

use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    println!("Hello world!");

    let socket = TftpSocket::bind((Ipv4Addr::UNSPECIFIED, 69).into());

    let packet= tftp::Packet::ReadReq {
        path: "/hello/world.txt".to_string(),
        mode: tftp::FileMode::NetAscii,
    };

    let handler = ServerRequestHandler::new(&packet, (Ipv4Addr::new(127, 0, 0, 1), 12345).into());

    Ok(())
}

