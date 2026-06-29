use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};

/// Abstract stream trait that fits UDS on Unix and Named Pipes on Windows.
pub trait IpcStream: AsyncRead + AsyncWrite + Unpin + Send + 'static {}

#[cfg(unix)]
impl IpcStream for tokio::net::UnixStream {}

#[cfg(windows)]
impl IpcStream for tokio::net::windows::named_pipe::NamedPipeClientStream {}
#[cfg(windows)]
impl IpcStream for tokio::net::windows::named_pipe::NamedPipeServerStream {}

/// Helper to read a packet from an IpcStream.
/// Format: [Version: 1 byte] [Command: 4 bytes] [Length: 4 bytes] [Payload: Length bytes]
pub async fn read_packet<S: IpcStream>(
    stream: &mut S,
) -> std::io::Result<(u8, u32, Vec<u8>)> {
    let mut header = [0u8; 9];
    stream.read_exact(&mut header).await?;

    let version = header[0];
    let command = u32::from_be_bytes([header[1], header[2], header[3], header[4]]);
    let length = u32::from_be_bytes([header[5], header[6], header[7], header[8]]) as usize;

    let mut payload = vec![0u8; length];
    stream.read_exact(&mut payload).await?;

    Ok((version, command, payload))
}

/// Helper to write a packet to an IpcStream.
pub async fn write_packet<S: IpcStream>(
    stream: &mut S,
    version: u8,
    command: u32,
    payload: &[u8],
) -> std::io::Result<()> {
    let mut header = [0u8; 9];
    header[0] = version;
    header[1..5].copy_from_slice(&command.to_be_bytes());
    header[5..9].copy_from_slice(&(payload.len() as u32).to_be_bytes());

    stream.write_all(&header).await?;
    stream.write_all(payload).await?;
    stream.flush().await?;

    Ok(())
}

/// A wrapper enum for a unified client connection.
pub enum ClientConnection {
    #[cfg(unix)]
    Unix(tokio::net::UnixStream),
    #[cfg(windows)]
    Windows(tokio::net::windows::named_pipe::NamedPipeClientStream),
}

impl AsyncRead for ClientConnection {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            #[cfg(unix)]
            ClientConnection::Unix(s) => Pin::new(s).poll_read(cx, buf),
            #[cfg(windows)]
            ClientConnection::Windows(s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for ClientConnection {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            #[cfg(unix)]
            ClientConnection::Unix(s) => Pin::new(s).poll_write(cx, buf),
            #[cfg(windows)]
            ClientConnection::Windows(s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            #[cfg(unix)]
            ClientConnection::Unix(s) => Pin::new(s).poll_flush(cx),
            #[cfg(windows)]
            ClientConnection::Windows(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            #[cfg(unix)]
            ClientConnection::Unix(s) => Pin::new(s).poll_shutdown(cx),
            #[cfg(windows)]
            ClientConnection::Windows(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}

impl IpcStream for ClientConnection {}

/// A wrapper enum for a unified server connection.
pub enum ServerConnection {
    #[cfg(unix)]
    Unix(tokio::net::UnixStream),
    #[cfg(windows)]
    Windows(tokio::net::windows::named_pipe::NamedPipeServerStream),
}

impl AsyncRead for ServerConnection {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            #[cfg(unix)]
            ServerConnection::Unix(s) => Pin::new(s).poll_read(cx, buf),
            #[cfg(windows)]
            ServerConnection::Windows(s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for ServerConnection {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            #[cfg(unix)]
            ServerConnection::Unix(s) => Pin::new(s).poll_write(cx, buf),
            #[cfg(windows)]
            ServerConnection::Windows(s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            #[cfg(unix)]
            ServerConnection::Unix(s) => Pin::new(s).poll_flush(cx),
            #[cfg(windows)]
            ServerConnection::Windows(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            #[cfg(unix)]
            ServerConnection::Unix(s) => Pin::new(s).poll_shutdown(cx),
            #[cfg(windows)]
            ServerConnection::Windows(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}

impl IpcStream for ServerConnection {}

/// Get the platform IPC address.
pub fn get_ipc_path() -> &'static str {
    if cfg!(windows) {
        r"\\.\pipe\torrentd"
    } else {
        "/tmp/torrentd.sock"
    }
}

/// Connect to the daemon.
pub async fn connect_daemon() -> std::io::Result<ClientConnection> {
    let path = get_ipc_path();
    #[cfg(unix)]
    {
        let stream = tokio::net::UnixStream::connect(path).await?;
        Ok(ClientConnection::Unix(stream))
    }
    #[cfg(windows)]
    {
        use tokio::net::windows::named_pipe::ClientOptions;
        let stream = ClientOptions::new().open(path)?;
        Ok(ClientConnection::Windows(stream))
    }
}
