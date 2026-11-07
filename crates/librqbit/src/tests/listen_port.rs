use std::{
    net::{Ipv4Addr, SocketAddr},
    num::NonZeroU16,
    sync::Arc,
    time::Duration,
};

use anyhow::Context;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::mpsc,
    time::timeout,
};

use crate::{
    AddTorrent, AddTorrentOptions, CreateTorrentOptions, ListenerMode, ListenerOptions, Session,
    SessionOptions, create_torrent, spawn_utils::BlockingSpawner,
};

use super::test_util::{create_default_random_dir_with_torrents, setup_test_logging, wait_until};

fn localhost(port: u16) -> SocketAddr {
    (Ipv4Addr::LOCALHOST, port).into()
}

/// A port that's free over both TCP and UDP.
fn free_port() -> u16 {
    loop {
        let tcp = std::net::TcpListener::bind(localhost(0)).unwrap();
        let port = tcp.local_addr().unwrap().port();
        if std::net::UdpSocket::bind(localhost(port)).is_ok() {
            return port;
        }
    }
}

fn nz(port: u16) -> NonZeroU16 {
    NonZeroU16::new(port).unwrap()
}

async fn make_session(
    dir: &std::path::Path,
    listen: Option<ListenerOptions>,
) -> anyhow::Result<Arc<Session>> {
    Session::new_with_opts(
        dir.into(),
        SessionOptions {
            dht: None,
            persistence: None,
            disable_local_service_discovery: true,
            listen,
            ..Default::default()
        },
    )
    .await
}

fn loopback_listener() -> ListenerOptions {
    ListenerOptions {
        mode: ListenerMode::TcpAndUtp,
        listen_addr: localhost(0),
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_set_listen_port_moves_listeners() -> anyhow::Result<()> {
    setup_test_logging();
    let dir = tempfile::TempDir::with_prefix("test_set_listen_port")?;
    let session = make_session(dir.path(), Some(loopback_listener())).await?;

    let old_port = session.listen_addr().context("should listen")?.port();
    // Loopback addresses aren't announced by default.
    assert_eq!(session.announce_port(), None);

    let new_port = free_port();
    session.set_listen_port(nz(new_port)).await?;

    assert_eq!(session.listen_addr().unwrap().port(), new_port);
    assert_eq!(session.announce_port(), Some(new_port));

    TcpStream::connect(localhost(new_port))
        .await
        .context("should accept TCP on the new port")?;
    assert!(
        std::net::UdpSocket::bind(localhost(new_port)).is_err(),
        "uTP should be bound to the new port"
    );

    // Cancelled tasks release the old port asynchronously.
    wait_until(
        || {
            std::net::TcpListener::bind(localhost(old_port)).context("TCP port not released")?;
            std::net::UdpSocket::bind(localhost(old_port)).context("UDP port not released")?;
            Ok(())
        },
        Duration::from_secs(5),
    )
    .await?;

    session.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_set_listen_port_failure_keeps_current_listeners() -> anyhow::Result<()> {
    setup_test_logging();
    let dir = tempfile::TempDir::with_prefix("test_set_listen_port_failure")?;
    let session = make_session(dir.path(), Some(loopback_listener())).await?;
    let port = session.listen_addr().context("should listen")?.port();

    let occupied = std::net::TcpListener::bind(localhost(0))?;
    let occupied_port = occupied.local_addr()?.port();

    assert!(session.set_listen_port(nz(occupied_port)).await.is_err());

    assert_eq!(session.listen_addr().unwrap().port(), port);
    assert_eq!(session.announce_port(), None);
    TcpStream::connect(localhost(port))
        .await
        .context("should still accept TCP on the current port")?;

    session.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_set_listen_port_when_not_listening() -> anyhow::Result<()> {
    setup_test_logging();
    let dir = tempfile::TempDir::with_prefix("test_set_listen_port_no_listener")?;
    let session = make_session(dir.path(), None).await?;

    assert!(session.set_listen_port(nz(free_port())).await.is_err());
    assert_eq!(session.listen_addr(), None);

    session.stop().await;
    Ok(())
}

/// Reads the announced port of every request sent to it.
async fn run_fake_http_tracker(listener: TcpListener, ports: mpsc::UnboundedSender<u16>) {
    loop {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 4096];
        let len = stream.read(&mut buf).await.unwrap();
        let request = String::from_utf8_lossy(&buf[..len]);
        let port = request
            .split(['?', '&', ' '])
            .find_map(|part| part.strip_prefix("port="))
            .and_then(|port| port.parse().ok())
            .expect("tracker request should have a port");
        let _ = ports.send(port);

        let body = "d8:intervali1800e5:peers0:e";
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        stream.write_all(response.as_bytes()).await.unwrap();
    }
}

async fn next_announced_port(rx: &mut mpsc::UnboundedReceiver<u16>) -> anyhow::Result<u16> {
    timeout(Duration::from_secs(10), rx.recv())
        .await
        .context("timeout waiting for the tracker to be called")?
        .context("tracker is dead")
}

#[tokio::test(flavor = "multi_thread")]
async fn test_set_listen_port_reannounces_to_trackers() -> anyhow::Result<()> {
    setup_test_logging();

    let tracker = TcpListener::bind(localhost(0)).await?;
    let tracker_url = format!("http://{}/announce", tracker.local_addr()?);
    let (ports_tx, mut ports_rx) = mpsc::unbounded_channel();
    let tracker_task = tokio::spawn(run_fake_http_tracker(tracker, ports_tx));

    let files = create_default_random_dir_with_torrents(1, 8192, Some("test_listen_port_reann"));
    let torrent = create_torrent(
        files.path(),
        CreateTorrentOptions {
            piece_length: Some(1024),
            ..Default::default()
        },
        &BlockingSpawner::new(1),
    )
    .await?;

    let first_port = free_port();
    let session = make_session(
        files.path(),
        Some(ListenerOptions {
            announce_port: Some(first_port),
            ..loopback_listener()
        }),
    )
    .await?;

    session
        .add_torrent(
            AddTorrent::from_bytes(torrent.as_bytes()?),
            Some(AddTorrentOptions {
                output_folder: Some(files.path().to_str().unwrap().to_owned()),
                overwrite: true,
                trackers: Some(vec![tracker_url]),
                ..Default::default()
            }),
        )
        .await?;

    assert_eq!(next_announced_port(&mut ports_rx).await?, first_port);

    let second_port = free_port();
    session.set_listen_port(nz(second_port)).await?;
    assert_eq!(next_announced_port(&mut ports_rx).await?, second_port);

    // The same port again: the VPN's public IP might have changed.
    session.set_listen_port(nz(second_port)).await?;
    assert_eq!(next_announced_port(&mut ports_rx).await?, second_port);

    tracker_task.abort();
    session.stop().await;
    Ok(())
}
