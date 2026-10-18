//! PeerPool: one persistent connection carries many RPCs, and the pool
//! reconnects after the peer goes away and comes back on the same address.

use std::collections::HashMap;
use std::time::Duration;

use raft_kv_rs::rpc::Message;
use raft_kv_rs::transport::{Frame, PeerMap, PeerPool, PoolConfig};
use tokio::net::TcpListener;
use tokio::sync::mpsc;

/// Accept peer connections; forward (connection number, message) pairs.
fn serve(
    listener: TcpListener,
    tx: mpsc::UnboundedSender<(usize, Message)>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        // Connection tasks live in the JoinSet: aborting the server drops
        // them too, which closes every socket like a crashed process.
        let mut conns = tokio::task::JoinSet::new();
        let mut n = 0;
        while let Ok((mut s, _)) = listener.accept().await {
            n += 1;
            let tx = tx.clone();
            let conn = n;
            conns.spawn(async move {
                let _hello = Frame::read_bytes(&mut s).await.unwrap();
                while let Ok(m) = Frame::read(&mut s).await {
                    let _ = tx.send((conn, m));
                }
            });
        }
    })
}

fn msg(term: u64) -> Message {
    Message::TimeoutNow { term, leader_id: 1 }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn persistent_connection_and_reconnect() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, mut rx) = mpsc::unbounded_channel();
    let server = serve(listener, tx.clone());

    let cfg = PoolConfig {
        backoff_base: Duration::from_millis(5),
        backoff_max: Duration::from_millis(50),
        ..PoolConfig::default()
    };
    let pool = PeerPool::new(1, PeerMap::new(HashMap::from([(2, addr)])), cfg);

    for t in 0..10 {
        pool.send(2, &msg(t)).await.unwrap();
    }
    for t in 0..10 {
        let (conn, m) = rx.recv().await.unwrap();
        assert_eq!(conn, 1, "all RPCs must share the first connection");
        assert!(
            matches!(m, Message::TimeoutNow { term, .. } if term == t),
            "order kept"
        );
    }

    // Peer dies (listener and live connections gone).
    server.abort();
    let _ = server.await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Comes back on the same address.
    let listener = TcpListener::bind(addr).await.unwrap();
    let (tx2, mut rx2) = mpsc::unbounded_channel();
    let _server2 = serve(listener, tx2);

    // Sends may fail while the stale socket is detected and during backoff;
    // the pool must recover within a bounded time without being recreated.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    let mut delivered = false;
    let mut t = 100;
    while tokio::time::Instant::now() < deadline {
        let _ = pool.send(2, &msg(t)).await;
        t += 1;
        if let Ok(Some((_, m))) = tokio::time::timeout(Duration::from_millis(30), rx2.recv()).await
        {
            assert!(matches!(m, Message::TimeoutNow { term, .. } if term >= 100));
            delivered = true;
            break;
        }
    }
    assert!(delivered, "pool did not reconnect to the restarted peer");
    drop(tx);
}

#[tokio::test]
async fn unknown_peer_and_backoff_errors() {
    let pool = PeerPool::new(1, PeerMap::default(), PoolConfig::default());
    let e = pool.send(9, &msg(1)).await.unwrap_err();
    assert_eq!(e.kind(), std::io::ErrorKind::NotFound);

    // Nothing listens here: first send fails to connect, second is in backoff.
    let dead = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();
    let pool = PeerPool::new(
        1,
        PeerMap::new(HashMap::from([(2, dead)])),
        PoolConfig {
            backoff_base: Duration::from_secs(5),
            backoff_max: Duration::from_secs(5),
            ..PoolConfig::default()
        },
    );
    assert!(pool.send(2, &msg(1)).await.is_err());
    let e = pool.send(2, &msg(2)).await.unwrap_err();
    assert_eq!(e.kind(), std::io::ErrorKind::WouldBlock);
}
