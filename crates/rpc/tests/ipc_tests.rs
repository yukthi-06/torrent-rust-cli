use torrent_rpc::{
    receive_request, receive_response, send_request, send_response,
    transport::{connect_daemon, get_ipc_path, ServerConnection},
    Request, Response,
};

#[tokio::test]
async fn test_request_response_ipc() {
    let path = get_ipc_path();

    #[cfg(unix)]
    {
        let _ = std::fs::remove_file(path);
        let listener = tokio::net::UnixListener::bind(path).expect("Failed to bind UDS");

        let server_task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("Accept failed");
            let mut conn = ServerConnection::Unix(stream);

            let req = receive_request(&mut conn)
                .await
                .expect("Server failed to receive request");
            assert!(matches!(req, Request::Version));

            send_response(
                &mut conn,
                &Response::Version {
                    version: "0.1.0".to_string(),
                },
            )
            .await
            .expect("Server failed to send response");
        });

        let mut client = connect_daemon().await.expect("Client failed to connect");
        send_request(&mut client, &Request::Version)
            .await
            .expect("Client failed to send request");

        let resp = receive_response(&mut client)
            .await
            .expect("Client failed to receive response");
        if let Response::Version { version } = resp {
            assert_eq!(version, "0.1.0");
        } else {
            panic!("Unexpected response type");
        }

        server_task.await.expect("Server task panicked");
        let _ = std::fs::remove_file(path);
    }

    #[cfg(windows)]
    {
        use tokio::net::windows::named_pipe::ServerOptions;

        let server_pipe = ServerOptions::new()
            .first_pipe_instance(true)
            .create(path)
            .expect("Failed to create named pipe");

        let server_task = tokio::spawn(async move {
            server_pipe
                .connect()
                .await
                .expect("Named pipe connect failed");
            let mut conn = ServerConnection::Windows(server_pipe);

            let req = receive_request(&mut conn)
                .await
                .expect("Server failed to receive request");
            assert!(matches!(req, Request::Version));

            send_response(
                &mut conn,
                &Response::Version {
                    version: "0.1.0".to_string(),
                },
            )
            .await
            .expect("Server failed to send response");
        });

        let mut client = connect_daemon().await.expect("Client failed to connect");
        send_request(&mut client, &Request::Version)
            .await
            .expect("Client failed to send request");

        let resp = receive_response(&mut client)
            .await
            .expect("Client failed to receive response");
        if let Response::Version { version } = resp {
            assert_eq!(version, "0.1.0");
        } else {
            panic!("Unexpected response type");
        }

        server_task.await.expect("Server task panicked");
    }
}
