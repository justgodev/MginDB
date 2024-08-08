use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Error;
use serde::{Serialize, Deserialize};
use serde_json::json;
use std::error::Error as StdError;

#[derive(Serialize, Deserialize)]
struct AuthData {
    username: String,
    password: String,
}

struct MginDBClient {
    uri: String,
    username: String,
    password: String,
}

impl MginDBClient {
    fn new(protocol: &str, host: &str, port: u16, username: &str, password: &str) -> Self {
        let uri = format!("{}://{}:{}", protocol, host, port);
        Self {
            uri,
            username: username.to_string(),
            password: password.to_string(),
        }
    }

    async fn connect(&self) -> Result<mpsc::Receiver<String>, Box<dyn StdError>> {
        let (ws_stream, _) = connect_async(&self.uri).await?;
        let (write, read) = ws_stream.split();
        let (tx, rx) = mpsc::channel(32);

        let auth_data = AuthData {
            username: self.username.clone(),
            password: self.password.clone(),
        };

        let auth_message = json!(auth_data).to_string();
        let tx_write = write.send(Message::Text(auth_message)).await;

        tokio::spawn(async move {
            let mut read = read;
            while let Some(msg) = read.next().await {
                match msg {
                    Ok(Message::Text(text)) => {
                        if tx.send(text).await.is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        eprintln!("WebSocket error: {:?}", e);
                        break;
                    }
                    _ => {}
                }
            }
        });

        Ok(rx)
    }

    async fn send_command(&self, command: &str, rx: &mut mpsc::Receiver<String>) -> Result<String, Box<dyn StdError>> {
        let (ws_stream, _) = connect_async(&self.uri).await?;
        let (write, read) = ws_stream.split();

        let mut read = read;
        let send_message = write.send(Message::Text(command.to_string())).await;

        if let Some(response) = rx.recv().await {
            Ok(response)
        } else {
            Err("Failed to receive response".into())
        }
    }

    async fn set(&self, key: &str, value: &str, rx: &mut mpsc::Receiver<String>) -> Result<String, Box<dyn StdError>> {
        self.send_command(&format!("SET {} {}", key, value), rx).await
    }

    async fn indices(&self, action: &str, key: Option<&str>, value: Option<&str>, rx: &mut mpsc::Receiver<String>) -> Result<String, Box<dyn StdError>> {
        self.send_command(&format!("INDICES {} {} {}", action, key.unwrap_or(""), value.unwrap_or("")).trim(), rx).await
    }

    async fn incr(&self, key: &str, value: &str, rx: &mut mpsc::Receiver<String>) -> Result<String, Box<dyn StdError>> {
        self.send_command(&format!("INCR {} {}", key, value), rx).await
    }

    async fn decr(&self, key: &str, value: &str, rx: &mut mpsc::Receiver<String>) -> Result<String, Box<dyn StdError>> {
        self.send_command(&format!("DECR {} {}", key, value), rx).await
    }

    async fn delete(&self, key: &str, rx: &mut mpsc::Receiver<String>) -> Result<String, Box<dyn StdError>> {
        self.send_command(&format!("DEL {}", key), rx).await
    }

    async fn query(&self, key: &str, query_string: Option<&str>, options: Option<&str>, rx: &mut mpsc::Receiver<String>) -> Result<String, Box<dyn StdError>> {
        self.send_command(&format!("QUERY {} {} {}", key, query_string.unwrap_or(""), options.unwrap_or("")).trim(), rx).await
    }

    async fn count(&self, key: &str, rx: &mut mpsc::Receiver<String>) -> Result<String, Box<dyn StdError>> {
        self.send_command(&format!("COUNT {}", key), rx).await
    }

    async fn schedule(&self, action: &str, cron_or_key: Option<&str>, command: Option<&str>, rx: &mut mpsc::Receiver<String>) -> Result<String, Box<dyn StdError>> {
        self.send_command(&format!("SCHEDULE {} {} {}", action, cron_or_key.unwrap_or(""), command.unwrap_or("")).trim(), rx).await
    }

    async fn sub(&self, key: &str, rx: &mut mpsc::Receiver<String>) -> Result<String, Box<dyn StdError>> {
        self.send_command(&format!("SUB {}", key), rx).await
    }

    async fn unsub(&self, key: &str, rx: &mut mpsc::Receiver<String>) -> Result<String, Box<dyn StdError>> {
        self.send_command(&format!("UNSUB {}", key), rx).await
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn StdError>> {
    let client = MginDBClient::new("127.0.0.1", 6446, "your_username", "your_password");
    let mut rx = client.connect().await?;

    // Example usage
    let response = client.set("myKey", "myValue", &mut rx).await?;
    println!("Set Response: {}", response);

    let response = client.query("myKey", None, None, &mut rx).await?;
    println!("Query Response: {}", response);

    // Add more examples as needed...

    Ok(())
}
