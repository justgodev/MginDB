import Foundation
import Starscream
import SwiftyJSON

class MginDBClient: WebSocketDelegate {
    private var socket: WebSocket!
    private var username: String
    private var password: String
    private var responseHandler: ((String) -> Void)?
    
    init(host: String, port: Int, username: String, password: String) {
        self.username = username
        self.password = password
        var request = URLRequest(url: URL(string: "ws://\(host):\(port)")!)
        request.timeoutInterval = 5
        self.socket = WebSocket(request: request)
        self.socket.delegate = self
    }
    
    func connect() {
        socket.connect()
    }
    
    func disconnect() {
        socket.disconnect()
    }
    
    func sendCommand(_ command: String, completion: @escaping (String) -> Void) {
        responseHandler = completion
        if !socket.isConnected {
            connect()
        }
        socket.write(string: command)
    }
    
    func websocketDidConnect(socket: WebSocketClient) {
        let authData: [String: String] = ["username": username, "password": password]
        if let authJSON = try? JSON(authData).rawString() {
            socket.write(string: authJSON)
        }
    }
    
    func websocketDidDisconnect(socket: WebSocketClient, error: Error?) {
        print("Disconnected: \(error?.localizedDescription ?? "No error")")
    }
    
    func websocketDidReceiveMessage(socket: WebSocketClient, text: String) {
        responseHandler?(text)
        responseHandler = nil
    }
    
    func websocketDidReceiveData(socket: WebSocketClient, data: Data) {
        // No binary data expected
    }
    
    func set(key: String, value: String, completion: @escaping (String) -> Void) {
        sendCommand("SET \(key) \(value)", completion: completion)
    }
    
    func indices(action: String, key: String? = nil, value: String? = nil, completion: @escaping (String) -> Void) {
        sendCommand("INDICES \(action) \(key ?? "") \(value ?? "")".trimmingCharacters(in: .whitespaces), completion: completion)
    }
    
    func incr(key: String, value: String, completion: @escaping (String) -> Void) {
        sendCommand("INCR \(key) \(value)", completion: completion)
    }
    
    func decr(key: String, value: String, completion: @escaping (String) -> Void) {
        sendCommand("DECR \(key) \(value)", completion: completion)
    }
    
    func delete(key: String, completion: @escaping (String) -> Void) {
        sendCommand("DEL \(key)", completion: completion)
    }
    
    func query(key: String, queryString: String? = nil, options: String? = nil, completion: @escaping (String) -> Void) {
        sendCommand("QUERY \(key) \(queryString ?? "") \(options ?? "")".trimmingCharacters(in: .whitespaces), completion: completion)
    }
    
    func count(key: String, completion: @escaping (String) -> Void) {
        sendCommand("COUNT \(key)", completion: completion)
    }
    
    func schedule(action: String, cronOrKey: String? = nil, command: String? = nil, completion: @escaping (String) -> Void) {
        sendCommand("SCHEDULE \(action) \(cronOrKey ?? "") \(command ?? "")".trimmingCharacters(in: .whitespaces), completion: completion)
    }
    
    func sub(key: String, completion: @escaping (String) -> Void) {
        sendCommand("SUB \(key)", completion: completion)
    }
    
    func unsub(key: String, completion: @escaping (String) -> Void) {
        sendCommand("UNSUB \(key)", completion: completion)
    }
}

// Example usage
let client = MginDBClient(host: "127.0.0.1", port: 6446, username: "your_username", password: "your_password")
client.connect()

client.set(key: "myKey", value: "myValue") { response in
    print("Set Response: \(response)")
}

client.query(key: "myKey") { response in
    print("Query Response: \(response)")
}

client.disconnect()
