<?php
require_once 'vendor/autoload.php';
use TextWebSocketClient;

class MginDBClient {
    private $uri;
    private $username;
    private $password;
    private $websocket;

    public function __construct($host = '127.0.0.1', $port = 6446, $username = '', $password = '') {
        $this->uri = "ws://$host:$port";
        $this->username = $username;
        $this->password = $password;
        $this->websocket = null;
    }

    public function connect() {
        $this->websocket = new TextWebSocketClient($this->uri);
        $this->websocket->connect();
        $authData = json_encode(['username' => $this->username, 'password' => $this->password]);
        $this->websocket->send($authData);
        $response = $this->websocket->receive();
        if ($response !== "MginDB server connected... Welcome!") {
            throw new Exception("Failed to authenticate: " . $response);
        }
    }

    public function sendCommand($command) {
        if ($this->websocket === null || !$this->websocket->isConnected()) {
            $this->connect();
        }
        $this->websocket->send($command);
        return $this->websocket->receive();
    }

    public function set($key, $value) {
        return $this->sendCommand("SET $key $value");
    }

    public function indices($action, $key = null, $value = null) {
        $command = "INDICES $action" . ($key ? " $key" : '') . ($value ? " $value" : '');
        return $this->sendCommand($command);
    }

    public function incr($key, $value) {
        return $this->sendCommand("INCR $key $value");
    }

    public function decr($key, $value) {
        return $this->sendCommand("DECR $key $value");
    }

    public function delete($key) {
        return $this->sendCommand("DEL $key");
    }

    public function query($key, $queryString = null, $options = null) {
        $command = "QUERY $key" . ($queryString ? " $queryString" : '') . ($options ? " $options" : '');
        return $this->sendCommand($command);
    }

    public function count($key) {
        return $this->sendCommand("COUNT $key");
    }

    public function schedule($action, $cronOrKey = null, $command = null) {
        $cmd = "SCHEDULE $action" . ($cronOrKey ? " $cronOrKey" : '') . ($command ? " $command" : '');
        return $this->sendCommand($cmd);
    }

    public function sub($key) {
        return $this->sendCommand("SUB $key");
    }

    public function unsub($key) {
        return $this->sendCommand("UNSUB $key");
    }

    public function close() {
        if ($this->websocket !== null && $this->websocket->isConnected()) {
            $this->websocket->close();
        }
    }
}
?>