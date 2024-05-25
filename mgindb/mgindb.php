<?php
require_once 'vendor/autoload.php';
use TextWebSocketClient;

class MginDBClient {
    private $uri;  // WebSocket URI
    private $username;  // Username for authentication
    private $password;  // Password for authentication
    private $websocket;  // WebSocket client

    /**
     * Constructor to initialize the MginDBClient.
     *
     * @param string $host The host address of the MginDB server.
     * @param int $port The port number of the MginDB server.
     * @param string $username The username for authentication.
     * @param string $password The password for authentication.
     */
    public function __construct($host = '127.0.0.1', $port = 6446, $username = '', $password = '') {
        $this->uri = "ws://$host:$port";
        $this->username = $username;
        $this->password = $password;
        $this->websocket = null;
    }

    /**
     * Connects to the MginDB server and authenticates the user.
     *
     * @throws Exception If authentication fails.
     */
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

    /**
     * Sends a command to the MginDB server and returns the response.
     *
     * @param string $command The command to send.
     * @return string The server response.
     */
    public function sendCommand($command) {
        if ($this->websocket === null || !$this->websocket->isConnected()) {
            $this->connect();
        }
        $this->websocket->send($command);
        return $this->websocket->receive();
    }

    /**
     * Sets a value for a given key in the MginDB.
     *
     * @param string $key The key to set.
     * @param string $value The value to set.
     * @return string The server response.
     */
    public function set($key, $value) {
        return $this->sendCommand("SET $key $value");
    }

    /**
     * Manages indices in the MginDB.
     *
     * @param string $action The action to perform (e.g., LIST, ADD).
     * @param string|null $key The key for the action.
     * @param string|null $value The value for the action.
     * @return string The server response.
     */
    public function indices($action, $key = null, $value = null) {
        $command = "INDICES $action" . ($key ? " $key" : '') . ($value ? " $value" : '');
        return $this->sendCommand($command);
    }

    /**
     * Increments a key's value by a given amount in the MginDB.
     *
     * @param string $key The key to increment.
     * @param string $value The amount to increment by.
     * @return string The server response.
     */
    public function incr($key, $value) {
        return $this->sendCommand("INCR $key $value");
    }

    /**
     * Decrements a key's value by a given amount in the MginDB.
     *
     * @param string $key The key to decrement.
     * @param string $value The amount to decrement by.
     * @return string The server response.
     */
    public function decr($key, $value) {
        return $this->sendCommand("DECR $key $value");
    }

    /**
     * Deletes a key from the MginDB.
     *
     * @param string $key The key to delete.
     * @return string The server response.
     */
    public function delete($key) {
        return $this->sendCommand("DEL $key");
    }

    /**
     * Queries the MginDB with a given key and query string.
     *
     * @param string $key The key to query.
     * @param string|null $queryString The query string.
     * @param string|null $options Additional options for the query.
     * @return string The server response.
     */
    public function query($key, $queryString = null, $options = null) {
        $command = "QUERY $key" . ($queryString ? " $queryString" : '') . ($options ? " $options" : '');
        return $this->sendCommand($command);
    }

    /**
     * Counts the number of records for a given key in the MginDB.
     *
     * @param string $key The key to count.
     * @return string The server response.
     */
    public function count($key) {
        return $this->sendCommand("COUNT $key");
    }

    /**
     * Schedules a task in the MginDB.
     *
     * @param string $action The action to perform (e.g., ADD, REMOVE).
     * @param string|null $cronOrKey The cron expression or key for the action.
     * @param string|null $command The command to schedule.
     * @return string The server response.
     */
    public function schedule($action, $cronOrKey = null, $command = null) {
        $cmd = "SCHEDULE $action" . ($cronOrKey ? " $cronOrKey" : '') . ($command ? " $command" : '');
        return $this->sendCommand($cmd);
    }

    /**
     * Subscribes to a key in the MginDB.
     *
     * @param string $key The key to subscribe to.
     * @return string The server response.
     */
    public function sub($key) {
        return $this->sendCommand("SUB $key");
    }

    /**
     * Unsubscribes from a key in the MginDB.
     *
     * @param string $key The key to unsubscribe from.
     * @return string The server response.
     */
    public function unsub($key) {
        return $this->sendCommand("UNSUB $key");
    }

    /**
     * Closes the WebSocket connection to the MginDB server.
     */
    public function close() {
        if ($this->websocket !== null && $this->websocket->isConnected()) {
            $this->websocket->close();
        }
    }
}
?>