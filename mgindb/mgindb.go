package main

import (
    "encoding/json"
    "fmt"
    "log"
    "net/url"
    "sync"
    "time"

    "github.com/gorilla/websocket"
)

type MginDBClient struct {
    uri        string
    username   string
    password   string
    connection *websocket.Conn
    mutex      sync.Mutex
}

type AuthData struct {
    Username string `json:"username"`
    Password string `json:"password"`
}

func NewMginDBClient(protocol, host string, port int, username, password string) *MginDBClient {
    uri := fmt.Sprintf("%s://%s:%d", protocol, host, port)
    return &MginDBClient{uri: uri, username: username, password: password}
}

func (client *MginDBClient) Connect() error {
    u, err := url.Parse(client.uri)
    if err != nil {
        return err
    }

    client.mutex.Lock()
    defer client.mutex.Unlock()

    c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
    if err != nil {
        return err
    }
    client.connection = c

    authData := AuthData{Username: client.username, Password: client.password}
    authDataJson, err := ujson.Marshal(authData)
    if err != nil {
        return err
    }

    err = client.connection.WriteMessage(websocket.TextMessage, authDataJson)
    if err != nil {
        return err
    }

    _, message, err := client.connection.ReadMessage()
    if err != nil {
        return err
    }

    if string(message) != "MginDB server connected... Welcome!" {
        return fmt.Errorf("failed to authenticate: %s", message)
    }

    return nil
}

func (client *MginDBClient) sendCommand(command string) (string, error) {
    client.mutex.Lock()
    defer client.mutex.Unlock()

    if client.connection == nil {
        if err := client.Connect(); err != nil {
            return "", err
        }
    }

    err := client.connection.WriteMessage(websocket.TextMessage, []byte(command))
    if err != nil {
        return "", err
    }

    _, message, err := client.connection.ReadMessage()
    if err != nil {
        return "", err
    }

    return string(message), nil
}

func (client *MginDBClient) Set(key, value string) (string, error) {
    return client.sendCommand(fmt.Sprintf("SET %s %s", key, value))
}

func (client *MginDBClient) Indices(action, key, value string) (string, error) {
    return client.sendCommand(fmt.Sprintf("INDICES %s %s %s", action, key, value))
}

func (client *MginDBClient) Incr(key, value string) (string, error) {
    return client.sendCommand(fmt.Sprintf("INCR %s %s", key, value))
}

func (client *MginDBClient) Decr(key, value string) (string, error) {
    return client.sendCommand(fmt.Sprintf("DECR %s %s", key, value))
}

func (client *MginDBClient) Delete(key string) (string, error) {
    return client.sendCommand(fmt.Sprintf("DEL %s", key))
}

func (client *MginDBClient) Query(key, queryString, options string) (string, error) {
    return client.sendCommand(fmt.Sprintf("QUERY %s %s %s", key, queryString, options))
}

func (client *MginDBClient) Count(key string) (string, error) {
    return client.sendCommand(fmt.Sprintf("COUNT %s", key))
}

func (client *MginDBClient) Schedule(action, cronOrKey, command string) (string, error) {
    return client.sendCommand(fmt.Sprintf("SCHEDULE %s %s %s", action, cronOrKey, command))
}

func (client *MginDBClient) Sub(key string) (string, error) {
    return client.sendCommand(fmt.Sprintf("SUB %s", key))
}

func (client *MginDBClient) Unsub(key string) (string, error) {
    return client.sendCommand(fmt.Sprintf("UNSUB %s", key))
}

func (client *MginDBClient) Close() error {
    client.mutex.Lock()
    defer client.mutex.Unlock()

    if client.connection != nil {
        err := client.connection.Close()
        client.connection = nil
        return err
    }
    return nil
}
