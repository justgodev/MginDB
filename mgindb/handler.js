import WebSocket from 'ws'; // Make sure to install 'ws' package via npm

class MginDBClient {
    constructor(host = '127.0.0.1', port = 6446, username = '', password = '') {
        this.uri = `ws://${host}:${port}`;
        this.username = username;
        this.password = password;
        this.websocket = null;
    }

    async connect() {
        this.websocket = new WebSocket(this.uri);

        // Wait for connection to open
        await new Promise((resolve, reject) => {
            this.websocket.on('open', resolve);
            this.websocket.on('error', reject);
        });

        const authData = JSON.stringify({ username: this.username, password: this.password });
        this.websocket.send(authData);

        // Await the first message which should be a welcome message
        const response = await new Promise(resolve => this.websocket.on('message', resolve));
        if (response !== "MginDB server connected... Welcome!") {
            throw new Error("Failed to authenticate: " + response);
        }
    }

    async sendCommand(command) {
        if (!this.websocket || this.websocket.readyState !== WebSocket.OPEN) {
            await this.connect();
        }
        this.websocket.send(command);
        return new Promise(resolve => this.websocket.on('message', resolve));
    }

    async close() {
        if (this.websocket) {
            this.websocket.close();
        }
    }

    set(key, value) {
        return this.sendCommand(`SET ${key} ${value}`);
    }

    indices(action, key = null, value = null) {
        const command = `INDICES ${action}` + (key ? ` ${key}` : '') + (value ? ` ${value}` : '');
        return this.sendCommand(command);
    }

    incr(key, value) {
        return this.sendCommand(`INCR ${key} ${value}`);
    }

    decr(key, value) {
        return this.sendCommand(`DECR ${key} ${value}`);
    }

    delete(key) {
        return this.sendCommand(`DEL ${key}`);
    }

    query(key, queryString = null, options = null) {
        const command = `QUERY ${key}` + (queryString ? ` ${queryString}` : '') + (options ? ` ${options}` : '');
        return this.sendCommand(command);
    }

    count(key) {
        return this.sendCommand(`COUNT ${key}`);
    }

    schedule(action, cronOrKey = null, command = null) {
        const cmd = `SCHEDULE ${action}` + (cronOrKey ? ` ${cronOrKey}` : '') + (command ? ` ${command}` : '');
        return this.sendCommand(cmd);
    }

    sub(key) {
        return this.sendCommand(`SUB ${key}`);
    }

    unsub(key) {
        return this.sendCommand(`UNSUB ${key}`);
    }
}

export default MginDBClient;
