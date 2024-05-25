import WebSocket from 'ws'; // Make sure to install 'ws' package via npm

class MginDBClient {
    constructor(protocol = 'ws', host = '127.0.0.1', port = 6446, username = '', password = '') {
        this.uri = `${protocol}://${host}:${port}`; // WebSocket URI
        this.username = username; // Username for authentication
        this.password = password; // Password for authentication
        this.websocket = null; // WebSocket client
    }

    async connect() {
        /**
         * Connects to the MginDB server and authenticates the user.
         *
         * @throws Error If authentication fails.
         */
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
        /**
         * Sends a command to the MginDB server and returns the response.
         *
         * @param {string} command The command to send.
         * @returns {Promise<string>} The server response.
         */
        if (!this.websocket || this.websocket.readyState !== WebSocket.OPEN) {
            await this.connect();
        }
        this.websocket.send(command);
        return new Promise(resolve => this.websocket.on('message', resolve));
    }

    async close() {
        /**
         * Closes the WebSocket connection to the MginDB server.
         */
        if (this.websocket) {
            this.websocket.close();
        }
    }

    set(key, value) {
        /**
         * Sets a value for a given key in the MginDB.
         *
         * @param {string} key The key to set.
         * @param {string} value The value to set.
         * @returns {Promise<string>} The server response.
         */
        return this.sendCommand(`SET ${key} ${value}`);
    }

    indices(action, key = null, value = null) {
        /**
         * Manages indices in the MginDB.
         *
         * @param {string} action The action to perform (e.g., LIST, ADD).
         * @param {string|null} key The key for the action.
         * @param {string|null} value The value for the action.
         * @returns {Promise<string>} The server response.
         */
        const command = `INDICES ${action}` + (key ? ` ${key}` : '') + (value ? ` ${value}` : '');
        return this.sendCommand(command);
    }

    incr(key, value) {
        /**
         * Increments a key's value by a given amount in the MginDB.
         *
         * @param {string} key The key to increment.
         * @param {string} value The amount to increment by.
         * @returns {Promise<string>} The server response.
         */
        return this.sendCommand(`INCR ${key} ${value}`);
    }

    decr(key, value) {
        /**
         * Decrements a key's value by a given amount in the MginDB.
         *
         * @param {string} key The key to decrement.
         * @param {string} value The amount to decrement by.
         * @returns {Promise<string>} The server response.
         */
        return this.sendCommand(`DECR ${key} ${value}`);
    }

    delete(key) {
        /**
         * Deletes a key from the MginDB.
         *
         * @param {string} key The key to delete.
         * @returns {Promise<string>} The server response.
         */
        return this.sendCommand(`DEL ${key}`);
    }

    query(key, queryString = null, options = null) {
        /**
         * Queries the MginDB with a given key and query string.
         *
         * @param {string} key The key to query.
         * @param {string|null} queryString The query string.
         * @param {string|null} options Additional options for the query.
         * @returns {Promise<string>} The server response.
         */
        const command = `QUERY ${key}` + (queryString ? ` ${queryString}` : '') + (options ? ` ${options}` : '');
        return this.sendCommand(command);
    }

    count(key) {
        /**
         * Counts the number of records for a given key in the MginDB.
         *
         * @param {string} key The key to count.
         * @returns {Promise<string>} The server response.
         */
        return this.sendCommand(`COUNT ${key}`);
    }

    schedule(action, cronOrKey = null, command = null) {
        /**
         * Schedules a task in the MginDB.
         *
         * @param {string} action The action to perform (e.g., ADD, REMOVE).
         * @param {string|null} cronOrKey The cron expression or key for the action.
         * @param {string|null} command The command to schedule.
         * @returns {Promise<string>} The server response.
         */
        const cmd = `SCHEDULE ${action}` + (cronOrKey ? ` ${cronOrKey}` : '') + (command ? ` ${command}` : '');
        return this.sendCommand(cmd);
    }

    sub(key) {
        /**
         * Subscribes to a key in the MginDB.
         *
         * @param {string} key The key to subscribe to.
         * @returns {Promise<string>} The server response.
         */
        return this.sendCommand(`SUB ${key}`);
    }

    unsub(key) {
        /**
         * Unsubscribes from a key in the MginDB.
         *
         * @param {string} key The key to unsubscribe from.
         * @returns {Promise<string>} The server response.
         */
        return this.sendCommand(`UNSUB ${key}`);
    }
}

export default MginDBClient;