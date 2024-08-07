from .app_state import AppState  # Importing AppState class from app_state module
import ujson  # Module for JSON operations
import asyncio

class Subscriptions:
    def __init__(self, app_state):
        """Initialize Subscriptions with application state."""
        self.app_state = app_state

    def list_subscriptions(self):
        """
        List all current subscriptions.

        Returns:
            str: JSON representation of subscriptions.
        """
        subscriptions = {key: list(subscribers) for key, subscribers in self.app_state.sub_pub.items()}
        return ujson.dumps(subscriptions)

    def subscribe(self, keys, sid):
        """
        Subscribe a session to specified keys.

        Args:
            keys (list): List of keys to subscribe to.
            sid (str): Session ID.

        Returns:
            str: Result of the subscription process.
        """
        for key in keys:
            if key == "MONITOR":
                self.app_state.monitor_subscribers.add(sid)
            elif key == "NODE":
                self.app_state.node_subscribers.add(sid)
            elif key == "NODE_LITE":
                self.app_state.node_lite_subscribers.add(sid)
            else:
                if key not in self.app_state.sub_pub:
                    self.app_state.sub_pub[key] = set()
                self.app_state.sub_pub[key].add(sid)
        self.app_state.sessions[sid]['subscribed_keys'].update(keys)
        return "OK"

    def unsubscribe(self, keys, sid):
        """
        Unsubscribe a session from specified keys.

        Args:
            keys (list): List of keys to unsubscribe from.
            sid (str): Session ID.

        Returns:
            str: Result of the unsubscription process.
        """
        for key in keys:
            if key == "MONITOR":
                self.app_state.monitor_subscribers.discard(sid)
            elif key == "NODE":
                self.app_state.node_subscribers.discard(sid)
            elif key == "NODE_LITE":
                self.app_state.node_lite_subscribers.discard(sid)
            elif key in self.app_state.sub_pub and sid in self.app_state.sub_pub[key]:
                self.app_state.sub_pub[key].remove(sid)
                if not self.app_state.sub_pub[key]:
                    del self.app_state.sub_pub[key]
        self.app_state.sessions[sid]['subscribed_keys'].difference_update(keys)
        return "OK"

class Notifier:
    def __init__(self, app_state):
        """Initialize Notifier with application state."""
        self.app_state = app_state
        self.last_node_index = -1  # Track the last node that received a transaction

    async def notify_monitors(self, command_line, sid):
        """
        Notify all monitor subscribers of a command.

        Args:
            command_line (str): The command line to notify.
            sid (str): Session ID.
        """
        if self.app_state.monitor_subscribers:
            message = ujson.dumps({
                "command": command_line,
                "sid": sid
            })
            for subscriber_sid in self.app_state.monitor_subscribers:
                await self.send_websocket_message(subscriber_sid, message)

    async def notify_subscribers(self, key, data):
        """
        Notify all subscribers of a key with data.

        Args:
            key (str): The key to notify about.
            data (dict): The data to send.
        """
        key_parts = key.split(':')
        wildcard_keys = [":".join(key_parts[:i]) + ':*' for i in range(1, len(key_parts) + 1)]
        deeper_wildcards = [":".join(key_parts[:i]) + ':*:*' for i in range(1, len(key_parts))]

        subscribers = set()
        for k in wildcard_keys + deeper_wildcards:
            if k in self.app_state.sub_pub:
                subscribers.update(self.app_state.sub_pub[k])

        if key in self.app_state.sub_pub:
            subscribers.update(self.app_state.sub_pub[key])

        message = ujson.dumps({
            "key": key,
            "data": data
        })

        for sid in subscribers:
            await self.send_websocket_message(sid, message)

    async def notify_node(self, type, data, request_id=None, node_type='ALL', sid=None):
        """
        Notify node subscribers with the transaction data in a round-robin manner.

        Args:
            type (str): The type of data to send.
            data (dict): The data to send.
            request_id (str, optional): The request ID.
            node_type (str): The type of nodes to notify ('ALL', 'FULL', 'LITE').
            sid (str, optional): Session ID.
        """
        if isinstance(data, dict):
            data = ujson.dumps(data)

        message = ujson.dumps({
            "type": type,
            "data": data,
            "request_id": request_id,
            "sid": sid
        })

        if node_type == 'ALL':
            node_subscribers = list(self.app_state.node_subscribers.union(self.app_state.node_lite_subscribers))
        elif node_type == 'FULL':
            node_subscribers = list(self.app_state.node_subscribers)
        elif node_type == 'LITE':
            node_subscribers = list(self.app_state.node_lite_subscribers)
        else:
            return

        num_nodes = len(node_subscribers)
        if num_nodes > 0:
            self.last_node_index = (self.last_node_index + 1) % num_nodes
            next_node_sid = node_subscribers[self.last_node_index]
            await self.send_websocket_message(next_node_sid, message)

    async def notify_nodes(self, type, data, request_id=None, node_type='ALL', sid=None):
        """
        Notify all node subscribers with the transaction data.

        Args:
            type (str): The type of data to send.
            data (dict): The data to send.
            request_id (str, optional): The request ID.
            node_type (str): The type of nodes to notify ('ALL', 'FULL', 'LITE').
            sid (str, optional): Session ID.
        """
        message = ujson.dumps({
            "type": type,
            "data": data,
            "request_id": request_id,
            "sid": sid
        })

        if node_type == 'ALL':
            subscribers = self.app_state.node_subscribers.union(self.app_state.node_lite_subscribers)
        elif node_type == 'FULL':
            subscribers = self.app_state.node_subscribers
        elif node_type == 'LITE':
            subscribers = self.app_state.node_lite_subscribers
        else:
            return

        for sid in subscribers:
            await self.send_websocket_message(sid, message)

    async def send_websocket_message(self, sid, message):
        """
        Send a message to a specific websocket session.

        Args:
            sid (str): Session ID.
            message (str): The message to send.
        """
        session = self.app_state.sessions.get(sid, None)
        if session:
            websocket = session['websocket']
            try:
                if websocket.open:
                    asyncio.create_task(websocket.send(message))
                else:
                    print(f"WebSocket {sid} closed, cannot send message.")
            except Exception as e:
                print(f"Failed to send message to {sid}: {e}")

class SubPubManager:
    def __init__(self):
        """Initialize SubPubManager with application state, subscriptions, and notifier."""
        self.app_state = AppState()
        self.subscriptions = Subscriptions(self.app_state)
        self.notifier = Notifier(self.app_state)

    def list_subscriptions(self, *args, **kwargs):
        """
        List all current subscriptions.

        Returns:
            str: JSON representation of subscriptions.
        """
        return self.subscriptions.list_subscriptions()

    def subscribe_command(self, args, sid):
        """
        Handle the SUBSCRIBE command.

        Args:
            args (str): The keys to subscribe to, separated by commas.
            sid (str): Session ID.

        Returns:
            str: Result of the subscription process.
        """
        keys = [key.strip() for key in args.split(',')]
        return self.subscriptions.subscribe(keys, sid)

    def unsubscribe_command(self, args, sid):
        """
        Handle the UNSUBSCRIBE command.

        Args:
            args (str): The keys to unsubscribe from, separated by commas.
            sid (str): Session ID.

        Returns:
            str: Result of the unsubscription process.
        """
        keys = [key.strip() for key in args.split(',')]
        return self.subscriptions.unsubscribe(keys, sid)

    async def notify_monitors(self, command_line, sid):
        """
        Notify all monitor subscribers of a command.

        Args:
            command_line (str): The command line to notify.
            sid (str): Session ID.
        """
        await self.notifier.notify_monitors(command_line, sid)

    async def notify_subscribers(self, key, data):
        """
        Notify all subscribers of a key with data.

        Args:
            key (str): The key to notify about.
            data (dict): The data to send.
        """
        await self.notifier.notify_subscribers(key, data)

    async def notify_node(self, type, data, request_id=None, node_type='ALL', sid=None):
        """
        Notify node subscribers with the transaction data.

        Args:
            type (str): The type of data to send.
            data (dict): The data to send.
            request_id (str, optional): The request ID.
            node_type (str): The type of nodes to notify ('ALL', 'FULL', 'LITE').
            sid (str, optional): Session ID.
        """
        await self.notifier.notify_node(type, data, request_id, node_type, sid)

    async def notify_nodes(self, type, data, request_id=None, node_type='ALL', sid=None):
        """
        Notify all node subscribers with the transaction data.

        Args:
            type (str): The type of data to send.
            data (dict): The data to send.
            request_id (str, optional): The request ID.
            node_type (str): The type of nodes to notify ('ALL', 'FULL', 'LITE').
            sid (str, optional): Session ID.
        """
        await self.notifier.notify_nodes(type, data, request_id, node_type, sid)