from .app_state import AppState
import json

class Subscriptions:
    def __init__(self, app_state):
        self.app_state = app_state

    def list_subscriptions(self):
        subscriptions = {key: list(subscribers) for key, subscribers in self.app_state.sub_pub.items()}
        return json.dumps(subscriptions)

    def subscribe(self, keys, sid):
        for key in keys:
            if key == "MONITOR":
                self.app_state.monitor_subscribers.add(sid)
            else:
                if key not in self.app_state.sub_pub:
                    self.app_state.sub_pub[key] = set()
                self.app_state.sub_pub[key].add(sid)
        self.app_state.sessions[sid]['subscribed_keys'].update(keys)
        return "OK"

    def unsubscribe(self, keys, sid):
        for key in keys:
            if key == "MONITOR":
                self.app_state.monitor_subscribers.discard(sid)
            elif key in self.app_state.sub_pub and sid in self.app_state.sub_pub[key]:
                self.app_state.sub_pub[key].remove(sid)
                if not self.app_state.sub_pub[key]:
                    del self.app_state.sub_pub[key]
        self.app_state.sessions[sid]['subscribed_keys'].difference_update(keys)
        return "OK"

class Notifier:
    def __init__(self, app_state):
        self.app_state = app_state

    async def notify_monitors(self, command_line, sid):
        if self.app_state.monitor_subscribers:
            message = json.dumps({
                "command": command_line,
                "sid": sid
            })
            for subscriber_sid in self.app_state.monitor_subscribers:
                await self.send_websocket_message(subscriber_sid, message)

    async def notify_subscribers(self, key, data):
        key_parts = key.split(':')
        wildcard_keys = [":".join(key_parts[:i]) + ':*' for i in range(1, len(key_parts) + 1)]
        deeper_wildcards = [":".join(key_parts[:i]) + ':*:*' for i in range(1, len(key_parts))]

        subscribers = set()
        for k in wildcard_keys + deeper_wildcards:
            if k in self.app_state.sub_pub:
                subscribers.update(self.app_state.sub_pub[k])

        if key in self.app_state.sub_pub:
            subscribers.update(self.app_state.sub_pub[key])

        message = json.dumps({
            "key": key,
            "data": data
        })

        for sid in subscribers:
            await self.send_websocket_message(sid, message)

    async def send_websocket_message(self, sid, message):
        session = self.app_state.sessions.get(sid, None)
        if session:
            websocket = session['websocket']
            try:
                if websocket.open:
                    await websocket.send(message)
                else:
                    print(f"WebSocket {sid} closed, cannot send message.")
            except Exception as e:
                print(f"Failed to send message to {sid}: {e}")

class SubPubManager:
    def __init__(self):
        self.app_state = AppState()
        self.subscriptions = Subscriptions(self.app_state)
        self.notifier = Notifier(self.app_state)

    def list_subscriptions(self, *args, **kwargs):
        return self.subscriptions.list_subscriptions()

    def subscribe_command(self, args, sid):
        keys = [key.strip() for key in args.split(',')]
        return self.subscriptions.subscribe(keys, sid)

    def unsubscribe_command(self, args, sid):
        keys = [key.strip() for key in args.split(',')]
        return self.subscriptions.unsubscribe(keys, sid)

    async def notify_monitors(self, command_line, sid):
        await self.notifier.notify_monitors(command_line, sid)

    async def notify_subscribers(self, key, data):
        await self.notifier.notify_subscribers(key, data)
