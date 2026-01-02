A demo Android app showing how to handle flaky WebSocket connections.
Basically just a message queue with auto-reconnect. When your connection drops, messages stay queued and get sent when you're back online.
Has exponential backoff, heartbeat monitoring, and a simple CLI to test it all out.
Uses a fake WebSocket client that randomly fails so you can see the retry logic in action.
Built it to mess around with state machines and reliable messaging patterns.
