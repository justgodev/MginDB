using System;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using WebSocketSharp;

public class MginDBClient
{
    private readonly string _username;
    private readonly string _password;
    private readonly WebSocket _webSocket;

    public MginDBClient(string host = "127.0.0.1", int port = 6446, string username = "", string password = "")
    {
        _username = username;
        _password = password;
        _webSocket = new WebSocket($"ws://{host}:{port}");
        _webSocket.OnMessage += (sender, e) => OnMessageReceived(e.Data);
    }

    private TaskCompletionSource<string> _responseTcs;

    private void OnMessageReceived(string message)
    {
        _responseTcs?.SetResult(message);
    }

    public async Task ConnectAsync()
    {
        var tcs = new TaskCompletionSource<bool>();

        _webSocket.OnOpen += (sender, e) =>
        {
            var authData = new { username = _username, password = _password };
            _webSocket.Send(JsonConvert.SerializeObject(authData));
            tcs.SetResult(true);
        };

        _webSocket.OnMessage += (sender, e) =>
        {
            if (e.Data == "MginDB server connected... Welcome!")
            {
                tcs.SetResult(true);
            }
            else
            {
                tcs.SetException(new Exception("Failed to authenticate: " + e.Data));
            }
        };

        _webSocket.ConnectAsync();
        await tcs.Task;
    }

    private async Task<string> SendCommandAsync(string command)
    {
        if (!_webSocket.IsAlive)
        {
            await ConnectAsync();
        }

        _responseTcs = new TaskCompletionSource<string>();
        _webSocket.Send(command);
        return await _responseTcs.Task;
    }

    public async Task<string> Set(string key, string value)
    {
        return await SendCommandAsync($"SET {key} {value}");
    }

    public async Task<string> Indices(string action, string key = null, string value = null)
    {
        return await SendCommandAsync($"INDICES {action} {(key ?? "")} {(value ?? "")}".Trim());
    }

    public async Task<string> Incr(string key, string value)
    {
        return await SendCommandAsync($"INCR {key} {value}");
    }

    public async Task<string> Decr(string key, string value)
    {
        return await SendCommandAsync($"DECR {key} {value}");
    }

    public async Task<string> Delete(string key)
    {
        return await SendCommandAsync($"DEL {key}");
    }

    public async Task<string> Query(string key, string queryString = null, string options = null)
    {
        return await SendCommandAsync($"QUERY {key} {(queryString ?? "")} {(options ?? "")}".Trim());
    }

    public async Task<string> Count(string key)
    {
        return await SendCommandAsync($"COUNT {key}");
    }

    public async Task<string> Schedule(string action, string cronOrKey = null, string command = null)
    {
        return await SendCommandAsync($"SCHEDULE {action} {(cronOrKey ?? "")} {(command ?? "")}".Trim());
    }

    public async Task<string> Sub(string key)
    {
        return await SendCommandAsync($"SUB {key}");
    }

    public async Task<string> Unsub(string key)
    {
        return await SendCommandAsync($"UNSUB {key}");
    }

    public void Close()
    {
        _webSocket.Close();
    }
}
