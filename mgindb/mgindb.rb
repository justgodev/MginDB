require 'websocket-client-simple'
require 'json'

class MginDBClient
  def initialize(host = '127.0.0.1', port = 6446, username = '', password = '')
    @uri = "ws://#{host}:#{port}"
    @username = username
    @password = password
    @ws = nil
  end

  def connect
    @ws = WebSocket::Client::Simple.connect(@uri)

    @ws.on :open do
      auth_data = { username: @username, password: @password }.to_json
      @ws.send(auth_data)
    end

    @ws.on :message do |msg|
      @response = msg.data
    end

    @ws.on :close do |e|
      puts "Disconnected with error: #{e}"
    end

    @ws.on :error do |e|
      puts "Error: #{e}"
    end

    sleep 1
  end

  def send_command(command)
    @response = nil
    @ws.send(command)
    sleep 1
    @response
  end

  def set(key, value)
    send_command("SET #{key} #{value}")
  end

  def indices(action, key = nil, value = nil)
    send_command("INDICES #{action} #{key} #{value}".strip)
  end

  def incr(key, value)
    send_command("INCR #{key} #{value}")
  end

  def decr(key, value)
    send_command("DECR #{key} #{value}")
  end

  def delete(key)
    send_command("DEL #{key}")
  end

  def query(key, query_string = nil, options = nil)
    send_command("QUERY #{key} #{query_string} #{options}".strip)
  end

  def count(key)
    send_command("COUNT #{key}")
  end

  def schedule(action, cron_or_key = nil, command = nil)
    send_command("SCHEDULE #{action} #{cron_or_key} #{command}".strip)
  end

  def sub(key)
    send_command("SUB #{key}")
  end

  def unsub(key)
    send_command("UNSUB #{key}")
  end

  def close
    @ws.close
  end
end

# Example usage
client = MginDBClient.new('127.0.0.1', 6446, 'your_username', 'your_password')
client.connect

puts client.set('myKey', 'myValue')
puts client.query('myKey')

client.close
