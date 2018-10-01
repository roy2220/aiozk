# This project is unmaintained, move to the (go version)[https://github.com/let-z-go/zk]

# aiozk

AsyncIO client for ZooKeeper

## Requirements

- python >= 3.6.0

## Installation

```shell
python3 -m pip install -U --process-dependency-links git+git://github.com/roy2220/aiozk.git
```

## Usage examples

- listen session

  ```python
  import asyncio
  import aiozk

  async def listen_session():
      client = aiozk.Client(server_addresses=[("192.168.33.1", 2181)], loop=loop)
      session_listener = client.add_session_listener()
      await client.start()
      try:
          while True:
              state_change = await session_listener.get_state_change()
              session_state, session_event_type = state_change
              print(session_state, session_event_type)
      finally:
          if client.is_running():
              client.stop()
              await client.wait_for_stopped()

  loop = asyncio.get_event_loop()
  loop.run_until_complete(listen_session())
  loop.close()
  ```

- create node

  ```python
  import asyncio
  import aiozk

  async def create_node():
      client = aiozk.Client(server_addresses=[("192.168.33.1", 2181)], loop=loop)
      await client.start()
      try:
          result = await client.create(path="/foo", data=b"bar")
          print(result)
      finally:
          if client.is_running():
              client.stop()
              await client.wait_for_stopped()

  loop = asyncio.get_event_loop()
  loop.run_until_complete(create_node())
  loop.close()
  ```

- delete node

  ```python
  import asyncio
  import aiozk

  async def delete_node():
      client = aiozk.Client(server_addresses=[("192.168.33.1", 2181)], loop=loop)
      await client.start()
      try:
          await client.delete(path="/foo")
      finally:
          if client.is_running():
              client.stop()
              await client.wait_for_stopped()

  loop = asyncio.get_event_loop()
  loop.run_until_complete(delete_node())
  loop.close()
  ```

- node exists

  ```python
  import asyncio
  import aiozk

  async def node_exists():
      client = aiozk.Client(server_addresses=[("192.168.33.1", 2181)], loop=loop)
      await client.start()
      try:
          result, watcher = await client.exists_w(path="/foo")
          print(result)
          watcher_event_type = await watcher.wait_for_event()
          print(watcher_event_type)
      finally:
          if client.is_running():
              client.stop()
              await client.wait_for_stopped()

  loop = asyncio.get_event_loop()
  loop.run_until_complete(node_exists())
  loop.close()
  ```

- set node data

  ```python
  import asyncio
  import aiozk

  async def set_node_data():
      client = aiozk.Client(server_addresses=[("192.168.33.1", 2181)], loop=loop)
      await client.start()
      try:
          result = await client.set_data(path="/foo", data=b"bar")
          print(result)
      finally:
          if client.is_running():
              client.stop()
              await client.wait_for_stopped()

  loop = asyncio.get_event_loop()
  loop.run_until_complete(set_node_data())
  loop.close()
  ```

- get node data

  ```python
  import asyncio
  import aiozk

  async def get_node_data():
      client = aiozk.Client(server_addresses=[("192.168.33.1", 2181)], loop=loop)
      await client.start()
      try:
          result, watcher = await client.get_data_w(path="/foo")
          print(result)
          watcher_event_type = await watcher.wait_for_event()
          print(watcher_event_type)
      finally:
          if client.is_running():
              client.stop()
              await client.wait_for_stopped()

  loop = asyncio.get_event_loop()
  loop.run_until_complete(get_node_data())
  loop.close()
  ```

- get node children

  ```python
  import asyncio
  import aiozk

  async def get_node_children():
      client = aiozk.Client(server_addresses=[("192.168.33.1", 2181)], loop=loop)
      await client.start()
      try:
          result, watcher = await client.get_children_w(path="/foo")
          print(result)
          watcher_event_type = await watcher.wait_for_event()
          print(watcher_event_type)
      finally:
          if client.is_running():
              client.stop()
              await client.wait_for_stopped()

  loop = asyncio.get_event_loop()
  loop.run_until_complete(get_node_children())
  loop.close()
  ```
