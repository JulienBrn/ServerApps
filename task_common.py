import asyncio, json, io
import pandas as pd

async def send(writer: asyncio.StreamWriter, d):
    s = json.dumps(d, default=str) +"\n"
    b = s.encode()
    writer.write((str(len(b)) + "\n").encode())
    writer.write(b)
    await writer.drain()

async def receive(reader: asyncio.StreamReader):
    r = await reader.readline()
    if not r:
        return None
    size = int(r.decode())
    b = await reader.readexactly(size)
    s = b.decode()
    return json.loads(s)

async def send_df(writer: asyncio.StreamWriter, df: pd.DataFrame):
    s = df.to_json(orient="table", default_handler=str) + "\n"
    b = s.encode()
    writer.write((str(len(b)) + "\n").encode())
    writer.write(b)
    await writer.drain()

async def receive_df(reader: asyncio.StreamReader):
    r = await reader.readline()
    if not r:
        return None
    size = int(r.decode())
    b = await reader.readexactly(size)
    s = b.decode()
    return pd.read_json(s, orient="table")