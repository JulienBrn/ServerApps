import asyncio, json
from task_common import send, receive, receive_df
from pathlib import Path
import sys

async def get_steam_reader(pipe) -> asyncio.StreamReader:
    loop = asyncio.get_event_loop()
    reader = asyncio.StreamReader(loop=loop)
    protocol = asyncio.StreamReaderProtocol(reader)
    await loop.connect_read_pipe(lambda: protocol, pipe)
    return reader


async def read(writer: asyncio.StreamWriter, stin):
    while True:
        s = await stin.readline()
        writer.write(s)
        await writer.drain()

async def write(reader: asyncio.StreamReader):
    while True:
        s = await reader.readline()
        sys.stdout.buffer.write(s)
        sys.stdout.flush()

async def main():
    reader, writer = await asyncio.open_connection("localhost", 8888)
    stin = await get_steam_reader(sys.stdin)
    await asyncio.gather(read(writer, stin), write(reader))
    
asyncio.run(main())