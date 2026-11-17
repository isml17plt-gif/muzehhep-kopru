import asyncio
import re
import socket
import sqlite3
from collections import OrderedDict
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import StreamingResponse, Response
from telethon import TelegramClient
from telethon.tl.types import PeerChannel

API_ID = 34080926
API_HASH = "9906c31817f36a3a607692373d3a4237"
KANAL_ID = 1781104221

PY_BLOCK_SIZE = 65536     # 64 KB (Ilk acilista sadece gerekli indeks parcasini aninda ceker)
MAX_CACHED_BLOCKS = 2000  # PC RAM'de ~128 MB onbellek

client = TelegramClient("oturum_dosyasi", API_ID, API_HASH)
kanal_entity = None
msg_cache = {}
block_cache: OrderedDict[tuple[int, int], bytes] = OrderedDict()
block_locks: dict[tuple[int, int], asyncio.Lock] = {}

def yerel_ip_bul():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"

async def get_cached_message(msg_id: int):
    message = msg_cache.get(msg_id)
    if message is None:
        message = await client.get_messages(kanal_entity, ids=msg_id)
        if not message or not message.file:
            raise HTTPException(status_code=404, detail="Kitap bulunamadi")
        msg_cache[msg_id] = message
    return message

async def fetch_block(message, msg_id: int, block_idx: int) -> bytes:
    key = (msg_id, block_idx)
    if key in block_cache:
        block_cache.move_to_end(key)
        return block_cache[key]

    lock = block_locks.setdefault(key, asyncio.Lock())
    async with lock:
        if key in block_cache:
            block_cache.move_to_end(key)
            return block_cache[key]

        file_size = message.file.size
        offset = block_idx * PY_BLOCK_SIZE
        if offset >= file_size:
            return b""

        expected_len = min(PY_BLOCK_SIZE, file_size - offset)
        buf = bytearray()
        async for chunk in client.iter_download(
            message.media, offset=offset, request_size=65536
        ):
            buf.extend(chunk)
            if len(buf) >= expected_len:
                break

        data = bytes(buf[:expected_len])
        block_cache[key] = data
        if len(block_cache) > MAX_CACHED_BLOCKS:
            block_cache.popitem(last=False)
        block_locks.pop(key, None)
        return data

@asynccontextmanager
async def lifespan(app: FastAPI):
    global kanal_entity
    await client.start()
    await client.get_dialogs()
    kanal_entity = await client.get_entity(PeerChannel(KANAL_ID))

    ip = yerel_ip_bul()
    print("\n" + "=" * 55)
    print("64 KB ANLIK ACILIS KOPRUSU HAZIR!")
    print(f"Bilgisayar Yerel IP Adresi : {ip}")
    print("=" * 55 + "\n")
    yield
    await client.disconnect()

app = FastAPI(lifespan=lifespan)

@app.get("/kitaplar")
def kitaplari_listele(q: str = ""):
    conn = sqlite3.connect("telegram_katalog.db")
    conn.row_factory = sqlite3.Row
    if q:
        rows = conn.execute(
            "SELECT * FROM kitaplar WHERE baslik LIKE ? OR aciklama LIKE ? LIMIT 100",
            (f"%{q}%", f"%{q}%")
        ).fetchall()
    else:
        rows = conn.execute("SELECT * FROM kitaplar LIMIT 100").fetchall()
    conn.close()
    return [dict(r) for r in rows]

@app.api_route("/stream/{msg_id}", methods=["GET", "HEAD"])
async def stream_pdf(msg_id: int, request: Request):
    message = await get_cached_message(msg_id)
    file_size = message.file.size

    if request.method == "HEAD":
        return Response(
            status_code=200,
            headers={
                "Content-Length": str(file_size),
                "Accept-Ranges": "bytes",
                "Content-Type": "application/pdf",
            },
        )

    range_header = request.headers.get("range")
    start, end = 0, file_size - 1
    status_code = 200

    if range_header:
        match = re.search(r"bytes=(\d+)-(\d*)", range_header)
        if match:
            start = int(match.group(1))
            if match.group(2):
                end = min(int(match.group(2)), file_size - 1)
            status_code = 206

    content_length = end - start + 1

    async def byte_generator():
        pos = start
        while pos <= end:
            block_idx = pos // PY_BLOCK_SIZE
            block_offset = pos % PY_BLOCK_SIZE
            block_data = await fetch_block(message, msg_id, block_idx)
            if not block_data:
                break
            take = min(len(block_data) - block_offset, end - pos + 1)
            if take <= 0:
                break
            yield block_data[block_offset : block_offset + take]
            pos += take

    headers = {
        "Accept-Ranges": "bytes",
        "Content-Length": str(content_length),
        "Content-Type": "application/pdf",
        "Content-Disposition": "inline",
    }
    if status_code == 206:
        headers["Content-Range"] = f"bytes {start}-{end}/{file_size}"

    return StreamingResponse(byte_generator(), status_code=status_code, headers=headers)