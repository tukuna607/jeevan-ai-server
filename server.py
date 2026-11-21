
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import httpx
import random
import asyncio
import html
from datetime import datetime

app = FastAPI(title="Jeevan AI Server")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

rooms = {}
room_lock = asyncio.Lock()


@app.get("/")
async def home():
    return {
        "status": "online",
        "app": "Jeevan AI",
        "service": "Phone-to-Phone Voice Translation",
        "version": "1.0"
    }


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "server": "Jeevan AI",
        "configured": True
    }


@app.post("/create-room")
async def create_room():
    async with room_lock:
        for attempt in range(100):
            code = str(random.randint(100000, 999999))

            if code not in rooms:
                rooms[code] = {
                    "clients": [],
                    "created": datetime.utcnow().isoformat()
                }
                return {
                    "success": True,
                    "room_code": code,
                    "message": "Room created successfully"
                }

    return {
        "success": False,
        "message": "Could not create room"
    }


@app.get("/join-room/{code}")
async def check_room(code: str):
    async with room_lock:
        room = rooms.get(code)

        if room is None:
            return {
                "success": False,
                "message": "Room not found"
            }

        if len(room["clients"]) >= 2:
            return {
                "success": False,
                "message": "Room is full"
            }

        return {
            "success": True,
            "room_code": code,
            "message": "Room available"
        }


async def translate_text(text, source, target):
    url = "https://api.mymemory.translated.net/get"

    params = {
        "q": text,
        "langpair": source + "|" + target
    }

    async with httpx.AsyncClient(timeout=20) as client:
        response = await client.get(url, params=params)
        response.raise_for_status()

        data = response.json()

        if data.get("responseStatus") != 200:
            raise Exception("Translation service error")

        translated = data.get("responseData", {}).get(
            "translatedText", ""
        )

        if not translated:
            raise Exception("Empty translation")

        return html.unescape(translated)


async def send_to_room(code, message):
    room = rooms.get(code)

    if room is None:
        return

    disconnected = []

    for client in list(room["clients"]):
        try:
            await client.send_json(message)
        except Exception:
            disconnected.append(client)

    for client in disconnected:
        if client in room["clients"]:
            room["clients"].remove(client)


@app.websocket("/ws/{code}")
async def websocket_room(websocket: WebSocket, code: str):

    await websocket.accept()

    async with room_lock:
        room = rooms.get(code)

        if room is None:
            await websocket.send_json({
                "type": "error",
                "message": "Room not found"
            })
            await websocket.close()
            return

        if len(room["clients"]) >= 2:
            await websocket.send_json({
                "type": "error",
                "message": "Room is full"
            })
            await websocket.close()
            return

        room["clients"].append(websocket)
        user_number = len(room["clients"])

    await send_to_room(code, {
        "type": "connected",
        "user_number": user_number,
        "total_users": len(rooms[code]["clients"]),
        "message": "Connected to Jeevan AI room"
    })

    try:
        while True:

            data = await websocket.receive_json()

            message_type = data.get("type")

            if message_type == "ping":
                await websocket.send_json({
                    "type": "pong"
                })
                continue

            if message_type == "speech":

                text = str(data.get("text", "")).strip()
                source = str(data.get("source", "hi")).strip()
                target = str(data.get("target", "te")).strip()

                if not text:
                    await websocket.send_json({
                        "type": "error",
                        "message": "Speech text is empty"
                    })
                    continue

                if len(text) > 3000:
                    await websocket.send_json({
                        "type": "error",
                        "message": "Text is too long"
                    })
                    continue

                if source not in ("hi", "te") or target not in ("hi", "te") or source == target:
                    await websocket.send_json({
                        "type": "error",
                        "message": "Unsupported language direction"
                    })
                    continue

                try:
                    translated = await translate_text(
                        text, source, target
                    )

                    await send_to_room(code, {
                        "type": "translation",
                        "original": text,
                        "translated": translated,
                        "source": source,
                        "target": target,
                        "sender": user_number
                    })

                except Exception:
                    await websocket.send_json({
                        "type": "error",
                        "message": "Translation failed. Try again."
                    })

            elif message_type == "end":
                await send_to_room(code, {
                    "type": "ended",
                    "message": "Conversation ended"
                })
                break

    except WebSocketDisconnect:
        pass

    except Exception:
        pass

    finally:

        async with room_lock:
            room = rooms.get(code)

            if room and websocket in room["clients"]:
                room["clients"].remove(websocket)

                if len(room["clients"]) == 0:
                    rooms.pop(code, None)

        await send_to_room(code, {
            "type": "user_left",
            "message": "A user disconnected"
        })


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000
        )
  
