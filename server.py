#!/usr/bin/env python3
"""
FNaF Survival — Multiplayer WebSocket Server
pip install websockets
python server.py
"""
import asyncio, json, math, random, time
import websockets
from websockets.server import serve

WORLD          = 3000
ROUND_DURATION = 120
LOBBY_DURATION = 30
MIN_PLAYERS    = 2
KILL_BONUS     = 30
TICK_RATE      = 1/30
COLA_COOLDOWN  = 30.0
AXE_PICKUP_DIST = 55   # distance for killer to grab axe from survivor

clients:     dict[str, dict] = {}
walls:       list[dict]      = []
game_phase   = "LOBBY"
round_timer  = LOBBY_DURATION
round_number = 0
next_pid     = 0

# ── World ──────────────────────────────────────────────────────────────────
def gen_walls():
    w = [
        {"x":0,        "y":0,        "w":WORLD,"h":50},
        {"x":0,        "y":WORLD-50, "w":WORLD,"h":50},
        {"x":0,        "y":0,        "w":50,   "h":WORLD},
        {"x":WORLD-50, "y":0,        "w":50,   "h":WORLD},
    ]
    for _ in range(40):
        w.append({"x":random.randint(200,2700),"y":random.randint(200,2700),
                  "w":random.randint(80,160),  "h":40})
    return w

def wall_hit(x, y, size):
    for w in walls:
        if x < w["x"]+w["w"] and x+size > w["x"] and y < w["y"]+w["h"] and y+size > w["y"]:
            return True
    return False

def safe_spot(size):
    for _ in range(600):
        sx, sy = random.randint(200,2700), random.randint(200,2700)
        if not wall_hit(sx, sy, size): return sx, sy
    return 500, 500

def dist(a, b):
    return math.sqrt((a["x"]-b["x"])**2 + (a["y"]-b["y"])**2)

def dist_xy(x1,y1,x2,y2):
    return math.sqrt((x1-x2)**2 + (y1-y2)**2)

# ── Factories ──────────────────────────────────────────────────────────────
def make_axe(x, y):
    return {"x":x,"y":y,"vx":0,"vy":0,
            "flying":False,"stuck":False,"stuckOnPid":None,"size":15}

def make_killer(pid, name):
    x, y = safe_spot(45)
    return {
        "pid":pid,"name":name,"role":"killer",
        "x":x,"y":y,"size":45,"speed":5.5,
        "hp":999,"alive":True,"invul":0,
        "hasAxe":True,"attackCooldown":0,
        "retrievingAxe":False,
        "axe":make_axe(x,y),
        "kills":0,
        "stamina":100,"maxStamina":100,
    }

def make_survivor(pid, name):
    x, y = safe_spot(30)
    return {
        "pid":pid,"name":name,"role":"survivor",
        "x":x,"y":y,"size":30,"speed":6,
        "hp":100,"alive":True,"invul":0,
        "coins":0,
        "colaBuff":0.0,
        "colaCooldown":0.0,
        "stamina":100,"maxStamina":100,"staminaRegen":1,
        "bleeding":0.0,
        "bleedTimer":0.0,
    }

# ── Broadcast ──────────────────────────────────────────────────────────────
async def broadcast(msg: dict):
    if not clients: return
    data = json.dumps(msg)
    await asyncio.gather(*[
        c["ws"].send(data) for c in clients.values() if c["ws"].open
    ], return_exceptions=True)

async def send_to(pid, msg):
    c = clients.get(pid)
    if c and c["ws"].open:
        await c["ws"].send(json.dumps(msg))

def serialize(d: dict) -> dict:
    return {k:v for k,v in d.items() if k != "ws_ref"}

# ── Round management ───────────────────────────────────────────────────────
async def start_round():
    global walls, game_phase, round_timer, round_number
    round_number += 1
    walls       = gen_walls()
    game_phase  = "PLAYING"
    round_timer = ROUND_DURATION

    pids       = list(clients.keys())
    killer_pid = random.choice(pids)
    for pid, c in clients.items():
        name = c["data"]["name"]
        c["data"] = make_killer(pid,name) if pid==killer_pid else make_survivor(pid,name)

    await broadcast({
        "type":"round_start","round":round_number,
        "walls":walls,
        "players":{pid:serialize(c["data"]) for pid,c in clients.items()},
    })

async def end_round(winner: str):
    global game_phase, round_timer
    game_phase  = "INTERMISSION"
    round_timer = LOBBY_DURATION
    await broadcast({"type":"round_end","winner":winner,"next_in":LOBBY_DURATION})

# ── Main tick ──────────────────────────────────────────────────────────────
async def game_tick():
    global round_timer, game_phase
    last = time.time()
    while True:
        await asyncio.sleep(TICK_RATE)
        now = time.time()
        dt  = min(now-last, 0.1)
        last = now

        if game_phase == "PLAYING":
            round_timer -= dt
            if round_timer <= 0:
                await end_round("survivors"); continue

            # Always re-fetch — list updates each tick
            all_survivors = [c["data"] for c in clients.values()
                             if c["data"]["role"]=="survivor"]
            alive_survivors = [s for s in all_survivors if s["alive"]]
            killer_rec = next((c["data"] for c in clients.values()
                               if c["data"]["role"]=="killer"), None)

            # BUG FIX: check alive survivors EVERY tick
            if not alive_survivors:
                await end_round("killer"); continue

            # ── Stamina regen (only when not sprinting) ───────────────────
            for c in clients.values():
                p = c["data"]
                if not p.get("isSprinting", False):
                    p["stamina"] = min(
                        p.get("maxStamina",100),
                        p.get("stamina",100) + p.get("staminaRegen",1)*dt*8
                    )

            # ── Cola buff & cooldown ───────────────────────────────────────
            for s in all_survivors:
                if s["colaBuff"] > 0:
                    s["colaBuff"] -= dt
                    if s["colaBuff"] <= 0:
                        s["speed"]        = 6
                        s["staminaRegen"] = 1
                if s["colaCooldown"] > 0:
                    s["colaCooldown"] = max(0, s["colaCooldown"]-dt)

            # ── Bleeding ──────────────────────────────────────────────────
            for s in all_survivors:
                if s["bleeding"] > 0 and s["alive"]:
                    s["bleeding"]   = max(0, s["bleeding"]-dt)
                    s["bleedTimer"] += dt
                    if s["bleedTimer"] >= 1.0:
                        s["bleedTimer"] -= 1.0
                        s["hp"] = max(0, s["hp"]-3)
                        if s["hp"] <= 0 and s["alive"]:
                            s["alive"] = False
                            if killer_rec:
                                killer_rec["kills"] += 1
                                round_timer = min(round_timer+KILL_BONUS, 999)

            # ── Axe logic ──────────────────────────────────────────────────
            if killer_rec:
                axe = killer_rec["axe"]

                # Axe follows survivor it's stuck on
                if axe["stuck"] and axe["stuckOnPid"]:
                    vic_client = clients.get(axe["stuckOnPid"])
                    vic = vic_client["data"] if vic_client else None
                    if vic and vic["alive"]:
                        axe["x"] = vic["x"] + vic["size"]//2
                        axe["y"] = vic["y"]
                    else:
                        # Survivor died — axe drops at last position, now retrievable
                        axe["stuckOnPid"] = None

                # Flying axe
                if axe["flying"]:
                    axe["x"] += axe["vx"]
                    axe["y"] += axe["vy"]

                    hit_wall = wall_hit(axe["x"], axe["y"], axe["size"])
                    hit_pid  = None
                    for s in alive_survivors:
                        if s.get("invul",0) <= 0:
                            # Use center-to-center distance
                            ax_cx = axe["x"] + axe["size"]/2
                            ax_cy = axe["y"] + axe["size"]/2
                            sv_cx = s["x"] + s["size"]/2
                            sv_cy = s["y"] + s["size"]/2
                            if dist_xy(ax_cx,ax_cy,sv_cx,sv_cy) < axe["size"]+s["size"]/2:
                                hit_pid = s["pid"]
                                break

                    if hit_wall and not hit_pid:
                        axe["flying"]     = False
                        axe["stuck"]      = True
                        axe["stuckOnPid"] = None

                    elif hit_pid:
                        s = clients[hit_pid]["data"]
                        s["hp"]         -= 25
                        s["invul"]       = 60
                        s["bleeding"]    = 10.0
                        s["bleedTimer"]  = 0.0
                        axe["flying"]    = False
                        axe["stuck"]     = True
                        axe["stuckOnPid"]= hit_pid
                        if s["hp"] <= 0 and s["alive"]:
                            s["alive"] = False
                            # Axe drops when survivor dies
                            axe["stuckOnPid"] = None
                            killer_rec["kills"] += 1
                            round_timer = min(round_timer+KILL_BONUS, 999)

                # ── Killer retrieves axe ───────────────────────────────────
                if not killer_rec["hasAxe"] and axe["stuck"]:
                    if axe["stuckOnPid"]:
                        # Axe is on a survivor — killer must walk to survivor to grab it
                        vic_client = clients.get(axe["stuckOnPid"])
                        vic = vic_client["data"] if vic_client else None
                        if vic and vic["alive"]:
                            killer_rec["retrievingAxe"] = True
                            ang = math.atan2(vic["y"]-killer_rec["y"], vic["x"]-killer_rec["x"])
                            spd = killer_rec["speed"]
                            nx  = killer_rec["x"] + math.cos(ang)*spd
                            ny  = killer_rec["y"] + math.sin(ang)*spd
                            if not wall_hit(nx, killer_rec["y"], killer_rec["size"]):
                                killer_rec["x"] = nx
                            if not wall_hit(killer_rec["x"], ny, killer_rec["size"]):
                                killer_rec["y"] = ny
                            # Close enough to survivor → grab axe, remove bleed
                            if dist(killer_rec, vic) < killer_rec["size"] + vic["size"] + AXE_PICKUP_DIST:
                                killer_rec["hasAxe"]        = True
                                killer_rec["retrievingAxe"] = False
                                axe["stuck"]      = False
                                axe["stuckOnPid"] = None
                                vic["bleeding"]   = 0.0   # pulling axe out stops bleed
                        else:
                            # Survivor died, axe already dropped
                            axe["stuckOnPid"] = None
                    else:
                        # Axe stuck in wall — walk to it
                        killer_rec["retrievingAxe"] = True
                        ang = math.atan2(axe["y"]-killer_rec["y"], axe["x"]-killer_rec["x"])
                        spd = killer_rec["speed"]
                        nx  = killer_rec["x"] + math.cos(ang)*spd
                        ny  = killer_rec["y"] + math.sin(ang)*spd
                        if not wall_hit(nx, killer_rec["y"], killer_rec["size"]):
                            killer_rec["x"] = nx
                        if not wall_hit(killer_rec["x"], ny, killer_rec["size"]):
                            killer_rec["y"] = ny
                        if dist(killer_rec, axe) < 40:
                            killer_rec["hasAxe"]        = True
                            killer_rec["retrievingAxe"] = False
                            axe["stuck"] = False
                else:
                    if killer_rec["hasAxe"]:
                        killer_rec["retrievingAxe"] = False

                if killer_rec["attackCooldown"] > 0:
                    killer_rec["attackCooldown"] -= dt * 60

            # ── Broadcast state ────────────────────────────────────────────
            await broadcast({
                "type":"state",
                "timer":round_timer,
                "walls":walls,
                "players":{pid:serialize(c["data"]) for pid,c in clients.items()},
            })

        elif game_phase == "INTERMISSION":
            round_timer -= dt
            if round_timer <= 0:
                if len(clients) >= MIN_PLAYERS:
                    await start_round()
                else:
                    game_phase = "LOBBY"
                    await broadcast({"type":"lobby","count":len(clients)})

        elif game_phase == "LOBBY":
            if len(clients) >= MIN_PLAYERS:
                await start_round()

# ── Input ──────────────────────────────────────────────────────────────────
async def handle_input(pid: str, msg: dict):
    global round_timer
    if pid not in clients: return
    d     = clients[pid]["data"]
    mtype = msg.get("type")

    # Chat always works
    if mtype == "chat":
        text = str(msg.get("text",""))[:120].strip()
        if text:
            await broadcast({"type":"chat","pid":pid,"name":d.get("name","?"),"text":text})
        return

    # WebRTC relay always works
    if mtype in ("rtc_offer","rtc_answer","rtc_ice"):
        to_pid = str(msg.get("to"))
        if to_pid in clients:
            relay = dict(msg); relay["from"] = pid
            await send_to(to_pid, relay)
        return

    if mtype == "rtc_join_voice":
        await broadcast({"type":"rtc_join_voice","pid":pid}); return
    if mtype == "rtc_leave_voice":
        await broadcast({"type":"rtc_leave_voice","pid":pid}); return

    if not d.get("alive", True): return

    if mtype == "move":
        if d.get("retrievingAxe"):
            return   # server controls killer position while retrieving
        d["x"] = max(50, min(float(msg.get("x",d["x"])), 2950))
        d["y"] = max(50, min(float(msg.get("y",d["y"])), 2950))
        if d.get("invul",0) > 0:
            d["invul"] = max(0, d["invul"]-1)

    elif mtype == "stamina_use":
        amount = float(msg.get("amount",0))
        d["stamina"] = max(0, d.get("stamina",100)-amount)
        d["isSprinting"] = True

    elif mtype == "stamina_stop":
        d["isSprinting"] = False

    elif mtype == "throw_axe" and d["role"] == "killer":
        axe = d["axe"]
        # BUG FIX: can throw even if axe is stuck on survivor (pull-throw)
        if d["hasAxe"] and not axe["flying"]:
            ang = float(msg.get("angle",0))
            d["axe"] = make_axe(d["x"]+d["size"]//2, d["y"]+d["size"]//2)
            d["axe"]["vx"]     = math.cos(ang)*18
            d["axe"]["vy"]     = math.sin(ang)*18
            d["axe"]["flying"] = True
            d["hasAxe"] = False

    elif mtype == "melee" and d["role"] == "killer":
        if d["attackCooldown"] <= 0:
            dmg = 32 if d["hasAxe"] else 28
            hit_any = False
            for c2 in clients.values():
                s = c2["data"]
                if s["role"]=="survivor" and s["alive"] and s.get("invul",0)<=0:
                    if dist(d,s) < d["size"]+s["size"]+15:
                        s["hp"]   -= dmg
                        s["invul"] = 60
                        hit_any    = True
                        if s["hp"] <= 0 and s["alive"]:
                            s["alive"] = False
                            d["kills"] += 1
                            round_timer = min(round_timer+KILL_BONUS, 999)
            d["attackCooldown"] = 50

    elif mtype == "cola" and d["role"] == "survivor":
        if d.get("colaCooldown",0) <= 0:
            d["speed"]        = 9
            d["staminaRegen"] = 2
            d["colaBuff"]     = 5.0
            d["colaCooldown"] = COLA_COOLDOWN

# ── Handler ────────────────────────────────────────────────────────────────
async def handler(ws):
    global next_pid
    pid = str(next_pid); next_pid += 1
    try:
        raw  = await asyncio.wait_for(ws.recv(), timeout=30)
        join = json.loads(raw)
        name = join.get("name", f"Player{pid}")[:20]

        clients[pid] = {"ws":ws, "data":make_survivor(pid,name)}
        clients[pid]["data"]["name"] = name
        print(f"[+] {name} (pid={pid})  total={len(clients)}")

        await send_to(pid, {
            "type":"welcome","pid":pid,
            "walls":walls,"phase":game_phase,
            "players":{p:serialize(c["data"]) for p,c in clients.items()},
        })
        await broadcast({"type":"player_joined","pid":pid,"name":name,"count":len(clients)})

        async for raw in ws:
            try: await handle_input(pid, json.loads(raw))
            except json.JSONDecodeError: pass

    except (websockets.exceptions.ConnectionClosed, asyncio.TimeoutError):
        pass
    finally:
        name = clients.get(pid,{}).get("data",{}).get("name",pid)
        clients.pop(pid, None)
        print(f"[-] {name} disconnected  total={len(clients)}")
        await broadcast({"type":"player_left","pid":pid,"count":len(clients)})

# ── Entry ──────────────────────────────────────────────────────────────────
async def main():
    print("FNaF Survival Server — ws://0.0.0.0:8765")
    async with serve(handler, "0.0.0.0", 8765, origins=None):
        await game_tick()

if __name__ == "__main__":
    asyncio.run(main())