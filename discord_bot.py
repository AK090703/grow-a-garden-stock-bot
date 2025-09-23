import os, json, asyncio, hashlib, signal, sys, io, time, re
from typing import Dict, Tuple, Optional, List, Any

import discord
from discord import Embed, Intents, AllowedMentions, app_commands
from aiohttp import ClientSession, ClientConnectorError, WSMsgType, web
from aiohttp.client_exceptions import WSServerHandshakeError
from dotenv import load_dotenv

load_dotenv()
DISCORD_TOKEN   = os.getenv("DISCORD_TOKEN")
EXTERNAL_WS_URL = os.getenv("EXTERNAL_WS_URL")
WS_HEADERS_JSON   = os.getenv("WS_HEADERS_JSON", "")
WS_SUBSCRIBE_JSON = os.getenv("WS_SUBSCRIBE_JSON", "")
PING_EVERY = int(os.getenv("WS_PING_INTERVAL", "20"))

DEBUG_RAW = os.getenv("DEBUG_RAW", "0") == "1"
DEBUG_CHANNEL_ID = int(os.getenv("DEBUG_CHANNEL_ID", "0"))
_DEBUG_SENT_ONCE = False
DEBUG_CAPTURE_PAYLOAD = os.getenv("DEBUG_CAPTURE_PAYLOAD", "0") == "1"
_last_raw_payload: Optional[dict] = None

CATEGORY_CHANNELS = {
    "seeds":     int(os.getenv("CHANNEL_SEEDS", "0")),
    "pets":      int(os.getenv("CHANNEL_PETS", "0")),
    "cosmetics": int(os.getenv("CHANNEL_COSMETICS", "0")),
    "weathers":  int(os.getenv("CHANNEL_WEATHERS", "0")),
    "gears":     int(os.getenv("CHANNEL_GEARS", "0")),
    "merchant":  int(os.getenv("CHANNEL_MERCHANT", "0")),
}
ALIAS = {
    "egg": "pets", "eggs": "pets",
    "weather": "weathers", "weathers": "weathers",
    "cosmetic": "cosmetics", "cosmetics": "cosmetics",
    "seed": "seeds", "seeds": "seeds",
    "gear": "gears", "gears": "gears",
    "merchant": "merchant", "travelingmerchant": "merchant",
}
ROLE_MENTIONS = os.getenv("ROLE_MENTIONS", "1") == "1"
ROLE_PREFIX_SEEDS     = os.getenv("ROLE_PREFIX_SEEDS", "")
ROLE_PREFIX_PETS      = os.getenv("ROLE_PREFIX_PETS", "")
ROLE_PREFIX_GEARS     = os.getenv("ROLE_PREFIX_GEARS", "")
ROLE_PREFIX_MERCHANT  = os.getenv("ROLE_PREFIX_MERCHANT", "")
ROLE_PREFIX_WEATHERS  = os.getenv("ROLE_PREFIX_WEATHERS", "")
_ROLE_CACHE: Dict[int, Dict[str, discord.Role]] = {}

ADMIN_ABUSE_WEATHERS = {
    "SummerHarvest",
    "Mega Harvest",
    "SpaceTravel",
    "Disco",
    "DJJhai",
    "Blackhole",
    "JandelStorm",
    "DJSandstorm",
    "Volcano",
    "UnderTheSea",
    "AlienInvasion",
    "JandelLazer",
    "Obby",
    "PoolParty",
    "JandelZombie",
    "RadioactiveCarrot",
    "Armageddon",
    "ZenAura",
    "JandelFloat",
    "ChickenRain",
    "TK_RouteRunner",
    "TK_MoneyRain",
    "TK_LightningStorm",
    "CorruptZenAura",
    "JandelKatana",
    "MeteorStrike",
    "FlamingoFloat",
    "FlamingoLazer",
    "JunkbotRaid",
    "Boil",
    "Oil",
    "KitchenStorm",
    "Stoplight",
    "ChocolateRain",
    "Boombox Party",
    "Brainrot Stampede",
    "Brainrot Portal",
    "Dissonant",
    "Beanaura",
    "fairies",
    "Jandel UFO",
    "Jandel Waldo",
    "Pyramid Obby",
    "Bean Aura",
    "BoomboxParty",
    "JandelWaldo",
    "WaterYourGardens",
    "RainDance",
    "Rainbow",
    "AirHead",
    "BeeNado"
}
ADMIN_ABUSE_ROLE_NAME = "Admin Abuse"
SPECIAL_WEATHER_NAMES = {
    "SummerHarvest": "Summer Harvest",
    "AuroraBorealis": "Aurora Borealis",
    "TropicalRain": "Tropical Rain",
    "NightEvent": "Night",
    "SunGod": "Sun God",
    "MegaHarvest": "Mega Harvest",
    "BloodMoonEvent": "Blood Moon",
    "MeteorShower": "Meteor Shower",
    "SpaceTravel": "Space Travel",
    "DJJhai": "DJ Jhai",
    "JandelStorm": "Jandel Storm",
    "DJSandstorm": "DJ Sandstorm",
    "UnderTheSea": "Under The Sea",
    "AlienInvasion": "Alien Invasion",
    "JandelLazer": "Jandel Lazer",
    "PoolParty": "Pool Party",
    "JandelZombie": "Jandel Zombie",
    "RadioactiveCarrot": "Radioactive Carrot",
    "ZenAura": "Zen Aura",
    "CrystalBeams": "Crystal Beams",
    "JandelFloat": "Jandel Float",
    "ChickenRain": "Chicken Rain",
    "TK_RouteRunner": "Route Runner",
    "TK_MoneyRain": "Money Rain",
    "TK_LightningStorm": "Lightning Storm",
    "CorruptZenAura": "Corrupt Zen Aura",
    "JandelKatana": "Jandel Katana",
    "AcidRain": "Acid Rain",
    "MeteorStrike": "Meteor Strike",
    "FlamingoFloat": "Flamingo Float",
    "FlamingoLazer": "Flamingo Lazer",
    "JunkbotRaid": "Junkbot Raid",
    "KitchenStorm": "Kitchen Storm",
    "SolarEclipse": "Solar Eclipse",
    "ChocolateRain": "Chocolate Rain",
    "Beanaura": "Bean Aura",
    "fairies": "Fairies",
    "BoomboxParty": "Boombox Party",
    "JandelWaldo": "Jandel Waldo",
    "WaterYourGardens": "Water Your Gardens",
    "RainDance": "Rain Dance",
    "BeeNado": "Beenado"
}

def repair_weather_name(raw: str) -> str:
    if not raw:
        return "(unknown)"
    return SPECIAL_WEATHER_NAMES.get(raw, raw)

def _map_cat(s: Optional[str]) -> Optional[str]:
    if not s: return None
    s = s.lower().strip()
    return ALIAS.get(s, s)

def _color(cat: str) -> int:
    return {"seeds":0x2ecc71,"pets":0x3498db,"cosmetics":0x9b59b6,"weathers":0xf1c40f,"gears":0xe67e22,"merchant":0x1abc9c}.get(cat,0x95a5a6)

intents = Intents.default()
bot = discord.Client(intents=intents)
tree = app_commands.CommandTree(bot)
_last_batch_hash: Dict[str, str] = {}
_last_item_hash: Dict[Tuple[str, str], str] = {}
_last_weather_hash: Optional[str] = None
_last_presence: Dict[str, bool] = {"merchant": False}
_last_merchant_name: Optional[str] = None
_last_cosmetics_sig: Optional[str] = None
_last_cosmetics_at: float = 0.0
COSMETICS_COOLDOWN_MINUTES = int(os.getenv("COSMETICS_COOLDOWN_MINUTES", "240"))
MERCHANT_SUPPRESS_MINUTES = int(os.getenv("MERCHANT_SUPPRESS_MINUTES", "30"))
_last_merchant_name: Optional[str] = None
_last_merchant_sig: Optional[str] = None
_last_merchant_at: float = 0.0
SINGLE_ITEM_DEBOUNCE_SEC = int(os.getenv("SINGLE_ITEM_DEBOUNCE_SEC", "5"))
_last_announced_snapshot: Dict[str, Dict[str, int]] = {"seeds": {}, "pets": {}, "gears": {}}
_single_change_debounce: Dict[str, Dict[str, Any]] = {}
WEATHER_SUPPRESS_WINDOW_SEC = int(os.getenv("WEATHER_SUPPRESS_WINDOW_SEC", "10"))
_last_weather_announced_set: set[str] = set()
_last_weather_msg_time: float = 0.0

async def _resolve_channel(cid: int):
    if not cid: return None
    ch = bot.get_channel(cid)
    if ch is None:
        try:
            ch = await bot.fetch_channel(cid)
        except Exception as e:
            print(f"[warn] cannot fetch channel {cid}: {e}")
            return None
    return ch

@tree.command(name="payload", description="Download the latest raw payload as payload.json")
async def payload_cmd(interaction: discord.Interaction):
    if DEBUG_CHANNEL_ID and interaction.channel_id != DEBUG_CHANNEL_ID:
        await interaction.response.send_message(f"Please use this command in <#{DEBUG_CHANNEL_ID}>.", ephemeral=True)
        return
    if not DEBUG_CAPTURE_PAYLOAD:
        await interaction.response.send_message("Payload capture is disabled. Set DEBUG_CAPTURE_PAYLOAD=1 and redeploy.", ephemeral=True)
        return
    if not _last_raw_payload:
        await interaction.response.send_message("No payload captured yet.", ephemeral=True)
        return
    buf = io.StringIO()
    json.dump(_last_raw_payload, buf, indent=2)
    data = buf.getvalue().encode("utf-8")
    file = discord.File(fp=io.BytesIO(data), filename="payload.json")
    await interaction.response.send_message(content="Latest payload:", file=file, ephemeral=False)

ORDER_CONFIG_PATH = os.getenv("ORDER_CONFIG_PATH", "order_config.json")

def _load_order_from_file() -> Dict[str, Dict[str, int]]:
    try:
        with open(ORDER_CONFIG_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        out: Dict[str, Dict[str, int]] = {}
        for cat, arr in (data or {}).items():
            if not isinstance(arr, list): continue
            norm_cat = _map_cat(cat)
            if not norm_cat: continue
            out[norm_cat] = {str(name).strip().lower(): i for i, name in enumerate(arr)}
        return out
    except FileNotFoundError:
        return {}
    except Exception as e:
        print(f"[warn] failed to read {ORDER_CONFIG_PATH}: {e}")
        return {}

def _parse_csv_env(name: str) -> Dict[str, int]:
    raw = os.getenv(name, "")
    if not raw.strip(): return {}
    parts = [p.strip().lower() for p in raw.split(",") if p.strip()]
    return {p: i for i, p in enumerate(parts)}

def build_custom_order() -> Dict[str, Dict[str, int]]:
    order = _load_order_from_file()
    if "seeds" not in order:
        order["seeds"] = _parse_csv_env("ORDER_SEEDS")
    if "pets" not in order:
        order["pets"] = _parse_csv_env("ORDER_PETS")
    if "gears" not in order:
        order["gears"] = _parse_csv_env("ORDER_GEARS")
    return order

CUSTOM_ORDER = build_custom_order()

def sort_items(category: str, items: List[dict]) -> List[dict]:
    pri = CUSTOM_ORDER.get(category, {})
    if not pri:
        return items
    enumerated = list(enumerate(items))

    def key(pair):
        idx, it = pair
        name_l = str(it.get("name","")).strip().lower()
        return (pri.get(name_l, 10_000 + idx), idx)

    enumerated.sort(key=key)
    return [it for _, it in enumerated]

def _normalize_items(items: List[dict]) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for it in items:
        n = str(it.get("name", "")).strip()
        q = it.get("qty", 0)
        try:
            q = int(q)
        except Exception:
            q = 0
        out[n] = q
    return out

def _changed_item_names(prev: Dict[str, int], curr: Dict[str, int]) -> set:
    names = set(prev.keys()) | set(curr.keys())
    return {n for n in names if prev.get(n, None) != curr.get(n, None)}

def _cancel_debounce(cat: str):
    st = _single_change_debounce.get(cat)
    if st:
        t = st.get("task")
        if t and not t.done():
            t.cancel()
    _single_change_debounce.pop(cat, None)

async def _debounced_send_after(cat: str):
    try:
        await asyncio.sleep(SINGLE_ITEM_DEBOUNCE_SEC)
    except asyncio.CancelledError:
        return
    st = _single_change_debounce.get(cat)
    if not st:
        return
    items = st.get("pending_items") or []
    try:
        await send_batch_text(cat, items)
        _last_announced_snapshot[cat] = _normalize_items(items)
    except Exception as e:
        print(f"[debounce] send_batch_text({cat}) error: {e}")
    finally:
        _cancel_debounce(cat)

def _start_or_reset_debounce(cat: str, items: List[dict]):
    st = _single_change_debounce.get(cat)
    if st:
        t = st.get("task")
        if t and not t.done():
            t.cancel()
    _single_change_debounce[cat] = {"pending_items": items, "task": asyncio.create_task(_debounced_send_after(cat)),}

def _signature_for_cosmetics(items: List[dict]) -> str:
    norm = []
    for it in items:
        n = str(it.get("name", "")).strip().lower()
        q = it.get("qty")
        norm.append({"n": n, "q": q})
    norm.sort(key=lambda d: (d["n"], d["q"] if d["q"] is not None else -1))
    return hashlib.sha256(json.dumps(norm, sort_keys=True).encode()).hexdigest()

def _merchant_signature(items: List[dict]) -> str:
    norm = [{"n": str(i.get("name","")), "q": i.get("qty")} for i in items]
    norm.sort(key=lambda x: (x["n"].lower(), x["q"] if x["q"] is not None else -1))
    return hashlib.sha256(json.dumps(norm, sort_keys=True).encode()).hexdigest()

def _slug(s: str) -> str:
    s = (s or "").strip().lower()
    return re.sub(r"[^a-z0-9]+", "-", s).strip("-")

def _role_candidates(name: str, category: str) -> list[str]:
    pref = {
        "seeds": ROLE_PREFIX_SEEDS,
        "pets": ROLE_PREFIX_PETS,
        "gears": ROLE_PREFIX_GEARS,
        "merchant": ROLE_PREFIX_MERCHANT,
        "weathers": ROLE_PREFIX_WEATHERS,
    }.get(category, "")
    clean = name.strip()
    return [c for c in [
        clean,
        f"{pref}{clean}" if pref else None,
        f"{category.capitalize()} - {clean}",
        f"{category.capitalize()} {clean}",
        f"{category}: {clean}",
    ] if c]

def _build_guild_role_cache(guild: discord.Guild) -> Dict[str, discord.Role]:
    cache: Dict[str, discord.Role] = {}
    for r in guild.roles:
        cache[_slug(r.name)] = r
    _ROLE_CACHE[guild.id] = cache
    return cache

def _find_role(guild: discord.Guild, display_name: str, category: str) -> Optional[discord.Role]:
    if not guild or not display_name:
        return None
    cache = _ROLE_CACHE.get(guild.id) or _build_guild_role_cache(guild)
    for cand in _role_candidates(display_name, category):
        r = cache.get(_slug(cand))
        if r:
            return r
    return None



async def send_debug(obj):
    if not DEBUG_RAW or not DEBUG_CHANNEL_ID: return
    ch = await _resolve_channel(DEBUG_CHANNEL_ID)
    if not ch: return
    s = json.dumps(obj, indent=2)
    if len(s) <= 1900:
        await ch.send(f"```json\n{s}\n```")
    else:
        fp = io.BytesIO(s.encode("utf-8"))
        await ch.send("Full payload attached:", file=discord.File(fp, filename="payload.json"))



def parse_stock_payload(raw: dict) -> Tuple[Dict[str, List[dict]], Dict[str, Any]]:
    stock_map: Dict[str, List[dict]] = {}
    extras: Dict[str, Any] = {}
    if not isinstance(raw, dict):
        return stock_map, extras
    for key, items in raw.items():
        if isinstance(key, str) and key.endswith("_stock") and isinstance(items, list):
            base = key[:-6]
            base = base.rstrip("s")
            category = _map_cat(base)
            if category not in CATEGORY_CHANNELS:
                continue
            stock_map.setdefault(category, [])
            for it in items:
                name = it.get("display_name") or it.get("item_id") or it.get("name") or "(unknown)"
                qty  = it.get("quantity") or it.get("stock") or it.get("amount") or it.get("qty")
                ts   = it.get("Date_Start") or it.get("Date_Start_ISO") or it.get("ts") or it.get("start_date_unix")
                stock_map[category].append({"name": name, "qty": qty, "ts": ts})
    tm = raw.get("travelingmerchant_stock")
    if isinstance(tm, dict):
        items = tm.get("stock") or []
        extras["merchant_name"] = tm.get("merchantName") or tm.get("merchant_name")
        category = "merchant"
        stock_map.setdefault(category, [])
        for it in items:
            name = it.get("display_name") or it.get("item_id") or it.get("name") or "(unknown)"
            qty  = it.get("quantity") or it.get("stock") or it.get("amount") or it.get("qty")
            ts   = it.get("Date_Start") or it.get("Date_Start_ISO") or it.get("ts") or it.get("start_date_unix")
            stock_map[category].append({"name": name, "qty": qty, "ts": ts})
    return stock_map, extras

def parse_weather_payload(raw: dict) -> List[dict]:
    if not isinstance(raw, dict): return []
    arr = raw.get("weather")
    if not isinstance(arr, list): return []
    now = int(time.time())
    out: List[dict] = []
    for w in arr:
        if not isinstance(w, dict) or not w.get("active"):
            continue
        raw_name = w.get("weather_name") or w.get("weather_id") or "(unknown)"
        fixedweather = repair_weather_name(str(raw_name))
        end = w.get("end_duration_unix") or 0
        start = w.get("start_duration_unix") or 0
        dur = w.get("duration")
        remaining = None
        if isinstance(end, (int, float)) and end > 0:
            remaining = max(0, int(end - now))
            end = int(end)
        elif isinstance(dur, (int, float)) and isinstance(start, (int, float)) and start > 0:
            remaining = max(0, int(start + dur - now))
            end = int(start + dur)
        icon = w.get("icon") or w.get("image") or w.get("thumbnail")
        out.append({"name": fixedweather, "raw": str(raw_name), "remaining": remaining, "end": end or 0, "icon": icon,})
    out.sort(key=lambda x: x["name"].lower())
    return out

def _fmt_duration(sec: Optional[int]) -> str:
    if sec is None: return "active"
    if sec <= 0: return "ending"
    m, s = divmod(int(sec), 60)
    if m > 0:
        return f"{m}m {s}s left"
    return f"{s}s left"

def _build_text_lines(category: str, items: List[dict], title_hint: Optional[str] = None) -> str:
    title = f"{category.capitalize()} stock"
    if title_hint:
        title += f" — {title_hint}"
    header = f"**{title} ({len(items)} item{'s' if len(items)!=1 else ''})**"
    lines = [header]
    remaining_chars = 2000 - len(header) - 1
    shown = 0
    for it in items:
        name = str(it.get("name", "(unknown)"))
        qty  = it.get("qty")
        line = f"• {name} — **{qty}**"
        if len(line) + 1 <= remaining_chars:
            lines.append(line)
            remaining_chars -= (len(line) + 1)
            shown += 1
        else:
            break
    if shown < len(items):
        lines.append(f"… +{len(items)-shown} more")
    return "\n".join(lines)

async def send_batch_text(category: str, items: List[dict], title_hint: Optional[str] = None):
    cid = CATEGORY_CHANNELS.get(category, 0)
    ch = await _resolve_channel(cid)
    if not ch:
        print(f"[warn] no channel for category={category} (ID={cid})")
        return
    guild = ch.guild if hasattr(ch, "guild") else None
    if category in ("seeds", "pets", "gears"):
        items = sort_items(category, items)
    if category == "cosmetics":
        global _last_cosmetics_sig, _last_cosmetics_at
        sig = _signature_for_cosmetics(items)
        if _last_cosmetics_sig == sig:
            return
        now = time.time()
        if (_last_cosmetics_at > 0) and (now - _last_cosmetics_at < COSMETICS_COOLDOWN_MINUTES * 60):
            return
        _last_cosmetics_sig = sig
        _last_cosmetics_at = now
        content = _build_text_lines(category, items, title_hint=title_hint)
        await ch.send(content)
        return
    batch_signature = json.dumps([{"n": it.get("name"), "q": it.get("qty")} for it in items], sort_keys=False)
    if title_hint:
        batch_signature += f"|{title_hint}"
    h = hashlib.sha256(batch_signature.encode()).hexdigest()
    if _last_batch_hash.get(category) == h:
        return
    _last_batch_hash[category] = h
    roles_to_ping: List[discord.Role] = []
    if category == "merchant":
        header_title = "Merchant stock"
        header_suffix = ""
        if ROLE_MENTIONS and guild and title_hint:
            r = _find_role(guild, title_hint, "merchant")
            if r:
                header_suffix = f" — {r.mention}"
                roles_to_ping.append(r)
            elif title_hint:
                header_suffix = f" — {title_hint}"
        elif title_hint:
            header_suffix = f" — {title_hint}"
        header = f"**{header_title}{header_suffix} ({len(items)} item{'s' if len(items)!=1 else ''})**"
    else:
        header = f"**{category.capitalize()} stock ({len(items)} item{'s' if len(items)!=1 else ''})**"
    lines = [header]
    remaining_chars = 2000 - len(header) - 1
    for it in items:
        name = str(it.get("name", "(unknown)"))
        qty  = it.get("qty")
        label = name
        if ROLE_MENTIONS and guild and category in ("seeds", "pets", "gears"):
            r = _find_role(guild, name, category)
            if r:
                label = r.mention
                roles_to_ping.append(r)
        line = f"• {label} — **{qty}**"
        if len(line) + 1 <= remaining_chars:
            lines.append(line)
            remaining_chars -= (len(line) + 1)
        else:
            lines.append(f"… +{len(items) - (len(lines)-1)} more")
            break
    content = "\n".join(lines)
    am = AllowedMentions(everyone=False, users=False, roles=list(set(roles_to_ping)))
    await ch.send(content, allowed_mentions=am)

async def send_absent_notice(category: str, title_hint: Optional[str] = None):
    cid = CATEGORY_CHANNELS.get(category, 0)
    ch = await _resolve_channel(cid)
    if not ch:
        print(f"[warn] no channel for category={category} (ID={cid})")
        return
    if category == "merchant":
        msg = "**Traveling Merchant** — none right now."
        if title_hint:
            msg = f"**Traveling Merchant** — none right now (last: {title_hint})."
    elif category == "weathers":
        msg = "**Active Weathers** — none."
    else:
        msg = f"**{category.capitalize()}** — no items."
    await ch.send(msg)

async def send_weather_embeds(active_weathers: List[dict]):
    if not active_weathers:
        return
    cid = CATEGORY_CHANNELS.get("weathers", 0)
    ch = await _resolve_channel(cid)
    if not ch:
        print(f"[warn] no channel for category=weathers (ID={cid})")
        return
    sig = json.dumps([{"n": w.get("raw", w["name"]), "e": w.get("end", 0)} for w in active_weathers], sort_keys=True)
    global _last_weather_hash, _last_weather_announced_set, _last_weather_msg_time
    h = hashlib.sha256(sig.encode()).hexdigest()
    if _last_weather_hash == h:
        return
    _last_weather_hash = h
    now = time.time()
    guild = ch.guild if hasattr(ch, "guild") else None
    current_raws = {w.get("raw", w["name"]) for w in active_weathers}
    _last_weather_announced_set &= current_raws
    within_window = (now - _last_weather_msg_time) <= WEATHER_SUPPRESS_WINDOW_SEC if _last_weather_msg_time else False
    if within_window:
        filtered = [w for w in active_weathers if w.get("raw", w["name"]) not in _last_weather_announced_set]
        if not filtered:
            return
        to_post = filtered
    else:
        to_post = active_weathers
    roles_to_ping: List[discord.Role] = []
    lines: List[str] = []
    remaining = 2000

    def add_line(s: str) -> bool:
        nonlocal remaining
        need = len(s) + (1 if lines else 0)
        if need > remaining:
            return False
        lines.append(s)
        remaining -= need
        return True

    for w in to_post:
        label = w["name"]
        role_to_ping = None
        if ROLE_MENTIONS and guild:
            raw_id = w.get("raw", w["name"])
            if raw_id in ADMIN_ABUSE_WEATHERS:
                role_to_ping = _find_role(guild, ADMIN_ABUSE_ROLE_NAME, "weathers")
            else:
                role_to_ping = _find_role(guild, w["name"], "weathers")
            if role_to_ping:
                label = role_to_ping.mention
                roles_to_ping.append(role_to_ping)
        if not add_line(label):
            break
    content = "\n".join(lines) if lines else "**Active Weathers**"
    embeds: List[discord.Embed] = []
    for w in to_post[:10]:
        desc = f"{w['name']} — ends <t:{int(w['end'])}:R>" if w.get("end") else f"{w['name']} — active"
        e = Embed(description=desc, color=_color('weathers'))
        if w.get("icon"):
            try:
                e.set_thumbnail(url=str(w["icon"]))
            except Exception:
                pass
        embeds.append(e)
    am = AllowedMentions(everyone=False, users=False, roles=list(set(roles_to_ping)))
    await ch.send(content=content, embeds=embeds, allowed_mentions=am)
    _last_weather_msg_time = now
    _last_weather_announced_set |= {w.get("raw", w["name"]) for w in to_post}

async def send_update(category: str, data: dict):
    cid = CATEGORY_CHANNELS.get(category, 0)
    ch = await _resolve_channel(cid)
    if not ch:
        print(f"[warn] no channel for category={category} (ID={cid})")
        return
    key = (category, str(data.get("item", "?")))
    h = hashlib.sha256(json.dumps(data, sort_keys=True).encode()).hexdigest()
    if _last_item_hash.get(key) == h:
        return
    _last_item_hash[key] = h
    line = f"**{category.capitalize()} update:** {data.get('item','(unknown)')} — **{data.get('stock','?')}**"
    await ch.send(line)

async def ws_consumer():
    global _last_merchant_name
    if not EXTERNAL_WS_URL:
        print("[error] EXTERNAL_WS_URL not set")
        await bot.close()
        return
    headers = {}
    if WS_HEADERS_JSON.strip():
        try:
            headers = json.loads(WS_HEADERS_JSON)
        except Exception as e:
            print(f"[warn] bad WS_HEADERS_JSON: {e}")
    subscribe = None
    if WS_SUBSCRIBE_JSON.strip():
        try:
            subscribe = json.loads(WS_SUBSCRIBE_JSON)
        except Exception as e:
            print(f"[warn] bad WS_SUBSCRIBE_JSON: {e}")
    backoff = 1
    async with ClientSession() as session:
        while not bot.is_closed():
            try:
                print(f"[ws] connecting to {EXTERNAL_WS_URL}")
                async with session.ws_connect(EXTERNAL_WS_URL, heartbeat=PING_EVERY, headers=headers) as ws:
                    print("[ws] connected")
                    backoff = 1
                    if subscribe:
                        try:
                            await ws.send_json(subscribe)
                            print("[ws] sent subscribe frame")
                        except Exception as e:
                            print(f"[ws] subscribe error: {e}")
                    async for msg in ws:
                        if msg.type == WSMsgType.TEXT:
                            try:
                                raw = json.loads(msg.data)
                                global _last_raw_payload
                                if DEBUG_CAPTURE_PAYLOAD:
                                    _last_raw_payload = raw
                            except json.JSONDecodeError:
                                print(f"[ws] bad json :: {str(msg.data)[:200]}")
                                continue
                            except Exception as e:
                                print(f"[ws] unexpected json error: {e}")
                                continue

                            global _DEBUG_SENT_ONCE
                            if DEBUG_RAW and not _DEBUG_SENT_ONCE:
                                try:
                                    await send_debug(raw)
                                    _DEBUG_SENT_ONCE = True
                                except Exception as e:
                                    print(f"[debug] send_debug failed: {e}")
                            processed_any = False

                            if isinstance(raw, dict) and (
                                any(isinstance(v, list) and isinstance(k, str) and k.endswith("_stock") for k, v in raw.items())
                                or isinstance(raw.get("travelingmerchant_stock"), dict)):
                                try:
                                    stock_map, extras = parse_stock_payload(raw)
                                except Exception as e:
                                    print(f"[ws] parse_stock_payload error: {e}")
                                    stock_map, extras = {}, {}
                                merchant_items = stock_map.get("merchant", [])
                                curr_name = (extras.get("merchant_name") or "").strip() if isinstance(extras, dict) else ""
                                if merchant_items and curr_name:
                                    try:
                                        curr_sig = _merchant_signature(merchant_items)
                                    except Exception as e:
                                        print(f"[ws] merchant sig error: {e}")
                                        curr_sig = None
                                now = time.time()
                                announce = False
                                if _last_merchant_name != curr_name:
                                    announce = True
                                else:
                                    if (_last_merchant_at == 0.0) or (now - _last_merchant_at >= MERCHANT_SUPPRESS_MINUTES * 60):
                                        if curr_sig and (_last_merchant_sig != curr_sig):
                                            announce = True
                                if announce:
                                    try:
                                        await send_batch_text("merchant", merchant_items, title_hint=curr_name)
                                        _last_merchant_name = curr_name
                                        _last_merchant_sig  = curr_sig
                                        _last_merchant_at   = now
                                    except Exception as e:
                                        print(f"[ws] merchant send error: {e}")
                                processed_any = True
                                for cat, items in stock_map.items():
                                    if cat == "merchant":
                                        continue
                                    if cat not in CATEGORY_CHANNELS or not items:
                                        continue
                                    try:
                                        if cat in ("seeds", "pets", "gears"):
                                            curr_map = _normalize_items(items)
                                            prev_map = _last_announced_snapshot.get(cat, {})
                                            changed = _changed_item_names(prev_map, curr_map)
                                            if len(changed) == 1:
                                                _start_or_reset_debounce(cat, items)
                                            else:
                                                if _single_change_debounce.get(cat):
                                                    _cancel_debounce(cat)
                                                await send_batch_text(cat, items)
                                                _last_announced_snapshot[cat] = curr_map
                                        else:
                                            await send_batch_text(cat, items)
                                        processed_any = True
                                    except Exception as e:
                                        print(f"[ws] send_batch_text({cat}) error: {e}")
                            if isinstance(raw, dict) and isinstance(raw.get("weather"), list):
                                try:
                                    active_weathers = parse_weather_payload(raw)
                                except Exception as e:
                                    print(f"[ws] parse_weather_payload error: {e}")
                                    active_weathers = []
                                if active_weathers:
                                    try:
                                        await send_weather_embeds(active_weathers)
                                    except Exception as e:
                                        print(f"[ws] send_weather_embeds error: {e}")
                                processed_any = True
                        elif msg.type in (WSMsgType.CLOSED, WSMsgType.ERROR):
                            print("[ws] stream closed")
                            break
                    print("[ws] disconnected; reconnecting")
            except (ClientConnectorError, WSServerHandshakeError) as e:
                print(f"[ws] connect error: {e}")
            except Exception as e:
                print(f"[ws] unexpected: {e}")
            await asyncio.sleep(min(backoff, 30))
            backoff *= 2

@bot.event
async def on_ready():
    print(f"Logged in as {bot.user} (ID: {bot.user.id})")
    try:
        await tree.sync()
        print("[slash] commands synced")
    except Exception as e:
        print(f"[slash] sync failed: {e}")
    bot.loop.create_task(ws_consumer())

def shutdown(*_):
    if not bot.is_closed():
        loop = asyncio.get_event_loop()
        loop.create_task(bot.close())

def make_app():
    app = web.Application()
    async def root(_):
        return web.Response(text="ok")
    async def health(_):
        return web.Response(text="ok")
    app.router.add_get("/", root)
    app.router.add_get("/healthz", health)
    return app

async def run_http_and_bot():
    port = int(os.getenv("PORT", "10000"))
    app = make_app()
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host="0.0.0.0", port=port)
    await site.start()
    print(f"[http] listening on 0.0.0.0:{port}")
    await bot.start(DISCORD_TOKEN)

def main():
    if not DISCORD_TOKEN:
        print("[error] DISCORD_TOKEN not set"); sys.exit(1)
    try:
        asyncio.run(run_http_and_bot())
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()