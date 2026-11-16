require("dotenv").config();

const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const express = require("express");
const session = require("express-session");
const mqtt = require("mqtt");

const PORT = process.env.PORT || 3000;
const REGISTER_WINDOW_MS = 30000;
const MAX_LOGS = 5000;

const T = {
  cmd: process.env.MQTT_TOPIC_CMD || "iot/esp32/relay/cmd",
  status: process.env.MQTT_TOPIC_STATUS || "iot/esp32/relay/status",
  event: process.env.MQTT_TOPIC_EVENT || "iot/esp32/event",
  door: process.env.MQTT_TOPIC_DOOR || "iot/esp32/door/status",
  online: process.env.MQTT_TOPIC_ONLINE || "iot/esp32/online",
  regCmd: process.env.MQTT_TOPIC_REG_CMD || "iot/esp32/register/cmd",
  regResult: process.env.MQTT_TOPIC_REG_RESULT || "iot/esp32/register/result",
  authList: process.env.MQTT_TOPIC_AUTH_LIST || "iot/esp32/auth/list",
};

/* ============================================================
   DATABASE (file JSON, tulis atomik)
   ============================================================ */
const DATA_DIR = path.join(__dirname, "data");
const DB_FILE = path.join(DATA_DIR, "db.json");
fs.mkdirSync(DATA_DIR, { recursive: true });

let db;

function saveNow() {
  const tmp = DB_FILE + ".tmp";
  fs.writeFileSync(tmp, JSON.stringify(db));
  fs.renameSync(tmp, DB_FILE);
}

let saveTimer = null;
function save() {
  clearTimeout(saveTimer);
  saveTimer = setTimeout(saveNow, 300);
}

try {
  db = JSON.parse(fs.readFileSync(DB_FILE, "utf8"));
  db.cards ||= [];
  db.logs ||= [];
  db.nextId ||= 1;
} catch {
  // Seed dengan kartu yang sebelumnya hardcode di ESP32
  db = {
    nextId: 1,
    logs: [],
    cards: ["DA:FA:10:85", "01:32:D0:1C", "1A:B7:14:85"].map((uid, i) => ({
      uid,
      name: `Kartu ${i + 1}`,
      createdAt: new Date().toISOString(),
    })),
  };
  saveNow();
}

["SIGINT", "SIGTERM"].forEach((sig) =>
  process.on(sig, () => {
    saveNow();
    process.exit(0);
  })
);

/* ============================================================
   HELPER
   ============================================================ */
function normalizeUid(input) {
  const hex = String(input ?? "").toUpperCase().replace(/[^0-9A-F]/g, "");
  if (hex.length < 8 || hex.length > 20 || hex.length % 2) return null;
  return hex.match(/../g).join(":");
}

const findCard = (uid) => db.cards.find((c) => c.uid === uid);

function safeEqual(a, b) {
  const ha = crypto.createHash("sha256").update(String(a ?? "")).digest();
  const hb = crypto.createHash("sha256").update(String(b ?? "")).digest();
  return crypto.timingSafeEqual(ha, hb);
}

/* ============================================================
   STATE + SSE (realtime ke dashboard)
   ============================================================ */
const state = {
  device: "UNKNOWN", // ONLINE | OFFLINE | UNKNOWN
  relay: "UNKNOWN",
  door: "UNKNOWN", // OPEN | CLOSED | UNKNOWN
  registration: { status: "IDLE", uid: null, name: null, expiresAt: null },
};

const sseClients = new Set();

function broadcast(event, data) {
  const payload = `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
  for (const res of sseClients) res.write(payload);
}

setInterval(() => {
  for (const res of sseClients) res.write(": ping\n\n");
}, 20000);

function publicState() {
  const midnight = new Date();
  midnight.setHours(0, 0, 0, 0);

  const stats = { granted: 0, denied: 0, forced: 0 };
  let unackAlerts = 0;

  for (const l of db.logs) {
    if (l.type === "FORCED_OPEN" && !l.ack) unackAlerts++;
  }
  for (const l of db.logs) {
    if (l.ts < midnight.getTime()) break;
    if (l.type === "ACCESS_GRANTED") stats.granted++;
    else if (l.type === "ACCESS_DENIED") stats.denied++;
    else if (l.type === "FORCED_OPEN") stats.forced++;
  }

  return {
    serverTime: Date.now(),
    mqttConnected: mqttClient.connected,
    device: state.device,
    relay: state.relay,
    door: state.door,
    registration: state.registration,
    unackAlerts,
    stats,
  };
}

const pushState = () => broadcast("state", publicState());

function addLog(type, { uid = null, via = null, name = null } = {}) {
  const card = uid ? findCard(uid) : null;
  const entry = {
    id: db.nextId++,
    ts: Date.now(),
    type,
    uid,
    name: name ?? card?.name ?? null,
    via,
  };
  if (type === "FORCED_OPEN") entry.ack = false;

  db.logs.unshift(entry);
  if (db.logs.length > MAX_LOGS) db.logs.length = MAX_LOGS;
  save();

  broadcast("log", entry);
  pushState();
  return entry;
}

/* ============================================================
   MQTT
   ============================================================ */
const mqttClient = mqtt.connect(process.env.MQTT_URL, {
  username: process.env.MQTT_USERNAME,
  password: process.env.MQTT_PASSWORD,
  clientId: "iot_backend_cinlok",
  reconnectPeriod: 3000,
});

function publishAuthList() {
  if (!mqttClient.connected) return;
  const payload = "LIST:" + db.cards.map((c) => c.uid).join(",");
  // retained: ESP mendapat daftar terbaru begitu tersambung
  mqttClient.publish(T.authList, payload, { qos: 1, retain: true });
}

const DEVICE_EVENTS = new Set([
  "ACCESS_GRANTED",
  "ACCESS_DENIED",
  "DOOR_OPENED",
  "DOOR_CLOSED",
  "FORCED_OPEN",
]);

function handleDeviceEvent(raw) {
  let ev;
  try {
    ev = JSON.parse(raw);
  } catch {
    return;
  }
  if (!ev || !DEVICE_EVENTS.has(ev.type)) return;

  const uid = ev.uid ? normalizeUid(ev.uid) : null;
  const via = ["CARD", "DASHBOARD"].includes(ev.via) ? ev.via : null;
  addLog(ev.type, { uid, via });
}

let regTimer = null;

function setRegistration(next) {
  clearTimeout(regTimer);
  state.registration = {
    status: "IDLE",
    uid: null,
    name: null,
    expiresAt: null,
    ...next,
  };
  if (state.registration.status === "WAITING") {
    regTimer = setTimeout(() => {
      if (state.registration.status === "WAITING") {
        setRegistration({ status: "TIMEOUT" });
      }
    }, REGISTER_WINDOW_MS + 3000);
  }
  pushState();
}

function handleRegisterResult(raw) {
  let r;
  try {
    r = JSON.parse(raw);
  } catch {
    return;
  }
  if (state.registration.status !== "WAITING") return;

  if (r.status === "SCANNED") {
    const uid = normalizeUid(r.uid);
    if (!uid) return;
    const existing = findCard(uid);
    if (existing) {
      setRegistration({ status: "DUPLICATE", uid, name: existing.name });
    } else {
      setRegistration({ status: "SCANNED", uid });
    }
  } else if (r.status === "TIMEOUT") {
    setRegistration({ status: "TIMEOUT" });
  } else if (r.status === "CANCELLED") {
    setRegistration({ status: "IDLE" });
  }
}

mqttClient.on("connect", () => {
  console.log("MQTT terhubung");
  mqttClient.subscribe(
    [T.status, T.event, T.door, T.online, T.regResult],
    { qos: 1 }
  );
  publishAuthList();
  pushState();
});

mqttClient.on("reconnect", () => console.log("Mencoba menghubungkan ulang MQTT..."));
mqttClient.on("close", pushState);
mqttClient.on("error", (err) => console.error("MQTT error:", err.message));

mqttClient.on("message", (topic, buf) => {
  const msg = buf.toString().trim();

  switch (topic) {
    case T.status:
      if (["ON", "OFF"].includes(msg)) {
        state.relay = msg;
        pushState();
      }
      break;

    case T.door:
      if (["OPEN", "CLOSED"].includes(msg)) {
        state.door = msg;
        pushState();
      }
      break;

    case T.online: {
      const next = msg === "online" ? "ONLINE" : msg === "offline" ? "OFFLINE" : null;
      if (!next || next === state.device) break;
      const hadState = state.device !== "UNKNOWN";
      state.device = next;
      if (hadState) addLog(next === "ONLINE" ? "DEVICE_ONLINE" : "DEVICE_OFFLINE");
      else pushState();
      break;
    }

    case T.event:
      handleDeviceEvent(msg);
      break;

    case T.regResult:
      handleRegisterResult(msg);
      break;
  }
});

/* ============================================================
   EXPRESS
   ============================================================ */
const app = express();

app.disable("x-powered-by");
app.set("trust proxy", 1); // di belakang Cloudflare Tunnel

app.use((req, res, next) => {
  res.set({
    "X-Content-Type-Options": "nosniff",
    "X-Frame-Options": "DENY",
    "Referrer-Policy": "same-origin",
  });
  next();
});

app.use(express.json({ limit: "10kb" }));
app.use(express.urlencoded({ extended: false, limit: "10kb" }));

app.use(
  session({
    name: "iot.sid",
    secret: process.env.SESSION_SECRET,
    resave: false,
    saveUninitialized: false,
    cookie: {
      httpOnly: true,
      secure: "auto", // aktif otomatis jika lewat HTTPS (Cloudflare)
      sameSite: "lax",
      maxAge: 2 * 60 * 60 * 1000,
    },
  })
);

const authPage = (req, res, next) =>
  req.session.loggedIn ? next() : res.redirect("/login");

const authApi = (req, res, next) =>
  req.session.loggedIn ? next() : res.status(401).json({ error: "Belum login" });

const view = (name) => (req, res) => {
  res.set("Cache-Control", "no-store");
  res.sendFile(path.join(__dirname, "views", name));
};

/* ---------- Login (dengan pembatasan percobaan) ---------- */
const fails = new Map(); // ip -> { count, resetAt }
const clientIp = (req) => req.headers["cf-connecting-ip"] || req.ip;

setInterval(() => {
  const now = Date.now();
  for (const [ip, f] of fails) if (f.resetAt < now) fails.delete(ip);
}, 10 * 60 * 1000);

app.get("/", (req, res) => res.redirect(req.session.loggedIn ? "/dashboard" : "/login"));
app.get("/login", view("login.html"));
app.get("/dashboard", authPage, view("dashboard.html"));

app.post("/login", (req, res, next) => {
  const ip = clientIp(req);
  const f = fails.get(ip);
  if (f && f.resetAt > Date.now() && f.count >= 5) {
    return res.redirect("/login?error=limit");
  }

  const okUser = safeEqual(req.body.username, process.env.WEB_USERNAME);
  const okPass = safeEqual(req.body.password, process.env.WEB_PASSWORD);

  if (!(okUser && okPass)) {
    const cur = f && f.resetAt > Date.now() ? f : { count: 0, resetAt: Date.now() + 15 * 60 * 1000 };
    cur.count++;
    fails.set(ip, cur);
    return res.redirect("/login?error=1");
  }

  fails.delete(ip);
  req.session.regenerate((err) => {
    if (err) return next(err);
    req.session.loggedIn = true;
    res.redirect("/dashboard");
  });
});

app.post("/logout", authPage, (req, res, next) => {
  req.session.destroy((err) => {
    if (err) return next(err);
    res.clearCookie("iot.sid");
    res.redirect("/login");
  });
});

/* ---------- Realtime stream ---------- */
app.get("/api/stream", authApi, (req, res) => {
  res.set({
    "Content-Type": "text/event-stream",
    "Cache-Control": "no-cache, no-transform",
    Connection: "keep-alive",
    "X-Accel-Buffering": "no",
  });
  res.flushHeaders();
  res.write("retry: 3000\n\n");
  sseClients.add(res);
  req.on("close", () => sseClients.delete(res));
});

/* ---------- State & riwayat ---------- */
app.get("/api/state", authApi, (req, res) => res.json(publicState()));

app.get("/api/logs", authApi, (req, res) => {
  const limit = Math.min(Math.max(parseInt(req.query.limit, 10) || 200, 1), 1000);
  res.json(db.logs.slice(0, limit));
});

app.post("/api/alerts/ack", authApi, (req, res) => {
  for (const l of db.logs) if (l.type === "FORCED_OPEN") l.ack = true;
  save();
  broadcast("ack", {});
  pushState();
  res.json({ message: "Peringatan ditandai sudah ditangani" });
});

/* ---------- Relay ---------- */
app.post("/api/relay", authApi, (req, res) => {
  const command = req.body.command;

  if (!["ON", "OFF"].includes(command)) {
    return res.status(400).json({ error: "Perintah harus ON atau OFF" });
  }
  if (!mqttClient.connected) {
    return res.status(503).json({ error: "MQTT broker tidak terhubung" });
  }

  mqttClient.publish(T.cmd, command, { qos: 1, retain: false }, (err) => {
    if (err) return res.status(500).json({ error: "Gagal mengirim perintah" });

    addLog(command === "ON" ? "REMOTE_UNLOCK" : "REMOTE_LOCK", { via: "DASHBOARD" });
    res.json({ message: `Perintah ${command} terkirim ke perangkat` });
  });
});

/* ---------- Kartu RFID ---------- */
app.get("/api/cards", authApi, (req, res) => res.json(db.cards));

app.post("/api/cards", authApi, (req, res) => {
  const uid = normalizeUid(req.body.uid);
  const name = String(req.body.name ?? "").trim().slice(0, 40);

  if (!uid) return res.status(400).json({ error: "UID tidak valid" });
  if (!name) return res.status(400).json({ error: "Nama wajib diisi" });
  if (findCard(uid)) return res.status(409).json({ error: "Kartu sudah terdaftar" });

  db.cards.push({ uid, name, createdAt: new Date().toISOString() });
  save();
  publishAuthList();

  if (state.registration.uid === uid) setRegistration({ status: "IDLE" });

  addLog("CARD_REGISTERED", { uid });
  broadcast("cards", db.cards);
  res.status(201).json({ message: `Kartu ${name} berhasil didaftarkan` });
});

app.patch("/api/cards/:uid", authApi, (req, res) => {
  const card = findCard(normalizeUid(req.params.uid));
  const name = String(req.body.name ?? "").trim().slice(0, 40);

  if (!card) return res.status(404).json({ error: "Kartu tidak ditemukan" });
  if (!name) return res.status(400).json({ error: "Nama wajib diisi" });

  card.name = name;
  save();
  broadcast("cards", db.cards);
  res.json({ message: "Nama diperbarui" });
});

app.delete("/api/cards/:uid", authApi, (req, res) => {
  const uid = normalizeUid(req.params.uid);
  const card = findCard(uid);
  if (!card) return res.status(404).json({ error: "Kartu tidak ditemukan" });

  db.cards = db.cards.filter((c) => c.uid !== uid);
  save();
  publishAuthList();

  addLog("CARD_DELETED", { uid, name: card.name });
  broadcast("cards", db.cards);
  res.json({ message: `Kartu ${card.name} dihapus` });
});

/* ---------- Registrasi kartu ---------- */
app.post("/api/register/start", authApi, (req, res) => {
  if (!mqttClient.connected) {
    return res.status(503).json({ error: "MQTT broker tidak terhubung" });
  }
  if (state.device === "OFFLINE") {
    return res.status(503).json({ error: "Perangkat ESP32 sedang offline" });
  }

  mqttClient.publish(T.regCmd, "START", { qos: 1, retain: false });
  setRegistration({
    status: "WAITING",
    expiresAt: Date.now() + REGISTER_WINDOW_MS,
  });
  res.json({ message: "Menunggu kartu di-scan pada reader" });
});

app.post("/api/register/cancel", authApi, (req, res) => {
  if (mqttClient.connected) {
    mqttClient.publish(T.regCmd, "CANCEL", { qos: 1, retain: false });
  }
  setRegistration({ status: "IDLE" });
  res.json({ message: "Registrasi dibatalkan" });
});

app.use((err, req, res, next) => {
  console.error(err);
  res.status(500).json({ error: "Terjadi kesalahan server" });
});

app.listen(PORT, "127.0.0.1", () => {
  console.log(`Backend berjalan pada port ${PORT}`);
});
