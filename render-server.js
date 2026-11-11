"use strict";

// Render-compatible implementation of the HTS relay protocol.
// Rooms live in memory and are intentionally hosted by one service instance.
const http = require("http");
const crypto = require("crypto");
const { WebSocket, WebSocketServer } = require("ws");

const PORT = Number.parseInt(process.env.PORT || "10000", 10);
const MAX_PACKET_LENGTH = 20000;
const MAX_SKIN_BYTES = 7 * 1024 * 1024;
const MAX_PLAYER_SKIN_BYTES = 12 * 1024 * 1024;
const MAX_ROOM_SKIN_BYTES = 128 * 1024 * 1024;
const ROOM_ID_RE = /^[A-F0-9]{32}$/;
const rooms = new Map();

function sendText(res, status, body, contentType) {
	res.writeHead(status, {
		"content-type": contentType || "text/plain; charset=utf-8",
		"cache-control": "no-store",
		"x-content-type-options": "nosniff"
	});
	res.end(body);
}

function base64(value) {
	return Buffer.from(value == null ? "" : String(value), "utf8").toString("base64");
}

function encodeWire(command, fields) {
	return [command].concat((fields || []).map(base64)).join("\t");
}

function decodeWire(line) {
	try {
		const parts = line.split("\t");
		if (!/^[A-Z_]{1,32}$/.test(parts[0])) return null;
		const fields = [];
		for (let i = 1; i < parts.length; i++) {
			if (!/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(parts[i])) return null;
			fields.push(Buffer.from(parts[i], "base64").toString("utf8"));
		}
		return [parts[0]].concat(fields);
	} catch (_) {
		return null;
	}
}

function sendWire(ws, command, fields) {
	if (ws.readyState === WebSocket.OPEN) ws.send(encodeWire(command, fields));
}

function broadcast(room, command, fields, excluded) {
	const line = encodeWire(command, fields);
	for (const client of room.members.values()) {
		if (client !== excluded && client.readyState === WebSocket.OPEN) client.send(line);
	}
}

function roomListing() {
	const lines = [];
	for (const room of rooms.values()) {
		if (room.visibility !== "public" || room.members.size < 1) continue;
		lines.push([base64(room.id), base64(room.name), room.members.size, room.capacity].join("|"));
	}
	return lines.join("\n");
}

function sendRoster(ws, room) {
	const fields = [];
	for (const client of room.members.values()) {
		if (client.peer) fields.push(client.peer.id, client.peer.nickname);
	}
	sendWire(ws, "ROSTER", fields);
}

function sendSnapshots(ws, room) {
	for (const client of room.members.values()) {		
		if (client === ws || !client.peer || !client.peer.lastMove) continue;
		const peer = client.peer;
		const fields = [peer.id, peer.nickname].concat(peer.lastMove, [peer.owner ? "1" : "0"]);
		if (fields.length > 11 && peer.lastState) fields[11] = peer.lastState;
		sendWire(ws, "TRUCK", fields);
	}
}

function fail(ws, message) {
	if (ws.readyState !== WebSocket.OPEN) return;
	sendWire(ws, "ERROR", [message]);
	ws.close(1008, String(message).slice(0, 120));
}

function joinRoom(ws, message) {
	const command = message[0];
	const create = command === "CREATE";
	if (!create && command !== "JOIN") return fail(ws, "Envie CREATE ou JOIN primeiro.");
	const roomId = ws.roomId;
	const nickname = (message[1] || "").trim();
	if (!nickname || nickname.length > 24) return fail(ws, "Nickname inválido.");

	let room = rooms.get(roomId);
	let capacity = 0;
	let roomName = "";
	let visibility = "private";
	if (create) {
		if (room) return fail(ws, "Este código de sala já está em uso.");
		capacity = Number.parseInt(message[3], 10);
		roomName = (message[4] || ("Sala de " + nickname)).trim().slice(0, 40);
		visibility = message[5] === "public" ? "public" : "private";
		if (!Number.isFinite(capacity) || capacity < 2 || capacity > 64) return fail(ws, "Capacidade inválida (2 a 64).");
		room = { id: roomId, name: roomName || ("Sala de " + nickname), capacity, visibility, ownerId: "", members: new Map(), banned: new Set(), skins: new Map(), skinBytes: 0 };
	} else {
		if (!room) return fail(ws, "Sala encerrada ou código inválido.");
		if (room.members.size >= room.capacity) return fail(ws, "A sala está cheia.");
		if (room.banned.has(nickname.toLowerCase())) return fail(ws, "Nickname banido desta sala.");
	}

	const playerId = crypto.randomBytes(16).toString("hex").toUpperCase();
	const peer = {
		id: playerId,
		token: crypto.randomBytes(16).toString("hex").toUpperCase(),
		nickname,
		owner: create,
		lastMove: null,
		lastState: ""
	};
	ws.peer = peer;
	ws.roomId = roomId;
	if (create) {
		room.ownerId = playerId;
		rooms.set(roomId, room);
	}
	room.members.set(playerId, ws);

	sendWire(ws, "WELCOME", [peer.id, "0", peer.token, peer.owner ? "1" : "0", room.ownerId]);
	if (create) sendWire(ws, "ROOM_CREATED", []);
	sendRoster(ws, room);
	sendSnapshots(ws, room);
	if (!create) broadcast(room, "PLAYER_JOINED", [peer.id, peer.nickname], ws);
}

function moderate(room, owner, command, targetId) {
	if (!owner.owner) return;
	for (const client of room.members.values()) {
		const target = client.peer;
		if (!target || target.id !== targetId || target.owner) continue;
		if (command === "BAN") room.banned.add(target.nickname.toLowerCase());
		sendWire(client, "ERROR", [command === "BAN" ? "Banido pelo dono da sala." : "Expulso pelo dono da sala."]);
		client.close(1008, command);
		return;
	}
}

function handleMessage(ws, raw, isBinary) {
	if (isBinary) return fail(ws, "Somente pacotes de texto são aceitos.");
	const line = raw.toString("utf8");
	if (Buffer.byteLength(line, "utf8") > MAX_PACKET_LENGTH) return fail(ws, "Pacote inválido.");
	const message = decodeWire(line);
	if (!message || message.length < 1) return fail(ws, "Pacote inválido.");
	if (!ws.peer) return joinRoom(ws, message);

	const peer = ws.peer;
	const room = rooms.get(ws.roomId);
	if (!room || !room.members.has(peer.id)) return fail(ws, "Sala encerrada.");

	if (message[0] === "MOVE" && message.length >= 7 && message.length <= 32) {
		const move = message.slice(1);
		if (move.some((value) => value.length > 4096)) return;
		peer.lastMove = move;
		if (move.length > 9 && move[9]) peer.lastState = move[9];
		const truck = [peer.id, peer.nickname].concat(move, [peer.owner ? "1" : "0"]);
		return broadcast(room, "TRUCK", truck, ws);
	}
	if (message[0] === "CHAT" && message.length >= 2) {
		return broadcast(room, "CHAT", [peer.nickname, (message[1] || "").slice(0, 500)], null);
	}
	if (message[0] === "VOICE" && message.length >= 2 && message[1].length <= 4096) {
		return broadcast(room, "VOICE", [message[1]], ws);
	}
	if ((message[0] === "KICK" || message[0] === "BAN") && message.length >= 2 && peer.owner) {
		return moderate(room, peer, message[0], message[1]);
	}
	if (message[0] === "LIST") {
		return sendWire(ws, "SERVER_LIST", [room.name + " " + room.members.size + "/" + room.capacity]);
	}
	if (message[0] === "SKIN_LIST") return sendSkinManifest(ws, room);
	if (message[0] === "SKIN_DELETE" && message.length >= 2 && /^[A-Za-z0-9_-]{1,48}$/.test(message[1])) {
		return removePlayerSkin(room, peer.id, message[1]);
	}
}

function removePeer(ws) {
	const peer = ws.peer;
	const roomId = ws.roomId;
	if (!peer) return;
	ws.peer = null;
	const room = rooms.get(roomId);
	if (!room) return;
	removeAllPlayerSkins(room, peer.id, ws);
	room.members.delete(peer.id);
	if (peer.owner) {
		const candidates = Array.from(room.members.values());
		if (candidates.length > 0) {
			const nextHost = candidates[Math.floor(Math.random() * candidates.length)];
			room.ownerId = nextHost.peer.id;
			for (const client of candidates) client.peer.owner = client === nextHost;
			broadcast(room, "PLAYER_LEFT", [peer.id], ws);
			broadcast(room, "HOST_CHANGED", [nextHost.peer.id, nextHost.peer.nickname, room.name, room.capacity], null);
			broadcast(room, "DIRECT_MODE", ["0"], null);
			return;
		}
		room.skins.clear();
		room.skinBytes = 0;
		rooms.delete(roomId);
		return;
	}
	broadcast(room, "PLAYER_LEFT", [peer.id], ws);
}

function sendSkinManifest(ws, room) {
	for (const entry of room.skins.values())
		sendWire(ws, "SKIN_AVAILABLE", [entry.playerId, entry.slot, entry.hash, entry.size, entry.type, entry.model]);
}

function removePlayerSkin(room, playerId, slot, excluded) {
	const key = playerId + "|" + slot;
	const old = room.skins.get(key);
	if (!old) return;
	room.skins.delete(key);
	room.skinBytes = Math.max(0, room.skinBytes - old.size);
	broadcast(room, "SKIN_REMOVED", [playerId, slot], excluded || null);
}

function removeAllPlayerSkins(room, playerId, excluded) {
	for (const [key, entry] of room.skins) {
		if (entry.playerId === playerId) removePlayerSkin(room, playerId, entry.slot, excluded);
	}
}

function readSkinBody(req) {
	return new Promise((resolve, reject) => {
		const chunks = [];
		let total = 0;
		let tooLarge = false;
		req.on("data", (chunk) => {
			if (tooLarge) return;
			total += chunk.length;
			if (total > MAX_SKIN_BYTES) {
				tooLarge = true;
				chunks.length = 0;
				reject(Object.assign(new Error("Skin exceeds 7 MB"), { statusCode: 413 }));
				return;
			}
			chunks.push(chunk);
		});
		req.on("end", () => { if (!tooLarge) resolve(Buffer.concat(chunks, total)); });
		req.on("error", reject);
	});
}

async function handleSkinRequest(req, res, roomId, targetPlayer, slot) {
	const room = rooms.get(roomId);
	const playerId = req.headers["x-hts-player"] || "";
	const token = req.headers["x-hts-token"] || "";
	const client = room && room.members.get(playerId);
	if (!client || !client.peer || client.peer.token !== token) return sendText(res, 401, "Unauthorized");
	if (!targetPlayer || !slot) {
		if (req.method !== "GET") return sendText(res, 400, "Bad request");
		const manifest = Array.from(room.skins.values()).map((entry) => ({ playerId: entry.playerId, slot: entry.slot, hash: entry.hash, size: entry.size, type: entry.type, model: entry.model }));
		return sendText(res, 200, JSON.stringify(manifest), "application/json; charset=utf-8");
	}
	if (!ROOM_ID_RE.test(targetPlayer) || !/^[A-Za-z0-9_-]{1,48}$/.test(slot)) return sendText(res, 400, "Bad request");
	const key = targetPlayer + "|" + slot;
	if (req.method === "DELETE") {
		if (targetPlayer !== playerId) return sendText(res, 403, "Forbidden");
		removePlayerSkin(room, playerId, slot);
		return sendText(res, 200, "deleted");
	}
	if (req.method === "PUT") {
		if (targetPlayer !== playerId) return sendText(res, 403, "Forbidden");
		const type = String(req.headers["content-type"] || "").split(";")[0].trim().toLowerCase();
		if (type !== "image/png" && type !== "image/jpeg") return sendText(res, 415, "Only PNG/JPEG skins are accepted");
		if (Number(req.headers["content-length"] || 0) > MAX_SKIN_BYTES) return sendText(res, 413, "Skin exceeds 7 MB");
		let bytes;
		try { bytes = await readSkinBody(req); }
		catch (error) { return sendText(res, error.statusCode || 400, error.message || "Upload failed"); }
		if (bytes.length < 16 || bytes.length > MAX_SKIN_BYTES) return sendText(res, 413, "Invalid skin size");
		const validPng = bytes.length >= 8 && bytes.subarray(0, 8).equals(Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]));
		const validJpeg = bytes.length >= 3 && bytes[0] === 0xff && bytes[1] === 0xd8 && bytes[2] === 0xff;
		if ((type === "image/png" && !validPng) || (type === "image/jpeg" && !validJpeg)) return sendText(res, 415, "Image signature does not match Content-Type");
		const model = String(req.headers["x-hts-model"] || "").replace(/[^A-Za-z0-9 _().-]/g, "_").slice(0, 64);
		const hash = crypto.createHash("sha256").update(bytes).digest("hex");
		const old = room.skins.get(key);
		let playerBytes = 0;
		for (const entry of room.skins.values()) if (entry.playerId === playerId) playerBytes += entry.size;
		const nextPlayerBytes = playerBytes - (old ? old.size : 0) + bytes.length;
		const nextRoomBytes = room.skinBytes - (old ? old.size : 0) + bytes.length;
		if (nextPlayerBytes > MAX_PLAYER_SKIN_BYTES) return sendText(res, 413, "Player skin cache is limited to 12 MB");
		if (nextRoomBytes > MAX_ROOM_SKIN_BYTES) return sendText(res, 413, "Room skin cache is full");
		if (!old || old.hash !== hash || old.model !== model || old.type !== type) {
			room.skins.set(key, { playerId, slot, hash, size: bytes.length, type, model, bytes });
			room.skinBytes = nextRoomBytes;
			broadcast(room, "SKIN_AVAILABLE", [playerId, slot, hash, bytes.length, type, model], null);
		}
		return sendText(res, 200, JSON.stringify({ hash, size: bytes.length }), "application/json; charset=utf-8");
	}
	if (req.method !== "GET") return sendText(res, 405, "Method not allowed");
	const stored = room.skins.get(key);
	if (!stored) return sendText(res, 404, "Not found");
	res.writeHead(200, { "content-type": stored.type, "content-length": String(stored.size), "x-hts-sha256": stored.hash, "cache-control": "no-store", "x-content-type-options": "nosniff" });
	res.end(stored.bytes);
}

const server = http.createServer((req, res) => {
	const url = new URL(req.url, "http://localhost");
	const skin = url.pathname.match(/^\/api\/rooms\/([A-F0-9]{32})\/skins(?:\/([A-F0-9]{32})\/([A-Za-z0-9_-]{1,48}))?$/i);
	if (skin && ["GET", "PUT", "DELETE"].includes(req.method)) {
		return handleSkinRequest(req, res, skin[1].toUpperCase(), skin[2] ? skin[2].toUpperCase() : "", skin[3] || "")
			.catch((error) => { if (!res.headersSent) sendText(res, error.statusCode || 500, error.message || "Skin request failed"); });
	}
	if (req.method === "GET" && url.pathname === "/health") {
		return sendText(res, 200, "HTS relay online");
	}
	if (req.method === "GET" && url.pathname === "/api/rooms") {
		return sendText(res, 200, roomListing());
	}
	const invite = url.pathname.match(/^\/join\/([A-F0-9]{32})$/i);
	if (req.method === "GET" && invite) {
		const roomId = invite[1].toUpperCase();
		const html = "<!doctype html><meta name=viewport content='width=device-width,initial-scale=1'>" +
			"<title>Convite HTS</title><main style='font:16px system-ui;max-width:34em;margin:12vh auto;padding:1em'>" +
			"<h2>Sala do Heavy Truck Simulator</h2><p>Copie o código abaixo e cole na opção Multiplayer do jogo.</p>" +
			"<code id=c style='word-break:break-all'>" + roomId + "</code> " +
			"<button onclick=\"navigator.clipboard.writeText(document.querySelector('#c').textContent)\">Copiar código</button></main>";
		return sendText(res, 200, html, "text/html; charset=utf-8");
	}
	return sendText(res, 404, "Not found");
});

const webSockets = new WebSocketServer({ noServer: true, maxPayload: MAX_PACKET_LENGTH, perMessageDeflate: false });

server.on("upgrade", (req, socket, head) => {
	let url;
	try { url = new URL(req.url, "http://localhost"); }
	catch (_) { socket.destroy(); return; }
	const match = url.pathname.match(/^\/ws\/([A-F0-9]{32})$/i);
	if (!match) { socket.destroy(); return; }
	const roomId = match[1].toUpperCase();
	webSockets.handleUpgrade(req, socket, head, (ws) => {
		ws.roomId = roomId;
		ws.isAlive = true;
		webSockets.emit("connection", ws, req);
	});
});

webSockets.on("connection", (ws) => {
	const joinTimeout = setTimeout(() => {
		if (!ws.peer) fail(ws, "Tempo esgotado para criar ou entrar na sala.");
	}, 12000);
	ws.on("message", (data, isBinary) => {
		if (!ws.peer) clearTimeout(joinTimeout);
		try { handleMessage(ws, data, isBinary); }
		catch (error) {
			console.error("Erro no pacote multiplayer:", error && error.message ? error.message : error);
			fail(ws, "Erro ao processar o pacote da sala.");
		}
	});
	ws.on("pong", () => { ws.isAlive = true; });
	ws.on("close", () => { clearTimeout(joinTimeout); removePeer(ws); });
	ws.on("error", (error) => { console.warn("WebSocket encerrado:", error.message); });
});

const heartbeat = setInterval(() => {
	for (const ws of webSockets.clients) {
		if (ws.isAlive === false) { ws.terminate(); continue; }
		ws.isAlive = false;
		ws.ping();
	}
}, 30000);

server.listen(PORT, "0.0.0.0", () => {
	console.log("HTS relay listening on port " + PORT);
});

function shutdown() {
	clearInterval(heartbeat);
	for (const ws of webSockets.clients) {
		try { sendWire(ws, "ERROR", ["Relay reiniciando; reconecte à sala."]); ws.close(1012, "service restart"); }
		catch (_) { ws.terminate(); }
	}
	server.close(() => process.exit(0));
	setTimeout(() => process.exit(0), 25000).unref();
}

process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);
