/*
 * HTS Online Multiplayer relay for Cloudflare Workers + Durable Objects.
 * The room creator is the logical owner. Durable Objects relay game packets
 * so phones on different networks do not need inbound ports or expose peer IPs.
 *
 * Correções desta versão:
 *  - O "attachment" do WebSocket (limite de 2 KB do Cloudflare) guarda só dados pequenos.
 *    O último MOVE/estado de cada jogador fica na memória do Durable Object. Antes, um MOVE
 *    grande (rodas do reboque, áudio, reboques extras) estourava o limite, dava exceção e
 *    derrubava a conexão, e se fosse o dono, a sala inteira fechava.
 *  - Se o dono cair, a sala NÃO fecha: o jogador mais antigo vira dono (mensagem OWNER).
 *    A sala só fecha quando não sobra ninguém.
 *  - Pacote ruim de um jogador já dentro da sala é ignorado em vez de derrubar a conexão.
 *  - MOVE é repassado sem decodificar/recodificar tudo (menos CPU a 30 pacotes por segundo).
 */

const ROOM_ID_RE = /^[A-F0-9]{32}$/;
const WIRE_CHARS_RE = /^[A-Za-z0-9+\/=\t]*$/;
const MAX_FIELD_B64 = 5600;
const B64_ONE = "MQ==";
const B64_ZERO = "MA==";
const MAX_SKIN_BYTES = 7 * 1024 * 1024;
const MAX_PLAYER_SKIN_BYTES = 12 * 1024 * 1024;
const MAX_ROOM_SKIN_BYTES = 64 * 1024 * 1024;
const SKIN_CHUNK = 1024 * 1024;
const SLOT_RE = /^[A-Za-z0-9_-]{1,48}$/;
const SKIN_PATH_RE = /^\/api\/rooms\/([A-F0-9]{32})\/skins(?:\/([A-F0-9]{32})\/([A-Za-z0-9_-]{1,48}))?$/i;

export default {
	async fetch(request, env) {
		const url = new URL(request.url);
		if (url.pathname === "/health") {
			return new Response("HTS relay online", { headers: { "content-type": "text/plain; charset=utf-8" } });
		}
		if (url.pathname === "/api/rooms" && request.method === "GET") {
			const directory = env.DIRECTORY.get(env.DIRECTORY.idFromName("directory"));
			return directory.fetch("https://directory/list");
		}
		const skinMatch = url.pathname.match(SKIN_PATH_RE);
		if (skinMatch && (request.method === "GET" || request.method === "PUT" || request.method === "DELETE")) {
			return env.ROOMS.get(env.ROOMS.idFromName(skinMatch[1].toUpperCase())).fetch(request);
		}
		const roomMatch = url.pathname.match(/^\/ws\/([A-F0-9]{32})$/i);
		if (roomMatch && request.headers.get("Upgrade") === "websocket") {
			const roomId = roomMatch[1].toUpperCase();
			const room = env.ROOMS.get(env.ROOMS.idFromName(roomId));
			const target = new URL(request.url);
			target.searchParams.set("room", roomId);
			return room.fetch(new Request(target.toString(), request));
		}
		const inviteMatch = url.pathname.match(/^\/join\/([A-F0-9]{32})$/i);
		if (inviteMatch && request.method === "GET") {
			const roomId = inviteMatch[1].toUpperCase();
			const origin = url.origin;
			const page = "<!doctype html><meta name=viewport content='width=device-width,initial-scale=1'>" +
				"<title>Convite HTS</title><main style='font:16px system-ui;max-width:34em;margin:12vh auto;padding:1em'>" +
				"<h2>Sala do Heavy Truck Simulator</h2><p>Copie o código abaixo e cole na opção Multiplayer do jogo.</p>" +
				"<code id=c style='word-break:break-all'>" + roomId + "</code> " +
				"<button onclick=\"navigator.clipboard.writeText(document.querySelector('#c').textContent)\">Copiar código</button>" +
				"<p>Relay: " + origin + "</p></main>";
			return new Response(page, { headers: { "content-type": "text/html; charset=utf-8", "cache-control": "no-store" } });
		}
		return new Response("Not found", { status: 404 });
	}
};

export class Room {
	constructor(ctx, env) {
		this.ctx = ctx;
		this.env = env;
		// Último MOVE de cada jogador (id -> { fields, state }). Só em memória: não cabe no attachment (2 KB).
		this.moves = new Map();
		try {
			// Responde "ping" -> "pong" sem acordar o objeto (útil se o cliente passar a mandar ping).
			this.ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair("ping", "pong"));
		} catch (_) { }
	}

	async fetch(request) {
		if (SKIN_PATH_RE.test(new URL(request.url).pathname)) return this.handleSkin(request);
		if (request.headers.get("Upgrade") !== "websocket") return new Response("Expected WebSocket", { status: 426 });
		const roomId = new URL(request.url).searchParams.get("room");
		if (!roomId || !ROOM_ID_RE.test(roomId)) return new Response("Invalid room code", { status: 400 });
		const pair = new WebSocketPair();
		const client = pair[0];
		const server = pair[1];
		this.ctx.acceptWebSocket(server);
		server.serializeAttachment({ roomId: roomId, id: "", nickname: "", token: "", owner: false, joined: false, joinedAt: 0 });
		return new Response(null, { status: 101, webSocket: client });
	}

	async webSocketMessage(ws, raw) {
		const peer = ws.deserializeAttachment() || {};
		if (typeof raw !== "string" || raw.length > 20000) {
			if (!peer.joined) return this.fail(ws, "Pacote inválido.");
			return;
		}
		// Caminho rápido: MOVE de quem já está na sala é repassado sem decodificar.
		if (peer.joined && raw.startsWith("MOVE\t")) return this.relayMove(ws, peer, raw);

		const message = decodeWire(raw);
		if (!message || message.length < 1) {
			if (!peer.joined) return this.fail(ws, "Pacote inválido.");
			return;
		}
		if (!peer.joined) return this.join(ws, peer, message);

		if (message[0] === "CHAT" && message.length >= 2) {
			const text = (message[1] || "").substring(0, 500);
			return this.broadcast(encodeWire("CHAT", [peer.nickname, text]), null);
		}
		if (message[0] === "VOICE" && message.length >= 2 && message[1].length <= 4096) {
			return this.broadcast(encodeWire("VOICE", [message[1]]), ws);
		}
		if ((message[0] === "KICK" || message[0] === "BAN") && message.length >= 2 && peer.owner) {
			return this.moderate(peer, message[0], message[1]);
		}
		if (message[0] === "SKIN_LIST") return this.sendSkinManifest(ws);
		if (message[0] === "SKIN_DELETE" && message.length >= 2 && SLOT_RE.test(message[1])) return this.removeSkin(peer.id, message[1], null);
		if (message[0] === "LIST") {
			const meta = await this.ctx.storage.get("room");
			if (meta) ws.send(encodeWire("SERVER_LIST", [meta.name + " " + this.joinedCount() + "/" + meta.capacity]));
		}
	}

	relayMove(ws, peer, raw) {
		if (!WIRE_CHARS_RE.test(raw)) return;
		const parts = raw.split("\t");
		if (parts.length < 7 || parts.length > 32) return;
		for (let i = 1; i < parts.length; i++) {
			if (parts[i].length > MAX_FIELD_B64) return;
		}
		const fields = parts.slice(1);
		const previous = this.moves.get(peer.id);
		const state = fields.length > 9 && fields[9] ? fields[9] : (previous ? previous.state : "");
		this.moves.set(peer.id, { fields: fields, state: state });
		this.broadcast(this.truckLine(peer, fields), ws);
	}

	truckLine(peer, fields) {
		return "TRUCK\t" + b64(peer.id) + "\t" + b64(peer.nickname) + "\t" + fields.join("\t") + "\t" + (peer.owner ? B64_ONE : B64_ZERO);
	}

	async join(ws, peer, message) {
		const command = message[0];
		const create = command === "CREATE";
		if (!create && command !== "JOIN") return this.fail(ws, "Envie CREATE ou JOIN primeiro.");
		const nickname = (message[1] || "").trim();
		if (!nickname || nickname.length > 24) return this.fail(ws, "Nickname inválido.");
		let meta = await this.ctx.storage.get("room");
		if (create) {
			if (meta) return this.fail(ws, "Este código de sala já está em uso.");
			const capacity = parseInt(message[3], 10);
			const roomName = (message[4] || ("Sala de " + nickname)).trim().substring(0, 40);
			const visibility = message[5] === "public" ? "public" : "private";
			if (!Number.isFinite(capacity) || capacity < 2 || capacity > 64) return this.fail(ws, "Capacidade inválida (2 a 64).");
			meta = { id: peer.roomId, name: roomName || ("Sala de " + nickname), capacity: capacity, visibility: visibility, ownerId: "" };
			peer.owner = true;
		} else {
			if (!meta) return this.fail(ws, "Sala encerrada ou código inválido.");
			if (this.joinedCount() >= meta.capacity) return this.fail(ws, "A sala está cheia.");
			const banned = await this.ctx.storage.get("banned") || [];
			if (banned.indexOf(nickname.toLowerCase()) >= 0) return this.fail(ws, "Nickname banido desta sala.");
		}

		peer.id = randomId();
		peer.token = randomId();
		peer.nickname = nickname;
		peer.joined = true;
		peer.joinedAt = Date.now();
		peer.roomId = peer.roomId || meta.id;
		if (peer.owner) meta.ownerId = peer.id;
		ws.serializeAttachment(peer);
		await this.ctx.storage.put("room", meta);
		await this.updateDirectory(meta);
		ws.send(encodeWire("WELCOME", [peer.id, "0", peer.token, peer.owner ? "1" : "0"]));
		if (create) ws.send(encodeWire("ROOM_CREATED", []));
		this.sendRoster(ws);
		this.sendSnapshots(ws);
		if (!create) this.broadcast(encodeWire("PLAYER_JOINED", [peer.id, peer.nickname]), ws);
	}

	async webSocketClose(ws) {
		await this.removePeer(ws);
		try { ws.close(1000, "closing"); } catch (_) { }
	}

	async webSocketError(ws) {
		return this.removePeer(ws);
	}

	async removePeer(ws) {
		const peer = ws.deserializeAttachment() || {};
		if (!peer.joined) return;
		this.moves.delete(peer.id);
		peer.joined = false;
		try { ws.serializeAttachment(peer); } catch (_) { }
		await this.removeAllSkins(peer.id, ws);

		const room = await this.ctx.storage.get("room");
		const others = [];
		for (const other of this.ctx.getWebSockets()) {
			if (other === ws) continue;
			if (other.readyState === 2 || other.readyState === 3) continue;
			const otherPeer = other.deserializeAttachment() || {};
			if (otherPeer.joined && otherPeer.id !== peer.id) others.push({ ws: other, peer: otherPeer });
		}

		if (peer.owner) {
			if (!room || others.length === 0) {
				// Ninguém sobrou: aí sim a sala fecha.
				await this.clearAllSkins();
				await this.ctx.storage.delete("room");
				await this.ctx.storage.delete("banned");
				await this.updateDirectory(null, peer.roomId);
				return;
			}
			// O dono caiu, mas tem gente na sala: o jogador mais antigo vira o novo dono.
			others.sort(function (a, b) { return (a.peer.joinedAt || 0) - (b.peer.joinedAt || 0); });
			const next = others[0];
			next.peer.owner = true;
			try { next.ws.serializeAttachment(next.peer); } catch (_) { }
			room.ownerId = next.peer.id;
			await this.ctx.storage.put("room", room);
			this.broadcast(encodeWire("PLAYER_LEFT", [peer.id]), ws);
			this.broadcast(encodeWire("HOST_CHANGED", [next.peer.id, next.peer.nickname, room.name, String(room.capacity)]), ws);
			this.broadcast(encodeWire("DIRECT_MODE", ["0"]), ws);
			await this.updateDirectory(room);
			return;
		}
		this.broadcast(encodeWire("PLAYER_LEFT", [peer.id]), ws);
		if (room) await this.updateDirectory(room);
	}

	async moderate(owner, command, targetId) {
		for (const ws of this.ctx.getWebSockets()) {
			const target = ws.deserializeAttachment() || {};
			if (target.id !== targetId || target.owner) continue;
			if (command === "BAN") {
				const banned = await this.ctx.storage.get("banned") || [];
				if (banned.indexOf(target.nickname.toLowerCase()) < 0) banned.push(target.nickname.toLowerCase());
				await this.ctx.storage.put("banned", banned);
			}
			try { ws.send(encodeWire("ERROR", [command === "BAN" ? "Banido pelo dono da sala." : "Expulso pelo dono da sala."])); ws.close(1008, command); } catch (_) { }
		}
	}

	// ---------- Skins (guardadas no storage do Durable Object, em pedaços de 1 MB) ----------
	skinChunkKeys(key, size) {
		const keys = [];
		const count = Math.max(1, Math.ceil(size / SKIN_CHUNK));
		for (let i = 0; i < count; i++) keys.push("skinbin:" + key + ":" + i);
		return keys;
	}

	skinResponse(status, text, type) {
		return new Response(text, { status: status, headers: { "content-type": type || "text/plain; charset=utf-8", "cache-control": "no-store" } });
	}

	async handleSkin(request) {
		const url = new URL(request.url);
		const m = url.pathname.match(SKIN_PATH_RE);
		if (!m) return this.skinResponse(404, "Not found");
		const targetPlayer = m[2] ? m[2].toUpperCase() : "";
		const slot = m[3] || "";
		const playerId = request.headers.get("x-hts-player") || "";
		const token = request.headers.get("x-hts-token") || "";
		let authed = false;
		if (playerId && token) {
			for (const w of this.ctx.getWebSockets()) {
				const p = w.deserializeAttachment() || {};
				if (p.joined && p.id === playerId && p.token === token) { authed = true; break; }
			}
		}
		if (!authed) return this.skinResponse(401, "Unauthorized");

		let index = (await this.ctx.storage.get("skins")) || {};
		if (!targetPlayer || !slot) {
			if (request.method !== "GET") return this.skinResponse(400, "Bad request");
			const manifest = Object.values(index).map(function (e) { return { playerId: e.playerId, slot: e.slot, hash: e.hash, size: e.size, type: e.type, model: e.model }; });
			return this.skinResponse(200, JSON.stringify(manifest), "application/json; charset=utf-8");
		}
		const key = targetPlayer + "|" + slot;

		if (request.method === "DELETE") {
			if (targetPlayer !== playerId) return this.skinResponse(403, "Forbidden");
			await this.removeSkin(playerId, slot, null);
			return this.skinResponse(200, "deleted");
		}

		if (request.method === "PUT") {
			if (targetPlayer !== playerId) return this.skinResponse(403, "Forbidden");
			const type = (request.headers.get("content-type") || "").split(";")[0].trim().toLowerCase();
			if (type !== "image/png" && type !== "image/jpeg") return this.skinResponse(415, "Only PNG/JPEG skins are accepted");
			if (Number(request.headers.get("content-length") || 0) > MAX_SKIN_BYTES) return this.skinResponse(413, "Skin exceeds 7 MB");
			let bytes;
			try { bytes = await readBodyLimited(request, MAX_SKIN_BYTES); }
			catch (e) { return this.skinResponse(e && e.status ? e.status : 400, (e && e.message) || "Upload failed"); }
			if (bytes.length < 16 || bytes.length > MAX_SKIN_BYTES) return this.skinResponse(413, "Invalid skin size");
			const validPng = bytes.length >= 8 && bytes[0] === 0x89 && bytes[1] === 0x50 && bytes[2] === 0x4e && bytes[3] === 0x47 && bytes[4] === 0x0d && bytes[5] === 0x0a && bytes[6] === 0x1a && bytes[7] === 0x0a;
			const validJpeg = bytes[0] === 0xff && bytes[1] === 0xd8 && bytes[2] === 0xff;
			if ((type === "image/png" && !validPng) || (type === "image/jpeg" && !validJpeg)) return this.skinResponse(415, "Image signature does not match Content-Type");
			const model = (request.headers.get("x-hts-model") || "").replace(/[^A-Za-z0-9 _().-]/g, "_").slice(0, 64);
			const hash = await sha256Hex(bytes);

			index = (await this.ctx.storage.get("skins")) || {};
			const old = index[key];
			let playerBytes = 0, roomBytes = 0;
			for (const e of Object.values(index)) { roomBytes += e.size; if (e.playerId === playerId) playerBytes += e.size; }
			const nextPlayer = playerBytes - (old ? old.size : 0) + bytes.length;
			const nextRoom = roomBytes - (old ? old.size : 0) + bytes.length;
			if (nextPlayer > MAX_PLAYER_SKIN_BYTES) return this.skinResponse(413, "Player skin cache is limited to 12 MB");
			if (nextRoom > MAX_ROOM_SKIN_BYTES) return this.skinResponse(413, "Room skin cache is full");

			if (!old || old.hash !== hash || old.model !== model || old.type !== type) {
				if (old) await this.ctx.storage.delete(this.skinChunkKeys(key, old.size));
				const puts = {};
				const keys = this.skinChunkKeys(key, bytes.length);
				for (let i = 0; i < keys.length; i++) puts[keys[i]] = bytes.slice(i * SKIN_CHUNK, Math.min(bytes.length, (i + 1) * SKIN_CHUNK));
				await this.ctx.storage.put(puts);
				index = (await this.ctx.storage.get("skins")) || {};
				index[key] = { playerId: playerId, slot: slot, hash: hash, size: bytes.length, type: type, model: model };
				await this.ctx.storage.put("skins", index);
				this.broadcast(encodeWire("SKIN_AVAILABLE", [playerId, slot, hash, bytes.length, type, model]), null);
			}
			return this.skinResponse(200, JSON.stringify({ hash: hash, size: bytes.length }), "application/json; charset=utf-8");
		}

		if (request.method !== "GET") return this.skinResponse(405, "Method not allowed");
		const stored = index[key];
		if (!stored) return this.skinResponse(404, "Not found");
		const keys = this.skinChunkKeys(key, stored.size);
		const parts = await this.ctx.storage.get(keys);
		const out = new Uint8Array(stored.size);
		let offset = 0;
		for (const k of keys) {
			const part = parts.get(k);
			if (!part) return this.skinResponse(404, "Not found");
			const u8 = part instanceof Uint8Array ? part : new Uint8Array(part);
			out.set(u8, offset);
			offset += u8.length;
		}
		return new Response(out, { status: 200, headers: { "content-type": stored.type, "x-hts-sha256": stored.hash, "cache-control": "no-store", "x-content-type-options": "nosniff" } });
	}

	async sendSkinManifest(ws) {
		const index = (await this.ctx.storage.get("skins")) || {};
		for (const e of Object.values(index)) {
			try { ws.send(encodeWire("SKIN_AVAILABLE", [e.playerId, e.slot, e.hash, e.size, e.type, e.model])); } catch (_) { }
		}
	}

	async removeSkin(playerId, slot, excluded) {
		const key = playerId + "|" + slot;
		const index = (await this.ctx.storage.get("skins")) || {};
		const old = index[key];
		if (!old) return;
		delete index[key];
		await this.ctx.storage.delete(this.skinChunkKeys(key, old.size));
		await this.ctx.storage.put("skins", index);
		this.broadcast(encodeWire("SKIN_REMOVED", [playerId, slot]), excluded || null);
	}

	async removeAllSkins(playerId, excluded) {
		const index = (await this.ctx.storage.get("skins")) || {};
		let changed = false;
		for (const key of Object.keys(index)) {
			const e = index[key];
			if (e.playerId !== playerId) continue;
			delete index[key];
			changed = true;
			await this.ctx.storage.delete(this.skinChunkKeys(key, e.size));
			this.broadcast(encodeWire("SKIN_REMOVED", [e.playerId, e.slot]), excluded || null);
		}
		if (changed) await this.ctx.storage.put("skins", index);
	}

	async clearAllSkins() {
		const index = (await this.ctx.storage.get("skins")) || {};
		for (const key of Object.keys(index)) await this.ctx.storage.delete(this.skinChunkKeys(key, index[key].size));
		await this.ctx.storage.delete("skins");
	}

	joinedCount() {
		let n = 0;
		for (const w of this.ctx.getWebSockets()) {
			if (w.readyState === 2 || w.readyState === 3) continue;
			const p = w.deserializeAttachment() || {};
			if (p.joined) n++;
		}
		return n;
	}

	sendRoster(target) {
		const fields = [];
		for (const ws of this.ctx.getWebSockets()) {
			const peer = ws.deserializeAttachment() || {};
			if (peer.joined) fields.push(peer.id, peer.nickname);
		}
		target.send(encodeWire("ROSTER", fields));
	}

	sendSnapshots(target) {
		for (const ws of this.ctx.getWebSockets()) {
			if (ws === target) continue;
			const peer = ws.deserializeAttachment() || {};
			if (!peer.joined) continue;
			const entry = this.moves.get(peer.id);
			if (!entry) continue;
			const fields = entry.fields.slice();
			if (fields.length > 9 && entry.state) fields[9] = entry.state;
			try { target.send(this.truckLine(peer, fields)); } catch (_) { }
		}
	}

	broadcast(line, excluded) {
		for (const ws of this.ctx.getWebSockets()) {
			if (ws === excluded) continue;
			try { ws.send(line); } catch (_) { }
		}
	}

	fail(ws, text) {
		try { ws.send(encodeWire("ERROR", [text])); ws.close(1008, "rejected"); } catch (_) { }
	}

	async updateDirectory(meta, removeId) {
		const directory = this.env.DIRECTORY.get(this.env.DIRECTORY.idFromName("directory"));
		const body = meta ? JSON.stringify({ id: meta.id, name: meta.name, capacity: meta.capacity, count: this.ctx.getWebSockets().filter(function (ws) { const peer = ws.deserializeAttachment() || {}; return peer.joined; }).length, visibility: meta.visibility }) : JSON.stringify({ id: removeId });
		try {
			await directory.fetch(meta ? "https://directory/upsert" : "https://directory/remove", { method: "POST", body: body });
		} catch (_) { }
	}
}

export class Directory {
	constructor(ctx, env) {
		this.ctx = ctx;
		this.env = env;
	}

	async fetch(request) {
		const url = new URL(request.url);
		if (request.method === "GET" && url.pathname === "/list") {
			const values = await this.ctx.storage.list({ prefix: "room:" });
			const lines = [];
			for (const entry of values.values()) {
				const room = entry;
				if (room.visibility !== "public" || room.count < 1) continue;
				lines.push([b64(room.id), b64(room.name), room.count, room.capacity].join("|"));
			}
			return new Response(lines.join("\n"), { headers: { "content-type": "text/plain; charset=utf-8", "cache-control": "no-store" } });
		}
		if (request.method === "POST" && (url.pathname === "/upsert" || url.pathname === "/remove")) {
			let body;
			try { body = await request.json(); } catch (_) { return new Response("Bad request", { status: 400 }); }
			if (!body || !ROOM_ID_RE.test(body.id || "")) return new Response("Bad room id", { status: 400 });
			if (url.pathname === "/remove") await this.ctx.storage.delete("room:" + body.id);
			else if (body.visibility === "public" && body.count > 0) await this.ctx.storage.put("room:" + body.id, body);
			else await this.ctx.storage.delete("room:" + body.id);
			return new Response("ok");
		}
		return new Response("Not found", { status: 404 });
	}
}

function decodeWire(line) {
	try {
		const parts = line.split("\t");
		return [parts[0]].concat(parts.slice(1).map(function (part) { return fromB64(part); }));
	} catch (_) { return null; }
}

function encodeWire(command, fields) {
	return [command].concat((fields || []).map(function (value) { return b64(value == null ? "" : String(value)); })).join("\t");
}

function b64(text) {
	const bytes = new TextEncoder().encode(String(text));
	let binary = "";
	for (let i = 0; i < bytes.length; i++) binary += String.fromCharCode(bytes[i]);
	return btoa(binary);
}

function fromB64(value) {
	const binary = atob(value);
	const bytes = new Uint8Array(binary.length);
	for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
	return new TextDecoder().decode(bytes);
}

async function readBodyLimited(request, max) {
	if (!request.body) return new Uint8Array(0);
	const reader = request.body.getReader();
	const chunks = [];
	let total = 0;
	for (;;) {
		const r = await reader.read();
		if (r.done) break;
		total += r.value.length;
		if (total > max) { try { await reader.cancel(); } catch (_) { } const err = new Error("Skin exceeds 7 MB"); err.status = 413; throw err; }
		chunks.push(r.value);
	}
	const out = new Uint8Array(total);
	let offset = 0;
	for (const c of chunks) { out.set(c, offset); offset += c.length; }
	return out;
}

async function sha256Hex(bytes) {
	const digest = new Uint8Array(await crypto.subtle.digest("SHA-256", bytes));
	return Array.from(digest).map(function (v) { return v.toString(16).padStart(2, "0"); }).join("");
}

function randomId() {
	const bytes = new Uint8Array(16);
	crypto.getRandomValues(bytes);
	return Array.from(bytes).map(function (value) { return value.toString(16).padStart(2, "0"); }).join("").toUpperCase();
}
