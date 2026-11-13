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

function randomId() {
	const bytes = new Uint8Array(16);
	crypto.getRandomValues(bytes);
	return Array.from(bytes).map(function (value) { return value.toString(16).padStart(2, "0"); }).join("").toUpperCase();
}
