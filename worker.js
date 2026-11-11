/*
 * HTS Online Multiplayer relay for Cloudflare Workers + Durable Objects.
 * The room creator starts as logical owner; if they leave, ownership moves to
 * a randomly selected connected player. Durable Objects handle room discovery,
 * signaling, and fallback relay. Direct UDP can reduce game-packet traffic, but
 * it exposes public endpoint addresses to room members and may fail behind NATs.
 */

const ROOM_ID_RE = /^[A-F0-9]{32}$/;

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
		// Último MOVE de cada jogador (id -> { move, state }). Fica só na memória: o attachment do
		// WebSocket tem limite de 2 KB e um MOVE grande (reboque, áudio) derrubava a conexão.
		this.moves = new Map();
	}

	async fetch(request) {
		if (request.headers.get("Upgrade") !== "websocket") return new Response("Expected WebSocket", { status: 426 });
		const roomId = new URL(request.url).searchParams.get("room");
		if (!roomId || !ROOM_ID_RE.test(roomId)) return new Response("Invalid room code", { status: 400 });
		const pair = new WebSocketPair();
		const client = pair[0];
		const server = pair[1];
		this.ctx.acceptWebSocket(server);
		server.serializeAttachment({ roomId: roomId, id: "", nickname: "", owner: false, joined: false, signalIp: request.headers.get("CF-Connecting-IP") || "", endpointIp: "", endpointPort: 0, p2pReady: false });
		return new Response(null, { status: 101, webSocket: client });
	}

	async webSocketMessage(ws, raw) {
		if (typeof raw !== "string" || raw.length > 20000) return this.fail(ws, "Pacote inválido.");
		const message = decodeWire(raw);
		if (!message || message.length < 1) return this.fail(ws, "Pacote inválido.");
		const peer = ws.deserializeAttachment() || {};
		if (!peer.joined) return this.join(ws, peer, message);
		if (message[0] === "P2P_READY") {
			peer.p2pReady = true;
			ws.serializeAttachment(peer);
			const room = await this.ctx.storage.get("room");
			if (!room) return;
			const peers = this.ctx.getWebSockets().map(function (socket) { return socket.deserializeAttachment() || {}; }).filter(function (state) { return state.joined; });
			if (peers.length > 1 && peers.every(function (state) { return state.p2pReady && state.endpointIp && state.endpointPort > 0; })) {
				room.directMode = true;
				await this.ctx.storage.put("room", room);
				this.broadcastJoined(encodeWire("DIRECT_MODE", ["1"]), null);
			}
			return;
		}
		if (message[0] === "P2P_FALLBACK") {
			return this.disableDirectMode(peer.roomId);
		}

		if (message[0] === "MOVE" && message.length >= 7 && message.length <= 32) {
			const room = await this.ctx.storage.get("room");
			if (room && room.directMode) return;
			const move = message.slice(1);
			if (move.some(function (value) { return value.length > 4096; })) return;
			const previousMove = this.moves.get(peer.id);
			this.moves.set(peer.id, { move: move, state: (move.length > 9 && move[9]) ? move[9] : (previousMove ? previousMove.state : "") });
			const truck = [peer.id, peer.nickname].concat(move, [peer.owner ? "1" : "0"]);
			return this.broadcast(encodeWire("TRUCK", truck), ws);
		}
		if (message[0] === "CHAT" && message.length >= 2) {
			const room = await this.ctx.storage.get("room");
			if (room && room.directMode) return;
			const text = (message[1] || "").substring(0, 500);
			return this.broadcast(encodeWire("CHAT", [peer.nickname, text]), null);
		}
		if (message[0] === "VOICE" && message.length >= 2 && message[1].length <= 4096) {
			const room = await this.ctx.storage.get("room");
			if (room && room.directMode) return;
			return this.broadcast(encodeWire("VOICE", [message[1]]), ws);
		}
		if (message[0] === "TRAFFIC" && message.length >= 3 && message.length <= 4 && peer.owner) {
			const room = await this.ctx.storage.get("room");
			if (room && room.directMode) return;
			return this.broadcast(encodeWire("TRAFFIC", message.slice(1)), ws);
		}
		if (message[0] === "ENDPOINT" && message.length >= 3) {
			const port = parseInt(message[2], 10);
			const address = message[1];
			if (!endpointAllowed(address, peer.signalIp) || !Number.isFinite(port) || port < 1 || port > 65535) return;
			peer.endpointIp = address;
			peer.endpointPort = port;
			peer.p2pReady = false;
			ws.serializeAttachment(peer);
			const room = await this.ctx.storage.get("room");
			if (room) {
				room.directMode = false;
				for (const other of this.ctx.getWebSockets()) {
					const state = other.deserializeAttachment() || {};
					if (state.joined) {
						state.p2pReady = false;
						other.serializeAttachment(state);
					}
				}
				await this.ctx.storage.put("room", room);
				this.broadcastJoined(encodeWire("DIRECT_MODE", ["0"]), null);
			}
			const endpoint = encodeWire("ENDPOINT", [peer.id, peer.nickname, address, port, peer.token, peer.lanIp || "", peer.lanPort || ""]);
			this.broadcastJoined(endpoint, ws);
			this.sendPeerEndpoints(ws);
			return;
		}
		if ((message[0] === "KICK" || message[0] === "BAN") && message.length >= 2 && peer.owner) {
			return this.moderate(peer, message[0], message[1]);
		}
		if (message[0] === "LIST") {
			const meta = await this.ctx.storage.get("room");
			if (meta) ws.send(encodeWire("SERVER_LIST", [meta.name + " " + this.ctx.getWebSockets().length + "/" + meta.capacity]));
		}
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
			meta = { id: peer.roomId, name: roomName || ("Sala de " + nickname), capacity: capacity, visibility: visibility, ownerId: "", directMode: false };
			peer.owner = true;
		} else {
			if (!meta) return this.fail(ws, "Sala encerrada ou código inválido.");
			if (this.ctx.getWebSockets().length >= meta.capacity) return this.fail(ws, "A sala está cheia.");
			const banned = await this.ctx.storage.get("banned") || [];
			if (banned.indexOf(nickname.toLowerCase()) >= 0) return this.fail(ws, "Nickname banido desta sala.");
		}

		peer.id = randomId();
		peer.token = randomId();
		peer.nickname = nickname;
		peer.joined = true;
		peer.roomId = peer.roomId || meta.id;
		peer.p2pReady = false;
		const candidateIp = create ? message[6] : message[3];
		const candidatePort = parseInt(create ? message[7] : message[4], 10);
		if (endpointAllowed(candidateIp, peer.signalIp) && Number.isFinite(candidatePort) && candidatePort > 0 && candidatePort <= 65535) {
			peer.endpointIp = candidateIp;
			peer.endpointPort = candidatePort;
		} else {
			peer.endpointIp = "";
			peer.endpointPort = 0;
		}
		// Candidato da rede local (IP privado + porta do mesmo socket UDP): permite P2P direto entre aparelhos no mesmo Wi-Fi.
		const lanIp = create ? message[8] : message[5];
		const lanPort = parseInt(create ? message[9] : message[6], 10);
		if (isPrivateIPv4(lanIp) && Number.isFinite(lanPort) && lanPort > 0 && lanPort <= 65535) {
			peer.lanIp = lanIp;
			peer.lanPort = lanPort;
		} else {
			peer.lanIp = "";
			peer.lanPort = 0;
		}
		if (peer.owner) meta.ownerId = peer.id;
		meta.directMode = false;
		for (const other of this.ctx.getWebSockets()) {
			if (other === ws) continue;
			const state = other.deserializeAttachment() || {};
			if (state.joined) {
				state.p2pReady = false;
				other.serializeAttachment(state);
			}
		}
		ws.serializeAttachment(peer);
		await this.ctx.storage.put("room", meta);
		await this.updateDirectory(meta);
		ws.send(encodeWire("WELCOME", [peer.id, "0", peer.token, peer.owner ? "1" : "0", meta.ownerId]));
		if (create) ws.send(encodeWire("ROOM_CREATED", []));
		this.sendRoster(ws);
		this.sendSnapshots(ws);
		this.sendPeerEndpoints(ws);
		if (!create) this.broadcastJoined(encodeWire("DIRECT_MODE", ["0"]), ws);
		if (!create) this.broadcast(encodeWire("PLAYER_JOINED", [peer.id, peer.nickname]), ws);
		if (peer.endpointIp && peer.endpointPort > 0) this.broadcastJoined(encodeWire("ENDPOINT", [peer.id, peer.nickname, peer.endpointIp, peer.endpointPort, peer.token, peer.lanIp || "", peer.lanPort || ""]), ws);
	}

	async webSocketClose(ws) {
		return this.removePeer(ws);
	}

	async webSocketError(ws) {
		return this.removePeer(ws);
	}

	async removePeer(ws) {
		const peer = ws.deserializeAttachment() || {};
		if (!peer.joined) return;
		const wasOwner = !!peer.owner;
		this.moves.delete(peer.id);
		peer.joined = false;
		peer.owner = false;
		try { ws.serializeAttachment(peer); } catch (_) { }
		const room = await this.ctx.storage.get("room");
		if (wasOwner) {
			const candidates = [];
			for (const other of this.ctx.getWebSockets()) {
				if (other === ws) continue;
				const state = other.deserializeAttachment() || {};
				if (state.joined) candidates.push({ socket: other, peer: state });
			}

			if (room && candidates.length > 0) {
				const selected = candidates[Math.floor(Math.random() * candidates.length)];
				room.ownerId = selected.peer.id;
				room.directMode = false;
				for (const other of this.ctx.getWebSockets()) {
					const state = other.deserializeAttachment() || {};
					if (!state.joined) continue;
					state.owner = other === selected.socket;
					state.p2pReady = false;
					try { other.serializeAttachment(state); } catch (_) { }
				}
				await this.ctx.storage.put("room", room);

				const left = encodeWire("PLAYER_LEFT", [peer.id]);
				const changed = encodeWire("HOST_CHANGED", [selected.peer.id, selected.peer.nickname, room.name, room.capacity]);
				const directMode = encodeWire("DIRECT_MODE", ["0"]);
				for (const other of this.ctx.getWebSockets()) {
					if (other === ws) continue;
					const state = other.deserializeAttachment() || {};
					if (!state.joined) continue;
					try { other.send(left); other.send(changed); other.send(directMode); } catch (_) { }
				}
				await this.updateDirectory(room);
				return;
			}

			await this.ctx.storage.delete("room");
			await this.ctx.storage.delete("banned");
			await this.updateDirectory(null, peer.roomId);
			for (const other of this.ctx.getWebSockets()) {
				if (other === ws) continue;
				try { other.send(encodeWire("ERROR", ["O host saiu e a sala não pôde ser transferida."])); other.close(1000, "room closed"); } catch (_) { }
			}
			return;
		}

		this.broadcast(encodeWire("PLAYER_LEFT", [peer.id]), ws);
		if (room) {
			await this.disableDirectMode(peer.roomId);
			await this.updateDirectory(room);
		}
	}

	async disableDirectMode(roomId) {
		const room = await this.ctx.storage.get("room");
		if (!room || room.id !== roomId) return;
		room.directMode = false;
		for (const socket of this.ctx.getWebSockets()) {
			const state = socket.deserializeAttachment() || {};
			if (state.joined) {
				state.p2pReady = false;
				socket.serializeAttachment(state);
			}
		}
		await this.ctx.storage.put("room", room);
		this.broadcastJoined(encodeWire("DIRECT_MODE", ["0"]), null);
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
			const saved = this.moves.get(peer.id);
			if (!peer.joined || !saved) continue;
			const truck = [peer.id, peer.nickname].concat(saved.move, [peer.owner ? "1" : "0"]);
			if (truck.length > 11 && saved.state) truck[11] = saved.state;
			try { target.send(encodeWire("TRUCK", truck)); } catch (_) { }
		}
	}

	sendPeerEndpoints(target) {
		for (const ws of this.ctx.getWebSockets()) {
			if (ws === target) continue;
			const peer = ws.deserializeAttachment() || {};
			if (!peer.joined || !peer.endpointIp || !peer.endpointPort) continue;
			try { target.send(encodeWire("ENDPOINT", [peer.id, peer.nickname, peer.endpointIp, peer.endpointPort, peer.token, peer.lanIp || "", peer.lanPort || ""])); } catch (_) { }
		}
	}

	broadcastJoined(line, excluded) {
		for (const ws of this.ctx.getWebSockets()) {
			if (ws === excluded) continue;
			const peer = ws.deserializeAttachment() || {};
			if (!peer.joined) continue;
			try { ws.send(line); } catch (_) { }
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
		await directory.fetch(meta ? "https://directory/upsert" : "https://directory/remove", { method: "POST", body: body });
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

// Aceita o endereço informado pelo STUN quando bate com o IP que o Cloudflare enxerga. Se o aparelho conectou por IPv6
// (comum em Wi-Fi no Brasil) não dá para comparar; nesse caso aceita qualquer IPv4 público.
function endpointAllowed(address, signalIp) {
	if (!isValidIPv4(address)) return false;
	if (address === signalIp) return true;
	return typeof signalIp === "string" && signalIp.indexOf(":") >= 0 && isPublicIPv4(address);
}

function isPublicIPv4(value) {
	if (!isValidIPv4(value)) return false;
	const o = value.split(".").map(Number);
	if (o[0] === 0 || o[0] === 10 || o[0] === 127 || o[0] >= 224) return false;
	if (o[0] === 100 && o[1] >= 64 && o[1] <= 127) return false;
	if (o[0] === 169 && o[1] === 254) return false;
	if (o[0] === 172 && o[1] >= 16 && o[1] <= 31) return false;
	if (o[0] === 192 && (o[1] === 168 || (o[1] === 0 && o[2] === 0))) return false;
	return true;
}

function isPrivateIPv4(value) {
	if (!isValidIPv4(value)) return false;
	const o = value.split(".").map(Number);
	return o[0] === 10 || (o[0] === 172 && o[1] >= 16 && o[1] <= 31) || (o[0] === 192 && o[1] === 168);
}

function isValidIPv4(value) {
	if (typeof value !== "string" || !/^(?:\d{1,3}\.){3}\d{1,3}$/.test(value)) return false;
	const octets = value.split(".");
	for (let i = 0; i < octets.length; i++) {
		const octet = Number(octets[i]);
		if (!Number.isInteger(octet) || octet < 0 || octet > 255) return false;
	}
	return true;
}
