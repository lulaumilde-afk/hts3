# HTS online relay

This folder contains the hybrid matchmaker and fallback relay for the Unity 5 mod. It uses one Cloudflare Durable Object per room and a small directory object for public rooms. The first player to create a room is its logical owner. If that player disconnects while others remain, the relay randomly selects a connected player as the new owner and keeps the room open.

The client discovers its UDP mapping with STUN, exchanges candidates through the room WebSocket, and tries direct UDP with the logical owner, who forwards game data to the other players. Once every player confirms the path, the Worker stops forwarding movement, chat, and voice. If a player cannot establish the direct path or packets stop arriving, the room stays on the WebSocket relay until the room membership changes. The WebSocket remains connected for room membership, signaling, moderation, migration, and fallback. The matching client and online room UI are in the root `MainMenu.cs` source; movement updates are sent at 10 Hz.

## Deploy

1. Install Node.js on a computer and sign in to a Cloudflare account.
2. Open this folder in a terminal and run `npx wrangler login`.
3. Run `npx wrangler deploy`.
4. Copy the resulting `https://<worker-name>.<account-subdomain>.workers.dev` URL into the game's **Relay online** field.
5. Keep the worker URL when sharing invites. The invite page at `/join/<room-code>` displays the code; paste either that URL or the code into the game.

No `.gitignore` is required for this source. Do not put Cloudflare API tokens in this folder or commit them.

## Privacy and cost

Private rooms are omitted from `/api/rooms`, but private online rooms still use the Worker for signaling and fallback. In direct mode, room members learn each other's public UDP candidates and can see the public IP address used for the P2P connection. Direct UDP payloads are not encrypted by this mod; the room token is a packet tag, not encryption. The WebSocket provider can see connection metadata. In fallback mode, game packets pass through the Worker over the WebSocket connection and do not expose peer-to-peer addresses to other players.

The free tier is for initial trials, not an unlimited game service. Direct mode reduces Cloudflare game traffic substantially; rooms that use fallback still send movement and voice over the relay and can exceed free daily limits. Cloudflare documents current limits and billing at https://developers.cloudflare.com/durable-objects/platform/pricing/ and https://developers.cloudflare.com/workers/platform/pricing/.

The relay requires `wss://` for game connections. The game also keeps its separate LAN mode. The edited `MainMenu.cs` still needs to be pasted into dnSpy and compiled against the game's Unity 5 assemblies; Unity 5's Android TLS support should be confirmed on the target build before sharing it widely.
