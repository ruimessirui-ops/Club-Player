# Club Player

A browser-based, online multiplayer football club/player chain game.

## Run locally
1. Install Node.js 18 or newer.
2. Open a terminal in this folder.
3. Run `npm install`
4. Run `npm start`
5. Open `http://localhost:3000`

For friends on different networks/computers, deploy this Node app to a host that supports WebSockets (for example Render, Railway, Fly.io, or a VPS). Share the deployed URL and room code. A static-only host such as GitHub Pages is not enough because the game needs a live server.

## Game flow
- Host creates a room; friends join with the room code.
- 2–8 players; host starts.
- First turn: choose a starting club.
- Next turn: name a player who played for that club.
- Next turn: name another club that player played for.
- Then alternate player / club answers.
- No repeated players or clubs. A player who cannot continue can choose “I'm out”.
- Last remaining player wins.

## Verification
The server checks club/player links using Wikidata's `P54` (member of sports team) records. Wikidata is community-maintained and incomplete, so it may reject genuine career links or fail to resolve some clubs/players. This is an initial automatic-verification implementation, not a complete football database.

## Important
The game server stores rooms in memory. Restarting/redeploying the server clears active games. Use a persistent production service for longer-running sessions.
