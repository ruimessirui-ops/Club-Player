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


## Search result filtering
Club autocomplete accepts only Wikidata entities typed as association football clubs or association football teams. It does not use general Wikipedia article search as a fallback, because that can return stadiums, cities, seasons and other unrelated pages. Player autocomplete accepts only entities identified as humans and association football players. The server also checks the selected club and player before verifying the career link.

## Data sources and planned local index
The current version still checks career links against Wikidata's P54 records at verification time. A future local index can combine:
- OpenFootball player lists (CC0) for additional player names.
- Wikidata (CC0) for player identity and club-career links.
- OpenFootball club/competition data (CC0) as an additional club-name source.

These sources have different coverage: OpenFootball's player lists do not provide complete career histories, and Wikidata's career records can be incomplete. Any imported records should retain their source and entity type. Stadiums and venues must never be imported into the selectable club table.
