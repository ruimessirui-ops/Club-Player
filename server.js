const express = require("express");
const http = require("http");
const { WebSocketServer } = require("ws");
const crypto = require("crypto");

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });
app.use(express.static("public"));
app.get("/health", (_, res) => res.json({ ok: true }));
app.get("/api/search", async (req, res) => {
  const q = clean(req.query.q);
  const type = req.query.type === "club" ? "club" : "player";
  if (q.length < 3) return res.json([]);
  const cacheKey = "football:" + type + ":" + q.toLowerCase();
  const cached = searchCache.get(cacheKey);
  if (cached) return res.json(cached);
  try {
    let items = [];
    if (process.env.API_FOOTBALL_KEY) {
      try {
        const data = type === "club"
          ? await apiFootballGet("/teams", { search: q })
          : await apiFootballGet("/players", { search: q });
        items = (data.response || []).slice(0, 10).map(item => {
          const entry = type === "club" ? item.team : item.player;
          return entry ? {
            id: String(entry.id),
            label: entry.name,
            description: type === "club" ? "Football club" : "Football player",
            source: "API-Football"
          } : null;
        }).filter(Boolean);
      } catch (apiError) {
        console.error("API-Football search failed:", apiError.message);
      }
    }
    // Keep the previous sources as a backup if API-Football has no result.
    if (!items.length) {
      try {
        const results = await wikidataSearch(q);
        const relevant = results.filter(item => {
          const d = (item.description || "").toLowerCase();
          return type === "club"
            ? /football|soccer|sports club|sport club|football team|soccer team/.test(d)
            : /football|soccer/.test(d);
        });
        items = [...relevant, ...results.filter(item => !relevant.includes(item))]
          .slice(0, 10).map(item => ({
            id: item.id, label: item.label, description: item.description || "", source: "Wikidata"
          }));
        if (!items.length) items = await wikipediaSearch(q, type);
      } catch (error) {
        items = await wikipediaSearch(q, type);
      }
    }
    searchCache.set(cacheKey, items);
    if (searchCache.size > 1000) searchCache.delete(searchCache.keys().next().value);
    return res.json(items);
  } catch (err) {
    return res.status(503).json({ error: "Football search is temporarily unavailable. Try again shortly." });
  }
});

const rooms = new Map();
const searchCache = new Map();
const clean = s => String(s || "").trim().replace(/\s+/g, " ").slice(0, 60);
const roomCode = () => crypto.randomBytes(3).toString("hex").toUpperCase();

function publicState(room) {
  return {
    code: room.code, status: room.status, players: room.players.map(p => ({id:p.id,name:p.name,connected:p.connected,alive:p.alive,hearts:p.hearts})),
    turn: room.turn, phase: room.phase, currentClub: room.currentClub, lastPlayer: room.lastPlayer,
    usedClubs: room.usedClubs, usedPlayers: room.usedPlayers, history: room.history,
    message: room.message, winner: room.winner
  };
}
function broadcast(room) {
  const msg = JSON.stringify({ type:"state", state:publicState(room) });
  for (const p of room.players) if (p.ws && p.ws.readyState === 1) p.ws.send(msg);
}
function send(ws, type, data={}) { if(ws.readyState===1) ws.send(JSON.stringify({type,...data})); }
function alive(room) { return room.players.filter(p=>p.alive); }
function current(room) { return room.players[room.turn % room.players.length]; }
function advance(room) {
  if (alive(room).length <= 1) {
    room.status="finished"; room.winner=alive(room)[0]?.name || "No winner"; room.message=`${room.winner} wins!`; return;
  }
  let tries=0;
  do { room.turn=(room.turn+1)%room.players.length; tries++; }
  while ((!room.players[room.turn].alive || !room.players[room.turn].connected) && tries <= room.players.length);
}
function normalizeName(s) { return clean(s).toLocaleLowerCase("en"); }
function handleWrongAnswer(room, player, ws, err) {
  const message = err?.message || "That answer could not be verified.";
  const temporary = /could not be reached|temporarily busy|temporarily unavailable|network|timed out|timeout|fetch failed/i.test(message);
  if (!temporary) {
    player.hearts = Math.max(0, (player.hearts ?? 2) - 1);
    if (player.hearts === 0) {
      player.alive = false;
      room.message = player.name + " is out of hearts and has been eliminated.";
      if (alive(room).length === 2) { room.phase = "startClub"; room.currentClub = null; room.lastPlayer = null; }
      advance(room);
    } else room.message = player.name + " gave an incorrect answer and lost a heart.";
  } else room.message = "The football database is having trouble. No heart was lost.";
  broadcast(room);
  send(ws, "error", { message: temporary ? message : message + " (" + player.hearts + " hearts left.)" });
}

// Resolve names with Wikidata search, then check football career club links (P54).
// Cache successful lookups and retry temporary Wikidata failures to reduce rate-limit issues.
const wikidataSearchCache = new Map();
const entityCache = new Map();
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));

async function wikidataFetch(url) {
  let lastError;
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      const r = await fetch(url, {
        headers: { "User-Agent": "ClubPlayerGame/1.0 (football club player game)" },
        signal: AbortSignal.timeout(10000)
      });
      if (r.ok) return r;
      if (r.status !== 429 && r.status < 500) {
        throw new Error("Wikidata rejected the lookup (HTTP " + r.status + ").");
      }
      lastError = new Error("Wikidata is temporarily busy (HTTP " + r.status + ").");
    } catch (err) {
      lastError = err;
    }
    if (attempt < 2) await delay(400 * (attempt + 1));
  }
  throw new Error("Wikidata could not be reached after 3 attempts. Please try again in a few seconds.");
}

async function wikidataSearch(name) {
  const key = normalizeName(name);
  if (wikidataSearchCache.has(key)) return wikidataSearchCache.get(key);
  const url = "https://www.wikidata.org/w/api.php?action=wbsearchentities&search="+encodeURIComponent(name)+"&language=en&format=json&limit=30&origin=*";
  const r = await wikidataFetch(url);
  const j = await r.json();
  const results = (j.search || []).slice(0,30);
  wikidataSearchCache.set(key, results);
  if (wikidataSearchCache.size > 500) wikidataSearchCache.delete(wikidataSearchCache.keys().next().value);
  return results;
}
async function wikipediaSearch(name, type) {
  const topic = type === "club" ? " football club" : " footballer";
  const url = "https://en.wikipedia.org/w/api.php?action=query&list=search&srsearch=" +
    encodeURIComponent(name + topic) + "&srlimit=10&format=json&origin=*";
  const r = await fetch(url, {
    headers: { "User-Agent": "ClubPlayerGame/1.0 (football club player game)" },
    signal: AbortSignal.timeout(8000)
  });
  if (!r.ok) throw new Error("Wikipedia search unavailable.");
  const data = await r.json();
  return (data.query?.search || []).slice(0, 10).map(item => ({
    id: null,
    label: item.title,
    description: (item.snippet || "").replace(/<[^>]*>/g, "").replace(/&quot;/g, '"').replace(/&#039;/g, "'"),
    source: "Wikipedia"
  }));
}
async function entity(id) {
  if (entityCache.has(id)) return entityCache.get(id);
  const r = await wikidataFetch("https://www.wikidata.org/wiki/Special:EntityData/" + id + ".json");
  const j = await r.json();
  const result = j.entities[id];
  if (!result) throw new Error("Wikidata returned no details for that entry. Please try again.");
  entityCache.set(id, result);
  if (entityCache.size > 500) entityCache.delete(entityCache.keys().next().value);
  return result;
}
function labels(e) {
  const out=[e.labels?.en?.value, ...(e.aliases?.en||[]).map(x=>x.value)];
  return out.filter(Boolean).map(normalizeName);
}
async function apiFootballGet(path, params = {}) {
  const key = process.env.API_FOOTBALL_KEY;
  if (!key) throw new Error("API_FOOTBALL_KEY is not configured in Render.");
  const url = new URL("https://v3.football.api-sports.io" + path);
  for (const [name, value] of Object.entries(params)) url.searchParams.set(name, String(value));
  const response = await fetch(url, {
    headers: { "x-apisports-key": key },
    signal: AbortSignal.timeout(10000)
  });
  if (!response.ok) throw new Error("API-Football returned HTTP " + response.status + ".");
  const data = await response.json();
  if (data.errors && Object.keys(data.errors).length) {
    throw new Error("API-Football: " + JSON.stringify(data.errors));
  }
  return data;
}

async function resolveFootball(name, type) {
  if (process.env.API_FOOTBALL_KEY) {
    const data = type === "club"
      ? await apiFootballGet("/teams", { search: name })
      : await apiFootballGet("/players", { search: name });
    const rows = data.response || [];
    const entry = rows.find(row => {
      const candidate = type === "club" ? row.team?.name : row.player?.name;
      return candidate && normalizeName(candidate) === normalizeName(name);
    }) || rows[0];
    const entityData = type === "club" ? entry?.team : entry?.player;
    if (entityData?.id) return { id: entityData.id, name: entityData.name, source: "API-Football" };
    throw new Error('I could not find "' + name + '" in the football database. Check the spelling or try the full name.');
  }
  const results = await wikidataSearch(name);
  const exact = results.find(x => normalizeName(x.label) === normalizeName(name)) || results[0];
  if (!exact) throw new Error('I could not find "' + name + '" in the football database.');
  return { id: exact.id, name: exact.label, source: "Wikidata", entity: await entity(exact.id) };
}

async function resolve(name) {
  return resolveFootball(name, "club");
}

async function verifyLink(clubName, playerName) {
  if (process.env.API_FOOTBALL_KEY) {
    const [club, player] = await Promise.all([
      resolveFootball(clubName, "club"),
      resolveFootball(playerName, "player")
    ]);
    const data = await apiFootballGet("/players/teams", { player: player.id });
    const teams = (data.response || []).flatMap(row => {
      if (row.team) return [row.team];
      return row.teams || [];
    });
    const found = teams.some(team =>
      String(team.id) === String(club.id) ||
      normalizeName(team.name || "") === normalizeName(club.name)
    );
    if (!found) {
      throw new Error(player.name + " is not listed as having played for " + club.name + " in API-Football. The career data may be incomplete.");
    }
    return { club: club.name, player: player.name, clubId: club.id, playerId: player.id };
  }

  const [club, player] = await Promise.all([resolveFootball(clubName, "club"), resolveFootball(playerName, "player")]);
  const clubClaims = player.entity?.claims?.P54 || [];
  const playedFor = clubClaims.map(x => x.mainsnak?.datavalue?.value?.id).filter(Boolean);
  if (!playedFor.includes(club.id)) {
    throw new Error(player.name + " is not listed as having played for " + club.name + " in Wikidata. The database can be incomplete.");
  }
  return { club: club.name, player: player.name, clubId: club.id, playerId: player.id };
}
async function verifyPlayerForClub(playerName, clubName) {
  return verifyLink(clubName, playerName);
}
async function verifyClubForPlayer(clubName, playerName) {
  return verifyLink(clubName, playerName);
}

wss.on("connection", ws => {
  ws.on("message", async raw => {
    let m; try { m=JSON.parse(raw.toString()); } catch { return; }
    try {
      if(m.type==="create") {
        const name=clean(m.name)||"Player 1"; const code=roomCode();
        const p={id:crypto.randomUUID(),name,ws,connected:true,alive:true,hearts:2};
        const room={code,players:[p],status:"lobby",turn:0,phase:"startClub",currentClub:null,lastPlayer:null,usedClubs:[],usedPlayers:[],history:[],message:"Waiting for players to join.",winner:null};
        rooms.set(code,room); ws.room=code; ws.player=p.id; send(ws,"joined",{id:p.id,code}); broadcast(room);
      }
      if(m.type==="join") {
        const code=clean(m.code).toUpperCase(), room=rooms.get(code);
        if(!room) return send(ws,"error",{message:"Room not found. Check the code."});
        if(room.status!=="lobby") return send(ws,"error",{message:"This game has already started."});
        if(room.players.length>=8) return send(ws,"error",{message:"This room is full (maximum 8 players)."});
        const p={id:crypto.randomUUID(),name:clean(m.name)||"Player",ws,connected:true,alive:true,hearts:2};
        room.players.push(p); ws.room=code; ws.player=p.id; send(ws,"joined",{id:p.id,code}); room.message=`${p.name} joined the room.`; broadcast(room);
      }
      if(m.type==="start") {
        const room=rooms.get(ws.room); if(!room) return;
        if(room.players[0].id!==ws.player) return send(ws,"error",{message:"Only the host can start the game."});
        if(room.players.length<2) return send(ws,"error",{message:"At least 2 players are needed."});
        room.status="playing"; room.phase="startClub"; room.turn=0; room.message=`${room.players[0].name}, choose the starting club.`; broadcast(room);
      }
      if(m.type==="move") {
        const room=rooms.get(ws.room); if(!room || room.status!=="playing") return;
        const p=room.players.find(x=>x.id===ws.player);
        if(!p || !p.alive || current(room)?.id!==p.id) return send(ws,"error",{message:"It's not your turn."});
        const value=clean(m.value); if(!value) return send(ws,"error",{message:"Enter a club or player name."});
        if(room.phase==="startClub") {
          try {
            const e=await resolve(value);
            const desc=(e.descriptions?.en?.value||"").toLowerCase();
            if(!desc.includes("football club") && !desc.includes("association football club") && !desc.includes("sports club")) {
              // Accept a team entity with a sports-team instance or a club description.
              if(!(e.claims?.P31||[]).some(c=>["Q476028","Q847017","Q12973014"].includes(c.mainsnak?.datavalue?.value?.id))) throw new Error("That doesn't look like a football club. Try a club name such as Dortmund or Juventus.");
            }
            const canon=e.labels?.en?.value||value;
            if(room.usedClubs.some(x=>x.id===e.id)) throw new Error("That club has already been used.");
            room.currentClub=canon; room.usedClubs.push({id:e.id,name:canon});
            room.history.push({kind:"club",name:canon,by:p.name});
            room.phase="player"; room.message=`${canon} selected. ${p.name}, name a player who played for this club.`;
          } catch(err) { return handleWrongAnswer(room,p,ws,err); }
        } else if(room.phase==="player") {
          try {
            const v=await verifyPlayerForClub(value,room.currentClub);
            if(room.usedPlayers.some(x=>x.id===v.playerId)) throw new Error("That player has already been used.");
            room.usedPlayers.push({id:v.playerId,name:v.player});
            room.lastPlayer=v.player;
            room.history.push({kind:"player",name:v.player,by:p.name});
            room.phase="nextClub";
            room.message=`${v.player} verified. ${p.name}, name another club ${v.player} played for.`;
          } catch(err) { return send(ws,"error",{message:err.message}); }
        } else if(room.phase==="nextClub") {
          try {
            const v=await verifyClubForPlayer(value,room.lastPlayer);
            if(room.usedClubs.some(x=>x.id===v.clubId)) throw new Error("That club has already been used.");
            room.currentClub=v.club; room.usedClubs.push({id:v.clubId,name:v.club});
            room.history.push({kind:"club",name:v.club,by:p.name});
            room.phase="player";
            room.message=`${v.club} verified. ${p.name}, name a player who played for this club.`;
          } catch(err) { return send(ws,"error",{message:err.message}); }
        }
        // Two players each submit a club and its player; with 3+ players, answers alternate.
        if (alive(room).length === 2) {
          if (room.phase === "player") {
            // Keep the same turn after the club so this player can name the player.
          } else if (room.phase === "nextClub") {
            room.phase = "startClub"; room.currentClub = null; room.lastPlayer = null;
            advance(room);
          }
        } else { advance(room); }
        broadcast(room);
      }
      if(m.type==="pass") {
        const room=rooms.get(ws.room); if(!room || room.status!=="playing") return;
        const p=room.players.find(x=>x.id===ws.player);
        if(!p || current(room)?.id!==p.id) return send(ws,"error",{message:"It's not your turn."});
        p.alive=false; room.message=`${p.name} couldn't continue and is out.`;
        advance(room); broadcast(room);
      }
    } catch(e) { send(ws,"error",{message:e.message||"Something went wrong."}); }
  });
  ws.on("close",()=>{
    const room=rooms.get(ws.room); if(!room) return;
    const p=room.players.find(x=>x.id===ws.player); if(p) p.connected=false;
    room.message=`${p?.name||"A player"} disconnected. They can rejoin only by creating/joining a new room.`;
    broadcast(room);
  });
});

const port=process.env.PORT||3000;
server.listen(port,()=>console.log(`Club Player running on port ${port}`));
