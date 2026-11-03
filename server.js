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
    if (type === "player") {
      try {
        const data = await theSportsDbGet("searchplayers.php", { p: q });
        items = (data.player || []).slice(0, 10).map(entry => ({
          id: String(entry.idPlayer), label: entry.strPlayer,
          description: "Football player", source: "TheSportsDB"
        })).filter(entry => entry.id && entry.label);
      } catch (error) { console.error("TheSportsDB player search failed:", error.message); }
      if (!items.length) {
        const results = await wikidataAutocompleteSearch(q);
        items = results.slice(0, 10).map(item => ({
          id: item.id, label: item.label, description: item.description || "", source: "Wikidata"
        }));
      }
    } else {
      // Try Wikidata first, then fall back to Wikipedia if Wikidata is unavailable
      // or returns no usable club suggestions. This keeps autocomplete working
      // during Wikidata rate limits/outages.
      let results = [];
      try {
        results = await wikidataAutocompleteSearch(q);
      } catch (error) {
        console.error("Wikidata club autocomplete failed; using Wikipedia fallback:", error.message);
      }
      const excluded = /women|woman|femen|femminile|ladies|girls|youth|under[- ]?\d{1,2}|reserve|academy|season|b team|ii team|iii team/i;
      const seen = new Set();
      items = results.filter(item => {
        const label = item.label || "";
        const description = item.description || "";
        if (excluded.test(label + " " + description)) return false;
        if (!/football|soccer/i.test(description)) return false;
        const key = normalizeName(label);
        if (!key || seen.has(key)) return false;
        seen.add(key);
        return true;
      }).slice(0, 10).map(item => ({
        id: item.id, label: item.label,
        country: "", league: "",
        description: item.description || "Football club",
        source: "Wikidata"
      }));
      if (!items.length) {
        try {
          const fallback = await wikipediaSearch(q, "club");
          items = fallback.filter(item => {
            if (excluded.test(item.label + " " + item.description)) return false;
            const key = normalizeName(item.label);
            if (!key || seen.has(key)) return false;
            seen.add(key);
            return /football club|association football club|soccer club/i.test(item.description) ||
              /football club/i.test(item.label);
          }).slice(0, 10).map(item => ({
            id: null, label: item.label,
            country: "", league: "",
            description: item.description || "Football club result",
            source: "Wikipedia"
          }));
        } catch (error) {
          console.error("Wikipedia club autocomplete fallback failed:", error.message);
        }
      }
    }
    searchCache.set(cacheKey, items);
    if (searchCache.size > 1000) searchCache.delete(searchCache.keys().next().value);
    return res.json(items);
  } catch (err) {
    console.error("Football search failed:", err.message);
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
    message: room.message, winner: room.winner, deadline: room.deadline || null
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
  const temporary = /could not be reached|connection failed|temporarily busy|temporarily unavailable|network|timed out|timeout|fetch failed|account is suspended|access.*suspended|API-Football returned HTTP 5/i.test(message);
  if (!temporary) {
    player.hearts = Math.max(0, (player.hearts ?? 2) - 1);
    if (player.hearts === 0) {
      player.alive = false;
      room.message = player.name + " is out of hearts and has been eliminated.";
      if (alive(room).length === 2) { room.phase = "startClub"; room.currentClub = null; room.lastPlayer = null; }
      advance(room);
    } else room.message = player.name + " gave an incorrect answer and lost a heart.";
  } else room.message = "The football database is having trouble. No heart was lost.";
  if (room.status === "playing") room.deadline = Date.now() + 30000;
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
      lastError = new Error("HTTP " + r.status);
      if (r.status !== 429 && r.status < 500) {
        throw new Error("Wikidata rejected the lookup (HTTP " + r.status + ").");
      }
    } catch (err) {
      lastError = err;
    }
    console.error("Wikidata request failed on attempt " + (attempt + 1) + "/3:", lastError?.message || lastError);
    if (attempt < 2) await delay(700 * (attempt + 1));
  }
  throw new Error("Wikidata connection failed after 3 attempts (" + (lastError?.message || "unknown error") + "). Please try again shortly.");
}

async function wikidataSearch(name) {
  const key = normalizeName(name);
  if (wikidataSearchCache.has(key)) return wikidataSearchCache.get(key);
  const url = "https://www.wikidata.org/w/api.php?action=wbsearchentities&search="+encodeURIComponent(name)+"&language=en&format=json&limit=50&origin=*";
  const r = await wikidataFetch(url);
  const j = await r.json();
  const results = (j.search || []).slice(0,50);
  wikidataSearchCache.set(key, results);
  if (wikidataSearchCache.size > 500) wikidataSearchCache.delete(wikidataSearchCache.keys().next().value);
  return results;
}
async function wikidataAutocompleteSearch(name) {
  const key = normalizeName(name);
  if (wikidataSearchCache.has(key)) return wikidataSearchCache.get(key);
  const url = "https://www.wikidata.org/w/api.php?action=wbsearchentities&search=" +
    encodeURIComponent(name) + "&language=en&format=json&limit=50&maxlag=5&origin=*";
  const response = await fetch(url, {
    headers: {
      "User-Agent": "ClubPlayerGame/1.0 (https://github.com/ruimessirui-ops/Club-Player)",
      "Accept": "application/json"
    },
    signal: AbortSignal.timeout(4500)
  });
  if (!response.ok) throw new Error("Wikidata autocomplete HTTP " + response.status);
  const data = await response.json();
  const results = (data.search || []).slice(0, 50);
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
const apiFootballCache = new Map();
async function apiFootballGet(path, params = {}) {
  const key = process.env.API_FOOTBALL_KEY;
  if (!key) throw new Error("API_FOOTBALL_KEY is not configured in Render.");
  const url = new URL("https://v3.football.api-sports.io" + path);
  for (const [name, value] of Object.entries(params)) url.searchParams.set(name, String(value));
  const cacheKey = url.toString();
  const cached = apiFootballCache.get(cacheKey);
  if (cached && Date.now() - cached.savedAt < 24 * 60 * 60 * 1000) return cached.data;
  const response = await fetch(url, {
    headers: { "x-apisports-key": key },
    signal: AbortSignal.timeout(10000)
  });
  if (!response.ok) throw new Error("API-Football returned HTTP " + response.status + ".");
  const data = await response.json();
  if (data.errors && Object.keys(data.errors).length) {
    throw new Error("API-Football: " + JSON.stringify(data.errors));
  }
  apiFootballCache.set(cacheKey, { data, savedAt: Date.now() });
  if (apiFootballCache.size > 1500) apiFootballCache.delete(apiFootballCache.keys().next().value);
  return data;
}

const theSportsDbCache = new Map();
let theSportsDbCooldownUntil = 0;
async function theSportsDbGet(endpoint, params = {}) {
  if (Date.now() < theSportsDbCooldownUntil) {
    throw new Error("TheSportsDB rate limit cooldown is active.");
  }
  const url = new URL("https://www.thesportsdb.com/api/v1/json/123/" + endpoint);
  for (const [key, value] of Object.entries(params)) url.searchParams.set(key, String(value).replace(/ /g, "_"));
  const cacheKey = url.toString();
  const cached = theSportsDbCache.get(cacheKey);
  if (cached && Date.now() - cached.savedAt < 24 * 60 * 60 * 1000) return cached.data;
  const response = await fetch(url, { signal: AbortSignal.timeout(10000) });
  if (response.status === 429) {
    theSportsDbCooldownUntil = Date.now() + 60 * 1000;
    console.error("TheSportsDB rate limit reached. Pausing requests for 60 seconds.");
    throw new Error("TheSportsDB returned HTTP 429; cooldown started.");
  }
  if (!response.ok) throw new Error("TheSportsDB returned HTTP " + response.status + ".");
  const data = await response.json();
  theSportsDbCache.set(cacheKey, { data, savedAt: Date.now() });
  if (theSportsDbCache.size > 1500) theSportsDbCache.delete(theSportsDbCache.keys().next().value);
  return data;
}

async function resolveFootball(name, type) {
  try {
    const data = type === "club"
      ? await theSportsDbGet("searchteams.php", { t: name })
      : await theSportsDbGet("searchplayers.php", { p: name });
    const rows = type === "club" ? (data.teams || []) : (data.player || []);
    const entry = rows.find(row => normalizeName(type === "club" ? row.strTeam : row.strPlayer) === normalizeName(name)) || rows[0];
    const id = type === "club" ? entry?.idTeam : entry?.idPlayer;
    const label = type === "club" ? entry?.strTeam : entry?.strPlayer;
    if (id && label) return { id: String(id), name: label, source: "TheSportsDB" };
  } catch (error) {
    console.error("TheSportsDB resolve failed:", error.message);
  }
  const results = await wikidataSearch(name);
  const exact = results.find(x => normalizeName(x.label) === normalizeName(name)) || results[0];
  if (!exact) throw new Error('I could not find "' + name + '" in the football database. Try another spelling.');
  return { id: exact.id, name: exact.label, source: "Wikidata", entity: await entity(exact.id) };
}

async function resolve(name) {
  return resolveFootball(name, "club");
}

const verificationCache = new Map();
async function verifyLink(clubName, playerName) {
  const verificationKey = normalizeName(clubName) + "|" + normalizeName(playerName);
  const cachedVerification = verificationCache.get(verificationKey);
  if (cachedVerification) return cachedVerification;
  const [club, player] = await Promise.all([resolveFootball(clubName, "club"), resolveFootball(playerName, "player")]);

  // TheSportsDB and Wikidata use different IDs. Never compare IDs across sources.
  if (club.source === "TheSportsDB" && player.source === "TheSportsDB") {
    try {
      const data = await theSportsDbGet("lookupformerteams.php", { id: player.id });
      const formerTeams = data.formerteams || data.teams || [];
      const found = formerTeams.some(team =>
        String(team.idTeam || team.id) === String(club.id) ||
        normalizeName(team.strTeam || team.name || "") === normalizeName(club.name)
      );
      // Continue to Wikidata below so we can check that the player has
      // represented at least two different clubs in their career.
    } catch (error) {
      console.error("TheSportsDB career lookup failed:", error.message);
    }
  }

  // Use Wikidata IDs for BOTH sides as a consistent fallback. This also checks
  // current clubs, which may not appear in TheSportsDB's former-teams endpoint.
  const [clubResults, playerResults] = await Promise.all([
    wikidataSearch(clubName),
    wikidataSearch(playerName)
  ]);
  const wdClub = clubResults.find(x => normalizeName(x.label) === normalizeName(clubName)) || clubResults[0];
  const wdPlayer = playerResults.find(x => normalizeName(x.label) === normalizeName(playerName)) || playerResults[0];
  if (!wdClub || !wdPlayer) {
    throw new Error("I couldn't verify both names in Wikidata. Try another spelling.");
  }
  const playerEntity = await entity(wdPlayer.id);
  const clubClaims = playerEntity?.claims?.P54 || [];
  const teamIds = [...new Set(clubClaims.map(x => x.mainsnak?.datavalue?.value?.id).filter(Boolean))];

  // Load all career team entities in parallel; serial lookups made verification slow.
  const loadedTeams = (await Promise.all(teamIds.map(async teamId => {
    try { return await entity(teamId); }
    catch (error) { console.error("Could not load Wikidata team entity:", teamId, error.message); return null; }
  }))).filter(Boolean);

  // Exclude national teams from the minimum-club rule, but retain every P54
  // entry for matching. Some genuine clubs have incomplete Wikidata descriptions.
  const isNationalTeam = teamEntity => {
    const description = (teamEntity.descriptions?.en?.value || "").toLowerCase();
    return /national (football|soccer) team|national association football team/.test(description);
  };
  const isFootballClub = teamEntity => {
    const description = (teamEntity.descriptions?.en?.value || "").toLowerCase();
    return /football club|association football club|soccer club/.test(description) ||
      (teamEntity.claims?.P31 || []).some(claim =>
        ["Q476028", "Q847017", "Q12973014"].includes(claim.mainsnak?.datavalue?.value?.id)
      );
  };
  const careerClubs = loadedTeams.filter(teamEntity => !isNationalTeam(teamEntity) && isFootballClub(teamEntity));
  if (careerClubs.length < 2) {
    throw new Error(wdPlayer.label + " cannot be used because Wikidata lists fewer than two football clubs in their career.");
  }

  // Match against ALL of the player's P54 teams, not only entities with a
  // complete football-club description. This avoids rejecting clubs like Roma
  // when Wikidata's description/type metadata is incomplete.
  const requested = normalizeName(clubName);
  let matchedTeam = loadedTeams.find(teamEntity => teamEntity.id === wdClub.id && !isNationalTeam(teamEntity)) || null;
  if (!matchedTeam) {
    matchedTeam = loadedTeams.find(teamEntity => {
      if (isNationalTeam(teamEntity)) return false;
      const teamLabels = labels(teamEntity);
      return teamLabels.includes(requested) || teamLabels.some(label =>
        label.startsWith(requested + " ") || requested.startsWith(label + " ")
      );
    }) || null;
  }
  if (!matchedTeam) {
    throw new Error(wdPlayer.label + " is not listed as having played for " + clubName + " in Wikidata. The database can be incomplete.");
  }
  const matchedLabel = matchedTeam.labels?.en?.value || clubName;
  const result = { club: matchedLabel, player: wdPlayer.label, clubId: matchedTeam.id, playerId: wdPlayer.id };
  verificationCache.set(verificationKey, result);
  if (verificationCache.size > 2000) verificationCache.delete(verificationCache.keys().next().value);
  return result;
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
        room.status="playing"; room.phase="startClub"; room.turn=0; room.deadline=Date.now()+30000; room.message=`${room.players[0].name}, choose the starting club.`; broadcast(room);
      }
      if(m.type==="move") {
        const room=rooms.get(ws.room); if(!room || room.status!=="playing") return;
        const p=room.players.find(x=>x.id===ws.player);
        if(!p || !p.alive || current(room)?.id!==p.id) return send(ws,"error",{message:"It's not your turn."});
        const value=clean(m.value); if(!value) return send(ws,"error",{message:"Enter a club or player name."});
        // Pause the clock while the submitted answer is being checked.
        room.deadline = null;
        room.message = p.name + " submitted an answer. Checking…";
        broadcast(room);
        if(room.phase==="startClub") {
          try {
            const e=await resolve(value);
            if (e.source === "Wikidata") {
              const desc=(e.entity?.descriptions?.en?.value||"").toLowerCase();
              if(!desc.includes("football club") && !desc.includes("association football club") && !desc.includes("sports club")) {
                if(!(e.entity?.claims?.P31||[]).some(c=>["Q476028","Q847017","Q12973014"].includes(c.mainsnak?.datavalue?.value?.id))) throw new Error("That doesn't look like a football club. Try a club name such as Dortmund or Juventus.");
              }
            }
            const canon=e.name||value;
            if(room.usedClubs.some(x=>String(x.id)===String(e.id))) throw new Error("That club has already been used.");
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
          } catch(err) { return handleWrongAnswer(room,p,ws,err); }
        } else if(room.phase==="nextClub") {
          try {
            const v=await verifyClubForPlayer(value,room.lastPlayer);
            if(room.usedClubs.some(x=>x.id===v.clubId)) throw new Error("That club has already been used.");
            room.currentClub=v.club; room.usedClubs.push({id:v.clubId,name:v.club});
            room.history.push({kind:"club",name:v.club,by:p.name});
            room.phase="player";
            room.message=`${v.club} verified. ${p.name}, name a player who played for this club.`;
          } catch(err) { return handleWrongAnswer(room,p,ws,err); }
        }
        // With exactly 2 players, each player submits a club and then a player.
        // After the player is verified, the opponent chooses the next club.
        // With 3+ players, turns alternate after every answer.
        let turnChanged = false;
        if (alive(room).length === 2) {
          if (room.phase === "player") {
            // Same player now names someone who played for the selected club.
          } else if (room.phase === "nextClub") {
            advance(room);
            turnChanged = true;
          }
        } else {
          advance(room);
          turnChanged = true;
        }
        if (turnChanged && room.status === "playing") {
          const next = current(room);
          if (room.phase === "nextClub") {
            room.message = `${next.name}, name another club ${room.lastPlayer} played for.`;
          } else if (room.phase === "player") {
            room.message = `${next.name}, name a player who played for ${room.currentClub}.`;
          }
        }
        if (room.status === "playing") room.deadline = Date.now() + 30000;
        broadcast(room);
      }
      if(m.type==="pass") {
        const room=rooms.get(ws.room); if(!room || room.status!=="playing") return;
        const p=room.players.find(x=>x.id===ws.player);
        if(!p || current(room)?.id!==p.id) return send(ws,"error",{message:"It's not your turn."});
        p.alive=false; room.message=`${p.name} couldn't continue and is out.`;
        advance(room); if (room.status === "playing") room.deadline = Date.now()+30000; broadcast(room);
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


setInterval(() => {
  for (const room of rooms.values()) {
    if (room.status !== "playing" || !room.deadline || Date.now() < room.deadline) continue;
    const p = current(room);
    if (!p || !p.alive) { advance(room); if (room.status === "playing") room.deadline = Date.now()+30000; broadcast(room); continue; }
    p.hearts = Math.max(0, (p.hearts ?? 2) - 1);
    if (p.hearts === 0) {
      p.alive = false;
      room.message = p.name + " ran out of time and was eliminated.";
      if (alive(room).length === 2) { room.phase = "startClub"; room.currentClub = null; room.lastPlayer = null; }
      advance(room);
    } else {
      room.message = p.name + " ran out of time and lost a heart.";
      advance(room);
    }
    if (room.status === "playing") room.deadline = Date.now()+30000;
    broadcast(room);
  }
}, 250);

const port=process.env.PORT||3000;
server.listen(port,()=>console.log(`Club Player running on port ${port}`));
