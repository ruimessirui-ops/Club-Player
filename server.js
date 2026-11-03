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
  try {
    const q = clean(req.query.q);
    const type = req.query.type === "club" ? "club" : "player";
    if (q.length < 2) return res.json([]);
    const results = await wikidataSearch(q);
    const filtered = results.filter(item => {
      const d = (item.description || "").toLowerCase();
      return type === "club" ? /football club|association football club|sports club/.test(d) : /footballer|soccer player|football player/.test(d);
    }).slice(0, 7);
    res.json(filtered.map(item => ({ id: item.id, label: item.label, description: item.description || "" })));
  } catch (err) { res.status(503).json({ error: "Search is temporarily unavailable." }); }
});

const rooms = new Map();
const clean = s => String(s || "").trim().replace(/\s+/g, " ").slice(0, 60);
const roomCode = () => crypto.randomBytes(3).toString("hex").toUpperCase();

function publicState(room) {
  return {
    code: room.code, status: room.status, players: room.players.map(p => ({id:p.id,name:p.name,connected:p.connected,alive:p.alive})),
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

// Resolve names with Wikidata search, then check football career club links (P54).
async function wikidataSearch(name) {
  const url = "https://www.wikidata.org/w/api.php?action=wbsearchentities&search="+encodeURIComponent(name)+"&language=en&format=json&limit=8&origin=*";
  const r = await fetch(url, {headers:{"User-Agent":"ClubPlayerGame/1.0 (browser game)"}});
  if (!r.ok) throw new Error("Wikidata is temporarily unavailable.");
  const j = await r.json();
  return (j.search || []).slice(0,8);
}
async function entity(id) {
  const r=await fetch("https://www.wikidata.org/wiki/Special:EntityData/"+id+".json", {headers:{"User-Agent":"ClubPlayerGame/1.0"}});
  if(!r.ok) throw new Error("Could not check that entry.");
  const j=await r.json(); return j.entities[id];
}
function labels(e) {
  const out=[e.labels?.en?.value, ...(e.aliases?.en||[]).map(x=>x.value)];
  return out.filter(Boolean).map(normalizeName);
}
async function resolve(name) {
  const results=await wikidataSearch(name);
  const exact=results.find(x=>normalizeName(x.label)===normalizeName(name)) || results[0];
  if(!exact) throw new Error(`I couldn't find "${name}" in the football database. Check the spelling or try the player's full name.`);
  return entity(exact.id);
}
async function verifyLink(clubName, playerName) {
  const [club, player]=await Promise.all([resolve(clubName),resolve(playerName)]);
  const clubNames=labels(club);
  const playerNames=labels(player);
  const clubClaims=player.claims?.P54 || []; // member of sports team
  const playedFor=clubClaims.map(c=>c.mainsnak?.datavalue?.value?.id).filter(Boolean);
  if(!playedFor.includes(club.id)) {
    const clubDesc=club.descriptions?.en?.value || "";
    const playerDesc=player.descriptions?.en?.value || "";
    throw new Error(`${player.labels?.en?.value || playerName} is not listed as having played for ${club.labels?.en?.value || clubName} in Wikidata. Try another player/club spelling. (The database can be incomplete.)`);
  }
  return {club:club.labels?.en?.value || clubName, player:player.labels?.en?.value || playerName, clubId:club.id, playerId:player.id};
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
        const p={id:crypto.randomUUID(),name,ws,connected:true,alive:true};
        const room={code,players:[p],status:"lobby",turn:0,phase:"startClub",currentClub:null,lastPlayer:null,usedClubs:[],usedPlayers:[],history:[],message:"Waiting for players to join.",winner:null};
        rooms.set(code,room); ws.room=code; ws.player=p.id; send(ws,"joined",{id:p.id,code}); broadcast(room);
      }
      if(m.type==="join") {
        const code=clean(m.code).toUpperCase(), room=rooms.get(code);
        if(!room) return send(ws,"error",{message:"Room not found. Check the code."});
        if(room.status!=="lobby") return send(ws,"error",{message:"This game has already started."});
        if(room.players.length>=8) return send(ws,"error",{message:"This room is full (maximum 8 players)."});
        const p={id:crypto.randomUUID(),name:clean(m.name)||"Player",ws,connected:true,alive:true};
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
          } catch(err) { return send(ws,"error",{message:err.message}); }
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
