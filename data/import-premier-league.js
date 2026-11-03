const fs = require("fs");
const path = require("path");

const endpoint = "https://query.wikidata.org/sparql";
const outputPath = path.join(__dirname, "football-db.json");
const startYear = 1990;
const pageSize = 100;
const pause = ms => new Promise(resolve => setTimeout(resolve, ms));

function iriToId(uri) {
  return String(uri || "").split("/").pop();
}
function yearOf(value) {
  if (!value) return null;
  const match = String(value).match(/^([+-]?\\d{4})/);
  return match ? Number(match[1]) : null;
}
async function runQuery(query, description) {
  const url = endpoint + "?query=" + encodeURIComponent(query) + "&format=json";
  let lastError;
  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      console.log(description + " (attempt " + attempt + "/3)");
      const response = await fetch(url, {
        headers: {
          "Accept": "application/sparql-results+json",
          "User-Agent": "ClubPlayerGame/1.0 (https://github.com/ruimessirui-ops/Club-Player; database-import)"
        },
        signal: AbortSignal.timeout(55000)
      });
      if (!response.ok) throw new Error("Wikidata query failed with HTTP " + response.status);
      const data = await response.json();
      return data.results?.bindings || [];
    } catch (error) {
      lastError = error;
      console.log("Query attempt failed: " + error.message);
      if (attempt < 3) await pause(attempt * 8000);
    }
  }
  throw lastError;
}
async function getPremierLeagueClubs() {
  const query = [
    "SELECT DISTINCT ?club ?clubLabel WHERE {",
    "  ?club wdt:P118 wd:Q9448.",
    "  SERVICE wikibase:label { bd:serviceParam wikibase:language 'en'. ?club rdfs:label ?clubLabel. }",
    "} ORDER BY ?club"
  ].join("\n");
  return runQuery(query, "Finding Premier League clubs");
}
async function getClubCareerPage(clubId, offset) {
  const query = [
    "SELECT ?player ?playerLabel ?start ?end WHERE {",
    "  ?player wdt:P106 wd:Q937857; p:P54 ?membership.",
    "  ?membership ps:P54 wd:" + clubId + ".",
    "  OPTIONAL { ?membership pq:P580 ?start. }",
    "  OPTIONAL { ?membership pq:P582 ?end. }",
    "  SERVICE wikibase:label { bd:serviceParam wikibase:language 'en'. ?player rdfs:label ?playerLabel. }",
    "  FILTER((!BOUND(?start) && !BOUND(?end)) || (BOUND(?start) && YEAR(?start) >= " + startYear + ") || (BOUND(?end) && YEAR(?end) >= " + startYear + "))",
    "}",
    "ORDER BY ?player",
    "LIMIT " + pageSize + " OFFSET " + offset
  ].join("\n");
  return runQuery(query, "Importing careers for club " + clubId + ", offset " + offset);
}

async function main() {
  const db = JSON.parse(fs.readFileSync(outputPath, "utf8"));
  db.schemaVersion = 1;
  db.coverage = { ...(db.coverage || {}), careerStartYear: startYear, scope: "premier_league", lastImportedAt: new Date().toISOString() };
  db.sources = db.sources || [];
  db.clubs = db.clubs || [];
  db.players = db.players || [];
  db.careerLinks = db.careerLinks || [];

  const clubs = new Map(db.clubs.map(item => [item.id, item]));
  const players = new Map(db.players.map(item => [item.id, item]));
  const links = new Map(db.careerLinks.map(item => [item.playerId + "|" + item.clubId, item]));

  const clubRows = await getPremierLeagueClubs();
  console.log("Found " + clubRows.length + " Premier League clubs in Wikidata.");
  for (const clubRow of clubRows) {
    const clubId = iriToId(clubRow.club?.value);
    if (!clubId) continue;
    if (!clubs.has(clubId)) clubs.set(clubId, {
      id: clubId, name: clubRow.clubLabel?.value || clubId, country: "England",
      aliases: [], type: "football_club", competition: "Premier League", source: "Wikidata"
    });

    for (let offset = 0; ; offset += pageSize) {
      const rows = await getClubCareerPage(clubId, offset);
      for (const row of rows) {
        const playerId = iriToId(row.player?.value);
        if (!playerId) continue;
        const start = yearOf(row.start?.value);
        const end = yearOf(row.end?.value);
        const undated = start === null && end === null;
        if (!undated && !((start !== null && start >= startYear) || (end !== null && end >= startYear))) continue;

        if (!players.has(playerId)) players.set(playerId, {
          id: playerId, name: row.playerLabel?.value || playerId,
          nationality: null, aliases: [], type: "football_player", source: "Wikidata"
        });
        const key = playerId + "|" + clubId;
        const existing = links.get(key);
        const record = {
          playerId, clubId, source: "Wikidata P54",
          startYear: start, endYear: end,
          dateStatus: undated ? "undated_needs_review" : "dated",
          evidenceUrl: "https://www.wikidata.org/wiki/" + playerId
        };
        if (!existing || (existing.dateStatus === "undated_needs_review" && record.dateStatus === "dated")) links.set(key, record);
      }
      if (rows.length < pageSize) break;
      await pause(3000);
    }
    await pause(3000);
  }

  db.clubs = [...clubs.values()].sort((a,b) => a.name.localeCompare(b.name));
  db.players = [...players.values()].sort((a,b) => a.name.localeCompare(b.name));
  db.careerLinks = [...links.values()];
  db.sources = db.sources.filter(source => source.id !== "wikidata-p54-premier-league");
  db.sources.push({
    id: "wikidata-p54-premier-league",
    name: "Wikidata",
    licence: "CC0",
    retrievedAt: db.coverage.lastImportedAt,
    description: "Premier League club career relationships from property P54; dates retained when supplied. Undated links require review and must not be treated as date-confirmed."
  });

  fs.writeFileSync(outputPath, JSON.stringify(db, null, 2) + "\n");
  console.log("Import complete.");
  console.log("Unique clubs: " + db.clubs.length + "; players: " + db.players.length + "; career links: " + db.careerLinks.length);
  console.log("Review undated records before using this data to reject an answer.");
}
main().catch(error => {
  console.error(error);
  process.exitCode = 1;
});
