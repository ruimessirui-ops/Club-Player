const fs = require("fs");
const path = require("path");

const endpoint = "https://query.wikidata.org/sparql";
const outputPath = path.join(__dirname, "football-db.json");
const startYear = 1990;
const pageSize = 100;
const maxPagesPerRun = 150;
const pause = ms => new Promise(resolve => setTimeout(resolve, ms));

function iriToId(uri) { return String(uri || "").split("/").pop(); }
function yearOf(value) {
  if (!value) return null;
  const match = String(value).match(/^([+-]?\d{4})/);
  return match ? Number(match[1]) : null;
}
async function runQuery(query, description) {
  const url = endpoint + "?query=" + encodeURIComponent(query) + "&format=json";
  let lastError;
  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      console.log(description + " (attempt " + attempt + "/3)");
      const response = await fetch(url, {
        headers: { "Accept": "application/sparql-results+json", "User-Agent": "ClubPlayerGame/1.0 (https://github.com/ruimessirui-ops/Club-Player; database-import)" },
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
    "  { ?club wdt:P118 wd:Q9448. }",
    "  UNION",
    "  { ?season wdt:P3450 wd:Q9448. ?club wdt:P118 ?season. }",
    "  ?club wdt:P31/wdt:P279* wd:Q476028.",
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
function saveDatabase(db, clubs, players, links) {
  db.clubs = [...clubs.values()].sort((a,b) => a.name.localeCompare(b.name));
  db.players = [...players.values()].sort((a,b) => a.name.localeCompare(b.name));
  db.careerLinks = [...links.values()];
  db.coverage.lastImportedAt = new Date().toISOString();
  fs.writeFileSync(outputPath, JSON.stringify(db, null, 2) + "\n");
}

async function main() {
  const db = JSON.parse(fs.readFileSync(outputPath, "utf8"));
  db.schemaVersion = 1;
  db.coverage = { ...(db.coverage || {}), careerStartYear: startYear, scope: "premier_league", lastImportedAt: db.coverage?.lastImportedAt || null };
  db.sources = db.sources || []; db.clubs = db.clubs || []; db.players = db.players || []; db.careerLinks = db.careerLinks || [];
  const clubs = new Map(db.clubs.map(item => [item.id, item]));
  const players = new Map(db.players.map(item => [item.id, item]));
  const links = new Map(db.careerLinks.map(item => [item.playerId + "|" + item.clubId, item]));
  const clubRows = await getPremierLeagueClubs();
  console.log("Found " + clubRows.length + " Premier League football clubs in Wikidata.");
  if (clubRows.length === 0 || clubRows.length > 500) throw new Error("The club discovery result failed its safety check (" + clubRows.length + " results). No database changes were made.");
  const progress = db.coverage.importProgress || { clubIndex: 0, offset: 0, completed: false, discoveryVersion: 2 };
  const discoveryVersion = 2;
  const needsRestart = progress.discoveryVersion !== discoveryVersion || progress.totalClubs !== clubRows.length;
  if (progress.completed && !needsRestart) { console.log("Historical club discovery import already complete. No work needed."); return; }
  let clubIndex = needsRestart ? 0 : (progress.clubIndex || 0);
  let offset = needsRestart ? 0 : (progress.offset || 0);
  let pagesDone = 0;
  while (clubIndex < clubRows.length && pagesDone < maxPagesPerRun) {
    const clubRow = clubRows[clubIndex];
    const clubId = iriToId(clubRow.club?.value);
    if (!clubId) { clubIndex++; offset = 0; continue; }
    if (!clubs.has(clubId)) clubs.set(clubId, { id: clubId, name: clubRow.clubLabel?.value || clubId, country: "England", aliases: [], type: "football_club", competition: "Premier League", source: "Wikidata" });
    const rows = await getClubCareerPage(clubId, offset);
    pagesDone++;
    for (const row of rows) {
      const playerId = iriToId(row.player?.value);
      if (!playerId) continue;
      const start = yearOf(row.start?.value); const end = yearOf(row.end?.value);
      const undated = start === null && end === null;
      if (!undated && !((start !== null && start >= startYear) || (end !== null && end >= startYear))) continue;
      if (!players.has(playerId)) players.set(playerId, { id: playerId, name: row.playerLabel?.value || playerId, nationality: null, aliases: [], type: "football_player", source: "Wikidata" });
      const key = playerId + "|" + clubId; const existing = links.get(key);
      const record = { playerId, clubId, source: "Wikidata P54", startYear: start, endYear: end, dateStatus: undated ? "undated_needs_review" : "dated", evidenceUrl: "https://www.wikidata.org/wiki/" + playerId };
      if (!existing || (existing.dateStatus === "undated_needs_review" && record.dateStatus === "dated")) links.set(key, record);
    }
    if (rows.length === pageSize) offset += pageSize;
    else { clubIndex++; offset = 0; }
    db.coverage.importProgress = { clubIndex, offset, totalClubs: clubRows.length, completed: clubIndex >= clubRows.length, discoveryVersion };
    saveDatabase(db, clubs, players, links);
    console.log("Saved progress: club " + clubIndex + " of " + clubRows.length + ", next offset " + offset + "; pages this run " + pagesDone + "/" + maxPagesPerRun);
    await pause(1500);
  }
  db.coverage.importProgress = { clubIndex, offset, totalClubs: clubRows.length, completed: clubIndex >= clubRows.length };
  db.sources = db.sources.filter(source => source.id !== "wikidata-p54-premier-league");
  db.sources.push({ id: "wikidata-p54-premier-league", name: "Wikidata", licence: "CC0", retrievedAt: db.coverage.lastImportedAt, description: "Premier League club career relationships from property P54; dates retained when supplied. Undated links require review and must not be treated as date-confirmed." });
  saveDatabase(db, clubs, players, links);
  console.log(db.coverage.importProgress.completed ? "Import complete." : "Batch complete. Run the workflow again to continue from the saved position.");
  console.log("Unique clubs: " + db.clubs.length + "; players: " + db.players.length + "; career links: " + db.careerLinks.length);
}
main().catch(error => { console.error(error); process.exitCode = 1; });
