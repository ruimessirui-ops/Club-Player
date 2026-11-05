const fs = require("fs");
const path = require("path");

const endpoint = "https://query.wikidata.org/sparql";
const outputPath = path.join(__dirname, "football-db.json");
const startYear = 1990;
const pageSize = 100;
const maxPagesPerRun = 100;
const discoveryVersion = 1;
const pause = ms => new Promise(resolve => setTimeout(resolve, ms));

const competitions = [
  { id: "Q9448", name: "Premier League" },
  { id: "Q324867", name: "La Liga" },
  { id: "Q82595", name: "Bundesliga" },
  { id: "Q15804", name: "Serie A" },
  { id: "Q13394", name: "Ligue 1" }
];

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
        headers: {
          "Accept": "application/sparql-results+json",
          "User-Agent": "ClubPlayerGame/1.0 (https://github.com/ruimessirui-ops/Club-Player; historical-club-import)"
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
async function discoverClubs() {
  const values = competitions.map(item => "wd:" + item.id).join(" ");
  const query = [
    "SELECT DISTINCT ?club ?clubLabel ?countryLabel ?competition ?competitionLabel WHERE {",
    "  VALUES ?competition { " + values + " }",
    "  { ?club wdt:P118 ?competition. }",
    "  UNION",
    "  { ?season wdt:P3450 ?competition. ?club wdt:P118 ?season. }",
    "  ?club wdt:P31/wdt:P279* wd:Q476028.",
    "  OPTIONAL { ?club wdt:P17 ?country. }",
    "  SERVICE wikibase:label { bd:serviceParam wikibase:language 'en'.",
    "    ?club rdfs:label ?clubLabel.",
    "    ?country rdfs:label ?countryLabel.",
    "    ?competition rdfs:label ?competitionLabel.",
    "  }",
    "  FILTER(!REGEX(LCASE(STR(?clubLabel)), 'women|woman|femen|femminile|ladies|girls|youth|academy|reserve|under[- ]?[0-9]{1,2}'))",
    "}",
    "ORDER BY ?club"
  ].join("\n");
  const rows = await runQuery(query, "Discovering clubs across five major European leagues");
  const byId = new Map();
  for (const row of rows) {
    const id = iriToId(row.club?.value);
    if (!id) continue;
    if (!byId.has(id)) byId.set(id, {
      id,
      name: row.clubLabel?.value || id,
      country: row.countryLabel?.value || "",
      competitions: new Set()
    });
    const competitionName = row.competitionLabel?.value;
    if (competitionName) byId.get(id).competitions.add(competitionName);
  }
  return [...byId.values()]
    .map(club => ({ ...club, competitions: [...club.competitions].sort() }))
    .sort((a, b) => a.name.localeCompare(b.name));
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
  db.clubs = [...clubs.values()].sort((a, b) => a.name.localeCompare(b.name));
  db.players = [...players.values()].sort((a, b) => a.name.localeCompare(b.name));
  db.careerLinks = [...links.values()];
  db.coverage.lastHistoricalImportAt = new Date().toISOString();
  fs.writeFileSync(outputPath, JSON.stringify(db, null, 2) + "\n");
}

async function main() {
  const db = JSON.parse(fs.readFileSync(outputPath, "utf8"));
  db.schemaVersion = 1;
  db.coverage = { ...(db.coverage || {}), careerStartYear: startYear };
  db.sources = db.sources || [];
  db.clubs = db.clubs || [];
  db.players = db.players || [];
  db.careerLinks = db.careerLinks || [];

  const clubs = new Map(db.clubs.map(item => [item.id, item]));
  const players = new Map(db.players.map(item => [item.id, item]));
  const links = new Map(db.careerLinks.map(item => [item.playerId + "|" + item.clubId, item]));

  const discovered = await discoverClubs();
  console.log("Discovered " + discovered.length + " unique clubs.");
  if (discovered.length === 0 || discovered.length > 1000) {
    throw new Error("Club discovery failed its safety check (" + discovered.length + " clubs). No database changes were made.");
  }

  const previous = db.coverage.historicalImportProgress || {};
  const restart = previous.discoveryVersion !== discoveryVersion || previous.totalClubs !== discovered.length;
  if (previous.completed && !restart) {
    console.log("Historical club import is already complete. No work needed.");
    return;
  }
  let clubIndex = restart ? 0 : (previous.clubIndex || 0);
  let offset = restart ? 0 : (previous.offset || 0);
  let pagesDone = 0;

  while (clubIndex < discovered.length && pagesDone < maxPagesPerRun) {
    const club = discovered[clubIndex];
    const existing = clubs.get(club.id);
    const clubCompetitions = [...new Set([...(existing?.competitions || []), ...(existing?.competition ? [existing.competition] : []), ...club.competitions])];
    if (existing) {
      existing.competitions = clubCompetitions;
      if (!existing.country && club.country) existing.country = club.country;
    } else {
      clubs.set(club.id, {
        id: club.id,
        name: club.name,
        country: club.country,
        aliases: [],
        type: "football_club",
        competition: club.competitions[0] || "",
        competitions: clubCompetitions,
        source: "Wikidata"
      });
    }

    const rows = await getClubCareerPage(club.id, offset);
    pagesDone++;
    for (const row of rows) {
      const playerId = iriToId(row.player?.value);
      if (!playerId) continue;
      const start = yearOf(row.start?.value);
      const end = yearOf(row.end?.value);
      const undated = start === null && end === null;
      if (!undated && !((start !== null && start >= startYear) || (end !== null && end >= startYear))) continue;
      if (!players.has(playerId)) {
        players.set(playerId, {
          id: playerId,
          name: row.playerLabel?.value || playerId,
          nationality: null,
          aliases: [],
          type: "football_player",
          source: "Wikidata"
        });
      }
      const key = playerId + "|" + club.id;
      const existingLink = links.get(key);
      const record = {
        playerId,
        clubId: club.id,
        source: "Wikidata P54",
        startYear: start,
        endYear: end,
        dateStatus: undated ? "undated_needs_review" : "dated",
        evidenceUrl: "https://www.wikidata.org/wiki/" + playerId
      };
      if (!existingLink || (existingLink.dateStatus === "undated_needs_review" && record.dateStatus === "dated")) {
        links.set(key, record);
      }
    }

    if (rows.length === pageSize) offset += pageSize;
    else { clubIndex++; offset = 0; }

    db.coverage.historicalImportProgress = {
      clubIndex,
      offset,
      totalClubs: discovered.length,
      completed: clubIndex >= discovered.length,
      discoveryVersion
    };
    saveDatabase(db, clubs, players, links);
    console.log("Saved progress: club " + clubIndex + " of " + discovered.length +
      ", next offset " + offset + "; pages this run " + pagesDone + "/" + maxPagesPerRun);
    await pause(1500);
  }

  db.coverage.historicalImportProgress = {
    clubIndex,
    offset,
    totalClubs: discovered.length,
    completed: clubIndex >= discovered.length,
    discoveryVersion
  };
  db.sources = db.sources.filter(source => source.id !== "wikidata-p54-top-five-leagues");
  db.sources.push({
    id: "wikidata-p54-top-five-leagues",
    name: "Wikidata",
    licence: "CC0",
    retrievedAt: new Date().toISOString(),
    description: "Player-to-club career relationships (P54) for clubs discovered through Premier League, La Liga, Bundesliga, Serie A and Ligue 1 membership. Career records from 1990 onwards; undated links are retained as unverified evidence."
  });
  saveDatabase(db, clubs, players, links);

  console.log(db.coverage.historicalImportProgress.completed
    ? "Historical five-league import complete."
    : "Batch complete. Run the workflow again to continue from saved progress.");
  console.log("Unique clubs: " + db.clubs.length + "; players: " + db.players.length +
    "; career links: " + db.careerLinks.length);
}

main().catch(error => {
  console.error(error);
  process.exitCode = 1;
});
