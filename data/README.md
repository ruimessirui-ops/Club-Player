# Club Player local football database

This directory contains the versioned local dataset used by Club Player.

## Coverage target
- Career records from 1990 onwards.
- Initial scope: the top divisions in England, Spain, Germany, Italy and France.
- Include former and current clubs; do not restrict a player to their current team.
- Expand to historically significant clubs and other major European leagues after validating the first import.

## Data model
- `clubs`: stable Wikidata QID, canonical name, country, aliases and club type.
- `players`: stable Wikidata QID, canonical name, aliases, nationality and football-player classification.
- `careerLinks`: player QID, club QID, source, evidence, and optional start/end years.
- `sources`: source name, licence, retrieval date and import notes.

## Important accuracy rules
1. A missing career link is **unknown**, not proof that a player never played for a club.
2. Only confirmed links can automatically validate an answer.
3. Never treat a national team, stadium, league, academy or women's team as a selectable men's club in the initial game mode.
4. Keep source identifiers so records from different databases are never joined by a coincidental numeric ID.
5. Retain the source and retrieval date for every imported relationship.
6. Career coverage begins in 1990. If a source gives no dates for a relationship, retain it as undated evidence rather than inventing dates; it can still be reviewed for inclusion.

## Import strategy
Use Wikidata's search/API for entity discovery and narrowly scoped Wikidata Query Service queries for player-to-club relationships. Do not issue one enormous unbounded query. Import in small batches, store results locally, and validate a sample of well-known players before connecting this dataset to live game verification.

Wikidata documentation recommends using its search/API for text discovery and scoped SPARQL queries for structured relationships. Large result sets may require dumps or a separately hosted index; the public query service is not intended for broad fuzzy search.

## Current status
The initial Premier League discovery import is complete for the first 20 discovered clubs. The database contains thousands of Wikidata player-club relationships. This is a research dataset, not yet authoritative: undated links require review, same-name players may be distinct people, and the initial discovery query may miss historical clubs.

## Premier League first import
Run `npm run import:premier-league` locally from the repository root. Run `npm run audit:football-db` to produce a structural quality report. The importer queries Wikidata for player P54 links to clubs with Premier League participation (P118), including clubs linked to Premier League season entities (P3450), stores stable Wikidata IDs, names, nationality, and any available start/end years, and preserves undated links for review. It uses small result pages and pauses between requests. Do not use undated links as definitive proof of a post-1990 appearance.

## Game modes
`game-modes.json` defines All Leagues, Single League, and Single Country modes. These are configuration definitions only; the game interface and server-side mode enforcement still need to be connected after the first data import is validated.
