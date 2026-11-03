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
The schema and initial competition scope are created. No career records have been imported yet, so the game must not treat this empty database as authoritative.
