# NBA source policy review

- Reviewed: 2026-07-17
- Purpose: Decide which sources can be bundled with the self-hosted platform
- Note: This is an engineering policy review, not legal advice. Recheck each source before release because terms and access policies change.

## Decision standard

A connector may be bundled by default only when all of the following are documented:

1. Automated access is affirmatively supported or written permission has been obtained.
2. The selected access method follows the source's API, bot, rate-limit, and user-agent policies.
3. The data license or written permission supports the platform's storage and intended output.
4. Attribution and share-alike requirements can be satisfied.
5. The connector does not bypass authentication, access controls, CAPTCHAs, or anti-bot systems.

`robots.txt` alone is not permission to copy, store, or redistribute data. It is one operational instruction considered alongside terms, API policy, and the data license.

## Approved candidates

| Source | Collection method | Useful NBA data | Permission basis | Default-pack decision |
|---|---|---|---|---|
| Wikidata | Wikidata API, SPARQL, or dumps | Teams, players, venues, league identity, aliases, external IDs | Structured data is CC0; automated access is supported subject to API and user-agent policies | Approved for a reference-data connector |
| Wikipedia | MediaWiki API or dumps | Season pages, team history, awards, contextual metadata | Content is generally CC BY-SA; automated API access is supported subject to Wikimedia policies | Conditional: approved only with attribution and share-alike handling |
| FiveThirtyEight data repository | Licensed file download, not page scraping | Historical NBA Elo, forecasts, RAPTOR, and related published datasets | Repository states datasets are CC BY 4.0 unless otherwise noted | Approved for optional historical imports with attribution; not live data |

## Live-feed candidates

| Source | Live capability | Free allowance | Decision |
|---|---|---|---|
| BetStack | NBA scores refreshed every 60 seconds through the events/results API | One request per 60 seconds; described as free forever for personal and commercial use | Recommended development feed; obtain written confirmation before bundling it into a competing normalized API |
| SportScore | NBA/basketball live scores with responses cached for 60 seconds | Approximately 10,000 requests per day with mandatory visible attribution | Not suitable for this project's public API without permission because its terms prohibit rehosting the feed as a competing sports-data service |
| Big Balls Sports Data | NBA REST scores with a 15-second live cache | 1,000 requests per day, or 2,000 with GitHub | Development candidate only; terms prohibit reselling the raw feed and are marked as a draft pending public launch |
| API-Basketball | Live basketball endpoint coverage | 100 requests per day | Useful for occasional testing, but insufficient for continuous one-minute polling |

### Recommended first live integration: BetStack

Use `GET /api/v1/events?league=basketball_nba` with an API key supplied through configuration. Poll no more often than once every 60 seconds, persist the provider's timestamps and raw response, and publish only changed observations to Kafka.

Before shipping the connector enabled by default, ask BetStack to confirm in writing that an open-source self-hosted application may cache, normalize, retain, and expose derived NBA score records through its own API. Their published documentation permits personal and commercial use, but it does not explicitly address a competing data API.

### Wikimedia requirements

- Prefer the supported APIs or dumps over HTML scraping.
- Send a descriptive user agent containing the project and contact information.
- Follow throttling and rate-limit responses.
- Do not distribute traffic across identities to evade limits.
- Preserve the applicable license and attribution information with imported records.

Suggested user agent:

```text
OpenSportsDataBot/0.1 (+https://github.com/OWNER/REPOSITORY; contact@example.com)
```

## Rejected for the bundled scraper pack

| Source | Decision | Reason |
|---|---|---|
| Basketball Reference and Sports Reference | Reject without written permission | Their current data-use policy says not to create websites or tools from scraped Sports Reference data, and their terms prohibit using content to create a competing or substitute database/service |
| NBA.com and stats.nba.com | Do not bundle without written permission or a specifically granted feed | Public technical endpoints and browser accessibility are not an affirmative grant to scrape and redistribute them through a replacement API |
| ESPN | Reject without express written permission | Disney's terms, which ESPN links as governing terms, prohibit automated access, monitoring, copying, or extraction for web scraping or building datasets/databases |
| CBS Sports, Yahoo Sports, and similar commercial score sites | Do not bundle until separately approved in writing | No affirmative permission suitable for a competing self-hosted data API was verified during this review |
| Random Kaggle or GitHub datasets derived from restricted sites | Reject by default | A repository license from an uploader does not necessarily establish that the upstream collection and redistribution were authorized |

## Candidate that requires direct permission

For a genuine live NBA HTML scraper, seek written permission from a smaller publisher, fan-operated statistics site, open-data project, or league-adjacent community source. The request should state:

- Exact pages or feeds collected
- Maximum request rate and expected schedule
- User-agent and contact details
- Fields stored
- Whether raw HTML is retained
- Whether normalized data is exposed publicly
- Whether commercial use is possible
- Attribution shown in the API and documentation
- How the source can disable or contact the connector maintainer

Written permission should be stored as project governance metadata, while private correspondence remains outside the public repository if necessary.

## Recommended initial bundled pack

1. `wikidata-basketball-reference-data`: CC0 identities, aliases, teams, players, and venues.
2. `wikipedia-nba-metadata`: optional CC BY-SA contextual metadata with required attribution.
3. `fivethirtyeight-nba-history`: optional historical import with CC BY 4.0 attribution.
4. `balldontlie-nba`: prebuilt free-key API connector for current teams and games.
5. `fixture-nba-live`: deterministic live-game simulator for development and demos.
6. A real live HTML connector only after obtaining and documenting affirmative permission.

During development, add `betstack-nba-live` as an opt-in connector. Promote it to the default bundled pack only after the redistribution clarification described above.

This pack provides useful out-of-box reference, current, and historical data while the project pursues permission for a bundled live scraper.

## Sources reviewed

- [Wikidata licensing](https://www.wikidata.org/wiki/Wikidata:Licensing)
- [Wikidata data access](https://www.wikidata.org/wiki/Wikidata:Download)
- [Wikimedia API Usage Guidelines](https://foundation.wikimedia.org/wiki/Policy:Wikimedia_Foundation_API_Usage_Guidelines/en)
- [Wikimedia User-Agent Policy](https://foundation.wikimedia.org/wiki/Policy:Wikimedia_Foundation_User-Agent_Policy)
- [Wikimedia Terms of Use](https://foundation.wikimedia.org/wiki/Policy:Terms_of_Use/en)
- [FiveThirtyEight data repository and license](https://github.com/fivethirtyeight/data)
- [Sports Reference data-use policy](https://www.sports-reference.com/data_use.html)
- [Sports Reference Terms of Use](https://www.sports-reference.com/termsofuse.html)
- [NBA.com Terms of Use](https://www.nba.com/termsofuse)
- [ESPN Terms of Use link](https://support.espn.com/hc/en-us/articles/360035445091-Terms-of-Use)
- [Disney Terms of Use governing ESPN products](https://disneytermsofuse.com/english/)
- [BetStack API documentation](https://api.betstack.dev/docs)
- [BetStack free live-data description](https://api.betstack.dev/)
- [SportScore developer documentation](https://sportscore.com/developers/)
- [SportScore API terms](https://sportscore.com/developers/terms/)
- [Big Balls Sports Data NBA API](https://bigballsdata.com/nba-api)
- [API-Basketball plans](https://api-sports.io/sports/basketball)
