# MARS Cosmetics: Order & Inventory Sync Service (design assessment)

This is my Round 1 take-home submission for the Software Engineer role. The brief asked for a design only, so the repo contains no application code.

| File | Contents |
|---|---|
| [DESIGN.md](DESIGN.md) | **The design doc (4 pages).** Covers architecture, data model, order ingestion, inventory consistency (the last-unit race), stock push, failure handling, observability, scope, trade-offs, assumptions, stretch A and the AI usage note. |
| [diagrams/architecture.png](diagrams/architecture.png) | Architecture diagram. Its Mermaid source is [architecture.mmd](diagrams/architecture.mmd). |
| [design/reservation.md](design/reservation.md) | Optional pseudo-code for stock reservation, cancellation and the channel stock pusher, plus a sequence diagram of the race. |

To regenerate the diagram after editing the source:

```bash
npx @mermaid-js/mermaid-cli -i diagrams/architecture.mmd -o diagrams/architecture.png -w 2200 -b white
```
