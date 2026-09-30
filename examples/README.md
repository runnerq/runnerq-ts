# Examples

These examples use the new separate-input schema. Use a fresh database, not an existing Go SDK database.

From the SDK directory:

```sh
npm install
npm run build
docker compose -f examples/docker-compose.yml up -d
node --experimental-strip-types examples/01-hello-workflow/main.ts
```

On Node releases where type stripping is enabled by default, the flag is optional. Alternatively compile the examples with TypeScript. `DATABASE_URL` overrides the local connection string.

- `01-hello-workflow`: two checkpointed steps, typed results, graceful cleanup.
- `02-crash-and-resume`: terminate and restart the process after its charge checkpoint commits; the business key reattaches to the same activity. Lease expiry/reaping recovers the killed execution. A new run after completion returns the stored result.
- `03-fan-out`: children and a durable join with only one execution slot.
- `04-signals-and-sleep`: buffered signals, persisted deadlines, and a durable timer longer than the handler timeout.
- `05-cloud`: a worker connected to RunnerQ Cloud with the conductor agent. It places an order every 3 seconds (some retry, some fail) until Ctrl+C, so Fleet and Activities in the console have something to show. Set `RUNNERQ_CONDUCTOR_KEY` to an agent key from the console's Connect page; `RUNNERQ_CLOUD_URL` defaults to a local conductord (`http://localhost:8088`), e.g. `wss://cloud.runnerq.dev` for the hosted Cloud.

The examples simulate external effects with console output. Real effects must use provider idempotency keys where possible: a crash between an effect and its checkpoint commit can repeat the effect.
