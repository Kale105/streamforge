# 01 - Event JSON round-trip

- Status: MVP laboratory guide
- Sequence: first implementation lesson
- Boundary: the event envelope only; no collector, sink, database, or broker

## Objective

Create confidence in the smallest transport contract: a Go Event can be encoded as JSON and decoded again without losing its required envelope fields or the source-specific payload. The learner writes the implementation and test; this guide supplies the reasoning and checks.

This is Gate 0 of the [Go concurrency track](go-concurrency-track.md). Keep it entirely synchronous: no goroutines, channels, parallel subtests, shared mutable fixtures, or asynchronous helpers. First prove the event contract in one goroutine.

The existing repository already contains internal/event/event.go with the intended field choices. Treat it as the starting point for the round-trip lesson, not as permission to expand the contract.

## Prerequisites

- Read sections 1-5 of [the MVP learning guide](../mvp-learning-guide.md), including its hint ladder.
- Read the event-contract and event-versioning sections of [the architecture design](../../architecture/design.md).
- Have Go 1.26.5, or the repository's compatible CI version, available through go version.
- Understand basic JSON objects, strings, timestamps, and arrays.
- Know that this laboratory is intentionally before PostgreSQL, Kafka, API keys, quotas, and external connectors.

## Conceptual explanation

An event has two layers:

1. The envelope says how to identify and route an observation: specification version, ID, source, type, time, and data.
2. The payload is source-specific JSON carried inside data.

The envelope is CloudEvents-inspired, but this MVP exercise does not claim full CloudEvents conformance. The useful lesson is the boundary: a transport can preserve metadata and carry data without understanding every provider field. If a provider renames home_team, the collector or normalizer should change; the transport envelope should not need to know that field.

json.RawMessage is the current contract because it represents already-encoded JSON and delays decoding. It prevents the envelope layer from turning every payload number into a generic Go value before a provider-specific parser has made the domain decision. A raw value is still required to be valid JSON when the complete event is marshaled.

Go 1.26.5 documentation now presents encoding/json/v2 as the preferred choice for new JSON work and describes encoding/json as the compatibility-oriented v1 package. Keep this MVP on the repository's existing encoding/json and json.RawMessage contract so the learner can understand the current code and preserve compatibility. Revisit a v2 migration only as a post-MVP promotion decision with explicit compatibility tests; do not mix it into this lesson.

### Comparison for a C# or Java developer

| Concern | C#/Java instinct | Go interpretation here |
|---|---|---|
| Envelope model | DTO/record/class with serializer attributes | Struct with exported fields and JSON tags |
| Raw payload | JsonElement, JsonNode, or a Jackson tree | json.RawMessage, a byte-backed encoded JSON value |
| Serialization failure | Often an exception | Return a value plus error and make the caller handle it |
| Field visibility | Serializer configuration can often see private members | Encoding follows exported Go fields; unexported fields are not part of the public JSON shape |
| Equality after a round-trip | May compare serialized text or object graphs | Decide whether the test means semantic values, valid payload, and preserved timestamp; whitespace is not identity |

The analogy is useful but incomplete: Go's standard JSON behavior is controlled by the type, field export, and tag rather than by a reflection-heavy application framework. Keep the transport type small enough that its public shape is obvious by inspection.

## Targeted resources

Read only the named portions and record the evidence you used:

- [encoding/json package overview](https://pkg.go.dev/encoding/json) - read RawMessage, Marshal, Unmarshal, and the v1/v2 migration note.
- [json.RawMessage](https://pkg.go.dev/encoding/json#RawMessage) - focus on “delay JSON decoding” and the marshal/unmarshal examples.
- [JSON and Go](https://go.dev/blog/json) - read “Encoding” and “Decoding,” especially how structs and generic values map to JSON.
- [CloudEvents specification](https://github.com/cloudevents/spec/blob/main/cloudevents/spec.md) - read the overview, event context, id, source, type, time, and data discussions. This is a primary specification, not a request to implement the full standard.
- [testing package](https://pkg.go.dev/testing) - read TestXxx, subtests, and the table-driven T.Run example.

## Design questions

Answer these in the engineering journal before editing code:

1. Why should the envelope carry a raw JSON value instead of a provider-specific struct?
2. What does a map[string]any make convenient, and what information or guarantees are weakened when the transport decodes into it?
3. Which parts of an event should remain stable when a provider changes one payload field?
4. Is an incrementing integer generated only in memory safe as a durable event ID after a process restart? What identity pair does the architecture recommend for a source event?
5. What is the difference between an envelope/specification version and a payload/schema version?
6. Is a round-trip test proving byte-for-byte preservation, semantic preservation, or only JSON validity? Which claim does this MVP actually need?

## Deliberately non-compilable pseudocode

This is control-flow notation, not Go. Do not paste it into a .go file and do not treat it as a completed solution.

~~~text
choose one event with every required envelope field populated
choose a small payload that is valid JSON but provider-shaped

encoded, encodeError := JSON-encode(event)
if encodeError exists:
    fail the test

assert encoded contains the six required JSON names
assert the payload portion is still valid JSON

decoded, decodeError := JSON-decode(encoded into an Event-shaped value)
if decodeError exists:
    fail the test

compare envelope values according to the contract
compare timestamps as instants, not formatted strings
assert decoded.data is valid JSON
inspect exported fields and reject accidental additions
~~~

The last check is intentionally architectural: adding a public field changes the wire contract even if all existing tests continue to pass.

## Implementation constraints

- Preserve the existing field names and JSON names: specversion, id, source, type, time, and data.
- Keep time.Time for the timestamp and json.RawMessage for the payload.
- Use the standard library only. Do not add UUID libraries, validation frameworks, schema registries, constructors, or provider DTOs.
- Do not edit the learning guide, README, application code, or any file outside the learner's event implementation/test area when performing the lesson.
- Do not migrate to encoding/json/v2 in this MVP step. Record it as a future compatibility decision instead.
- Keep all event-model and JSON tests synchronous. Concurrency begins only after this contract passes review.
- Test a semantic round-trip. Do not require the serialized bytes to have identical whitespace or object-key order.
- Keep any focused syntax request at hint level 4 and use an unrelated example domain. The mentor should not provide a complete event implementation.

## Common failure modes

- Unexported fields disappear. The type looks populated in Go, but the JSON object is missing fields because the serializer cannot see them.
- Tags are omitted or misspelled. Go emits field names that do not match the contract, or uses a spelling that downstream readers do not expect.
- Payload is double-encoded. A JSON object becomes a quoted JSON string because the learner stores its textual representation as a normal string instead of raw JSON.
- Generic decoding changes number behavior. Decoding into any is convenient, but generic numeric values do not provide the same typed intent as a provider parser.
- The test compares raw bytes. Formatting changes are mistaken for data loss.
- The test only checks Marshal success. A valid envelope can still lose a required field or contain invalid payload bytes if the assertions are too weak.
- The contract grows “for convenience.” Adding provider IDs, retry counts, or persistence metadata here couples the laboratory envelope to later layers.
- A v2 migration is started prematurely. New guidance is treated as a reason to change a working repository contract before compatibility behavior is defined.

## Tests to write

Create or extend a table-driven test in the event package. At minimum, cover:

- Every required JSON field name appears in the encoded object.
- The payload remains valid JSON after encoding and decoding.
- The decoded payload is still raw JSON rather than an already-selected provider/domain type.
- The timestamp survives the round-trip as the same instant.
- IDs, source, type, and specification version survive unchanged.
- An invalid raw payload is rejected when the complete event is encoded, or the test documents the exact standard-library behavior you observed.
- No accidental exported field has been added to the wire shape.

Use a small payload with nested data so the test catches accidental stringification. Keep test data unrelated to any real credentials or live provider response.

## Commands to run

Run these from the repository root:

~~~powershell
go fmt ./...
go test ./...
go vet ./...
~~~

If the test is hard to diagnose, narrow the loop with the event package and a test name, then rerun the full commands before review. Do not add a dependency merely to make assertions shorter.

## Acceptance criteria

- The event type has exactly the intended envelope fields and JSON names.
- A table-driven test demonstrates the required round-trip behaviors without comparing formatting artifacts.
- The test proves the payload stays valid JSON and is not double-encoded.
- go fmt ./..., go test ./..., and go vet ./... pass.
- The learner can explain why envelope identity and provider-record identity are different concerns.
- The learner can state why this lesson stays on v1 encoding/json despite current v2 guidance.
- No goroutine, channel, shared mutable fixture, or parallel test is required to prove the contract.

## Reflection questions

- Which guarantee did the test establish, and which guarantees did it deliberately leave for later layers?
- If the provider payload becomes a typed struct tomorrow, which package should own that interpretation?
- What would a consumer observe after a process restart if IDs were only a local counter?
- What compatibility test would you require before considering encoding/json/v2?

## Hint ladder and review submission

Reuse the project ladder from the [MVP learning guide](../mvp-learning-guide.md#hint-ladder): concept hint, direction hint, pseudocode hint, focused syntax hint on an unrelated domain, then rescue explanation after an attempted solution. Ask for the smallest level that unblocks the next experiment.

~~~text
Milestone: Event JSON round-trip
What I implemented:
What I expected:
What actually happened:
Commands I ran:
Errors or failing tests:
Decision I am unsure about:
My explanation of RawMessage and the envelope/payload boundary:
Evidence read and links:
~~~
