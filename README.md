# RSP.js

An RDF Stream Processing Library for Javascript built on top of [N3.js](https://github.com/rdfjs/N3.js/) and [Comunica](https://comunica.dev/)

## Installation

The library is available on npm. To install it, run the following command:

```
npm i rsp-js
```

## Usage

You can define a query using the RSP-QL syntax. An example query is shown below:

```ts
let query = `PREFIX : <https://rsp.js/>
    REGISTER RStream <output> AS
    SELECT *
    FROM NAMED WINDOW :w1 ON STREAM :stream1 [RANGE 10 STEP 2]
    WHERE{
        WINDOW :w1 { ?s ?p ?o}
    }`;
```

You can then create an instance of the RSPEngine and pass the query to it, as shown below:

```ts
let rspEngine = new RSPEngine(query);
```

You can add stream elements to the RSPEngine using the `add` method. The method takes in a stream element and a timestamp

```ts
stream.add(quad(
                namedNode('https://rsp.js/test_subject_'),
                namedNode('http://rsp.js/test_property'),
                namedNode('http://rsp.js/test_object'),
                defaultGraph(),
            ), timestamp_value);
```

In the following example, we will use a function `generate_data` to mock the stream.

```ts
import { RSPEngine, RDFStream } from "rsp4js";

async function RSP() {
  let query = `PREFIX : <https://rsp.js/>
    REGISTER RStream <output> AS
    SELECT *
    FROM NAMED WINDOW :w1 ON STREAM :stream1 [RANGE 10 STEP 2]
    WHERE{
        WINDOW :w1 { ?s ?p ?o}
    }`;

  let rspEngine = new RSPEngine(query);
  let stream = rspEngine.getStream("https://rsp.js/stream1");
  let emitter = rspEngine.register();
  let results = new Array<string>();
  emitter.on("RStream", (object: any) => {
    console.log("received results");
    results.push(object.bindings.toString());
  });
  if (stream) {
    generate_data(10, [stream]);
  }
  console.log(results);
}

RSP();
```

### Window timing and out-of-order events

The default `RSPEngine` behavior uses `max_delay = 0`, trailing window
timestamps, and half-open window bounds `[open, close)`. In-order streams keep
the upstream timing behavior while results report the actual window bounds in
`timestamp_from` and `timestamp_to`.

To accept bounded out-of-order events, provide a non-negative `max_delay` in
the same timestamp units as stream events:

```ts
const rspEngine = new RSPEngine(query, { max_delay: 5000 });
```

An event that arrives earlier than the current event-time reference is
accepted when its lateness is at most `max_delay`; later events are discarded.
Accepted late events do not move event time or the watermark backwards. The
watermark advances with in-order input to `event_timestamp - max_delay`, and a
window is eligible for its closing report when its close is covered by that
watermark.

Window starts are aligned to the configured `start_time` origin using floor
rounding. With the default origin of zero, an event at an exact boundary belongs
to the following window, so adjacent windows do not overlap at their close
boundary.

Results also include `logical_trigger_time` and `window_semantics`. The default
`trailing` semantics use the window close as the logical timestamp. Centered
semantics can be selected explicitly:

```ts
const rspEngine = new RSPEngine(query, { window_semantics: "centered" });
```

Centered results retain their actual `timestamp_from` and `timestamp_to` bounds
while reporting the midpoint as `logical_trigger_time`.

```ts
const N3 = require("n3");
const { DataFactory } = N3;
const { namedNode, defaultGraph, quad } = DataFactory;

async function generate_data(num_events: number, rdfStreams: RDFStream[]) {
  for (let i = 0; i < num_events; i++) {
    rdfStreams.forEach((stream: any) => {
      const stream_element = quad(
        namedNode("https://rsp.js/test_subject_" + i),
        namedNode("http://rsp.js/test_property"),
        namedNode("http://rsp.js/test_object"),
        defaultGraph()
      );
      stream.add(stream_element, i);
    });
  }
}
```

## Current Features:

- RSP-QL support
- Support multiple windows
- Support for stream and static data joins
