# firehose-write-stream

Firehose writable stream that buffers up records

# Usage

Records written to the stream will buffer up until `highWaterMark` has
been reached, or the stream is closed, before writing to Firehose using
`putRecordBatch`.

## Failed items

Failed items will be retried up until `options.maxRetries` has been
reached. The initial timeout before the retry is set in
`options.retryTimeout` and it increases by the fibonnaci sequence.

## Logging

A [bunyan](https://www.npmjs.com/package/bunyan),
[winston](https://www.npmjs.com/package/winston) or similar logger
instance that have methods like `debug`, `error` and `info` may be
sent in as `options.logger` to the constructor.

# Example

```javascript
var FirehoseWritable = require('firehose-write-stream');

var stream = new FirehoseWritable(firehoseClient, 'streamName', {
  highWaterMark: 16,
  maxRetries: 3,
  retryTimeout: 100
});

inputStream.pipe(stream);
```

# API

See [api.md](api.md).

# License

MIT
