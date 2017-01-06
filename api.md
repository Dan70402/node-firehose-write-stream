<a name="FirehoseWritable"></a>
## FirehoseWritable(client, streamName, options)
A simple stream wrapper around Firehose putRecordBatch

**Kind**: global function  

| Param | Type | Default | Description |
| --- | --- | --- | --- |
| client | <code>AWS.Firehose</code> |  | AWS Firehose client |
| streamName | <code>string</code> |  | Firehose stream name |
| options | <code>Object</code> |  | Options |
| [options.highWaterMark] | <code>number</code> | <code>16</code> | Number of items to buffer before writing. Also the size of the underlying stream buffer. |
| [options.maxRetries] | <code>Number</code> | <code>3</code> | How many times to retry failed data before rasing an error |
| [options.retryTimeout] | <code>Number</code> | <code>100</code> | How long to wait initially before retrying failed records |
| [options.logger] | <code>Object</code> |  | Instance of a logger like bunyan or winston |

**Kind**: instance method of <code>[FirehoseWritable](#FirehoseWritable)</code>  

| Param | Type | Description |
| --- | --- | --- |
| [record] | <code>Object</code> | Record |

