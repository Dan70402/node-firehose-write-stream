'use strict';

var util = require('util'),
    assert = require('assert'),
    retryFn = require('retry-fn'),
    FlushWritable = require('flushwritable');

util.inherits(FirehoseWritable, FlushWritable);

/**
 * A simple stream wrapper around Firehose putRecordBatch
 *
 * @param {AWS.Firehose} client AWS Firehose client
 * @param {string} streamName Firehose stream name
 * @param {Object} options Options
 * @param {number} [options.highWaterMark=16] Number of items to buffer before writing.
 * Also the size of the underlying stream buffer.
 * @param {Number} [options.maxRetries=3] How many times to retry failed data before
 * rasing an error
 * @param {Number} [options.retryTimeout=100] How long to wait initially before
 * retrying failed records
 * @param {Object} [options.logger] Instance of a logger like bunyan or winston
 * @param {String} [options.delimiter] String separator between Firehose record
 */
function FirehoseWritable(client, streamName, options) {
    assert(client, 'client is required');
    assert(streamName, 'streamName is required');

    options = options || {};
    options.objectMode = true;

    FlushWritable.call(this, options);

    this.client = client;
    this.streamName = streamName;
    this.logger = options.logger || null;
    this.highWaterMark = options.highWaterMark || 16;
    this.maxRetries = options.maxRetries || 3;
    this.delimiter = options.delimiter || '';

    // The maximum number of items that can be written to Firehose in
    // one request is 500
    assert(this.highWaterMark <= 500, 'Max highWaterMark is 500');

    this.queue = [];
}

/**
 * Transform record into one accepted by Firehose
 *
 * @private
 * @param {Object} record
 * @return {Object}
 */
FirehoseWritable.prototype.transformRecord = function transformRecord(record) {
    return {
        Data: JSON.stringify(record) + this.delimiter
    };
};

/**
 * Write records in queue to Firehose
 *
 * @private
 * @param {Function} callback
 */
FirehoseWritable.prototype.writeRecords = function writeRecords(callback) {
    if (this.logger) {
        this.logger.debug('Writing %d records to Firehose', this.queue.length);
    }

    var records = this.queue.map(this.transformRecord.bind(this));

    this.client.putRecordBatch({
        Records: records,
        DeliveryStreamName: this.streamName
    }, function(err, response) {
        if (err) {
            return callback(err);
        }

        if (this.logger) {
            this.logger.info('Wrote %d records to Firehose',
                records.length - response.FailedPutCount);
        }

        if (response.FailedPutCount !== 0) {
            if (this.logger) {
                this.logger.warn('Failed writing %d records to Firehose',
                    response.FailedPutCount);
            }

            var failedRecords = [];

            response.RequestResponses.forEach(function(record, index) {
                if (record.ErrorCode) {
                    if (this.logger) {
                        this.logger.warn('Failed record with message: %s', record.ErrorMessage);
                    }

                    failedRecords.push(this.queue[index]);
                }
            }.bind(this));

            this.queue = failedRecords;

            return callback(new Error('Failed to write ' + failedRecords.length + ' records'));
        }

        this.queue = [];

        callback();
    }.bind(this));
};

/**
 * Flush method needed by the underlying stream implementation
 *
 * @private
 * @param {Function} callback
 * @return {undefined}
 */
FirehoseWritable.prototype._flush = function _flush(callback) {
    if (this.queue.length === 0) {
        return callback();
    }

    var retry = retryFn.bind(null, {
        retries: this.maxRetries,
        timeout: retryFn.fib(this.retryTimeout)
    });

    retry(this.writeRecords.bind(this), callback);
};

/**
 * Write method needed by the underlying stream implementation
 *
 * @private
 * @param {Object} record
 * @param {string} enc
 * @param {Function} callback
 * @returns {undefined}
 */
FirehoseWritable.prototype._write = function _write(record, enc, callback) {
    if (this.logger) {
        this.logger.debug('Adding to Firehose queue', { record: record });
    }

    this.queue.push(record);

    if (this.queue.length >= this.highWaterMark) {
        return this._flush(callback);
    }

    callback();
};

module.exports = FirehoseWritable;
