'use strict';

var chai = require('chai'),
    sinon = require('sinon'),
    sinonChai = require('sinon-chai'),
    streamArray = require('stream-array'),
    FirehoseWritable = require('../'),
    recordsFixture = require('./fixture/records'),
    successResponseFixture = require('./fixture/success-response'),
    errorResponseFixture = require('./fixture/failed-response'),
    writeFixture = require('./fixture/write-fixture');

chai.use(sinonChai);

var expect = chai.expect;

describe('FirehoseWritable', function() {
    beforeEach(function() {
        this.sinon = sinon.sandbox.create();

        this.client = {
            putRecordBatch: sinon.stub()
        };

        this.stream = new FirehoseWritable(this.client, 'streamName', {
            highWaterMark: 6
        });
    });

    afterEach(function() {
        this.sinon.restore();
    });

    describe('constructor', function() {
        it('should throw error on missing client', function() {
            expect(function() {
                new FirehoseWritable();
            }).to.Throw(Error, 'client is required');
        });

        it('should throw error on missing streamName', function() {
            expect(function() {
                new FirehoseWritable({});
            }).to.Throw(Error, 'streamName is required');
        });

        it('should throw error on highWaterMark above 500', function() {
            expect(function() {
                new FirehoseWritable({}, 'test', { highWaterMark: 501 });
            }).to.Throw(Error, 'Max highWaterMark is 500');
        });
    });

    describe('_write', function() {
        it('should write to firehose when stream is closed', function(done) {
            this.client.putRecordBatch.yields(null, successResponseFixture);

            this.stream.on('finish', function() {
                expect(this.client.putRecordBatch).to.have.been.calledOnce;

                expect(this.client.putRecordBatch).to.have.been.calledWith({
                    Records: writeFixture,
                    DeliveryStreamName: 'streamName'
                });

                done();
            }.bind(this));

            streamArray(recordsFixture)
                .pipe(this.stream);
        });

        it('should do nothing if there is nothing in the queue when the stream is closed', function(done) {
            this.client.putRecordBatch.yields(null, successResponseFixture);

            this.stream.on('finish', function() {
                expect(this.client.putRecordBatch).to.have.been.calledOnce;

                done();
            }.bind(this));

            for (var i = 0; i < 6; i++) {
                this.stream.write(recordsFixture);
            }

            this.stream.end();
        });

        it('should buffer records up to highWaterMark', function(done) {
            this.client.putRecordBatch.yields(null, successResponseFixture);

            for (var i = 0; i < 4; i++) {
                this.stream.write(recordsFixture[0]);
            }

            this.stream.write(recordsFixture[0], function() {
                expect(this.client.putRecordBatch).to.not.have.been.called;

                this.stream.write(recordsFixture[0], function() {
                    expect(this.client.putRecordBatch).to.have.been.calledOnce;

                    done();
                }.bind(this));
            }.bind(this));
        });

        it('should emit error on Firehose error', function(done) {
            this.client.putRecordBatch.yields('Fail');

            this.stream.on('error', function(err) {
                expect(err).to.eq('Fail');

                done();
            });

            this.stream.end({ foo: 'bar' });
        });

        it('should emit error on records errors', function(done) {
            this.client.putRecordBatch.yields(null, errorResponseFixture);

            this.stream.on('error', function(err) {
                expect(err).to.be.ok;

                done();
            });

            this.stream.end({ foo: 'bar' });
        });

        it('should retry failed records', function(done) {
            this.client.putRecordBatch.yields(null, errorResponseFixture);
            this.client.putRecordBatch.onCall(2).yields(null, successResponseFixture);

            this.stream.on('finish', function() {
                expect(this.client.putRecordBatch).to.have.been.calledThrice;

                expect(this.client.putRecordBatch.secondCall).to.have.been.calledWith({
                    Records: [
                        writeFixture[1],
                        writeFixture[3]
                    ],
                    DeliveryStreamName: 'streamName'
                });

                done();
            }.bind(this));

            streamArray(recordsFixture).pipe(this.stream);
        });
    });
});
