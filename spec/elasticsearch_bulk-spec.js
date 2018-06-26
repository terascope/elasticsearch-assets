'use strict';

const esSender = require('../asset/elasticsearch_bulk');
const events = require('events');
const Promise = require('bluebird');
const eventEmitter = new events.EventEmitter();

describe('elasticsearch_bulk', () => {

    function makeClient() {
        return {
            bulk(results) {
                return Promise.resolve(results);
            }
        };
    }

    let opConfig = {};

    const context = {
        foundation: {
            getEventEmitter() {
                return eventEmitter;
            }
        },
        apis: {
            job_runner: { getOpConfig() { return opConfig;} },
            op_runner: { getClient() { return makeClient(); } }
        },
        logger: {
            error() {},
            info() {},
            warn() {}
        }
    };

    const { logger } = context

    it('has both a newSender and schema method', () => {
        expect(esSender.newProcessor).toBeDefined();
        expect(esSender.schema).toBeDefined();
        expect(typeof esSender.newProcessor).toEqual('function');
        expect(typeof esSender.schema).toEqual('function');
    });

    it('schema has defaults', () => {
        const defaults = esSender.schema();

        expect(defaults.size).toBeDefined();
        expect(defaults.size.default).toEqual(500);
    });

    it('returns a function', () => {
        const opConfig = { size: 100, multisend: false };
        const jobConfig = {};

        const sender = esSender.newProcessor(context, opConfig, jobConfig);

        expect(typeof sender).toEqual('function');
    });

   it('if no docs, returns a promise of passed in data', (done) => {
        const opConfig = { size: 100, multisend: false };
        const jobConfig = {};

        const sender = esSender.newProcessor(context, opConfig, jobConfig);
        Promise.resolve()
            .then(() =>  sender([], logger))
            .then((val) => {
                expect(val).toEqual([]);
                done();
            })
            .catch(fail)
            .finally(done);
    });

    it('does not split if the size is <= than 2 * size in opConfig', (done) => {
        // usually each doc is paired with metadata, thus doubling the size of incoming array,
        // hence we double size
        const opConfig = { size: 50, multisend: false };
        const jobConfig = {};
        const incData = [];

        for (let i = 0; i < 50; i += 1) {
            incData.push({ some: 'data' });
        }

        const sender = esSender.newProcessor(context, opConfig, jobConfig);
        Promise.resolve()
            .then(() =>  sender(incData, logger))
            .then((val) => {
                expect(val.length).toEqual(1);
                expect(val[0].body.length).toEqual(50);
            })
            .catch(fail)
            .finally(done);
    });

    it('it does split if the size is greater than 2 * size in opConfig', (done) => {
        // usually each doc is paired with metadata, thus doubling the size of incoming array,
        // hence we double size
        const opConfig = { size: 50, multisend: false };
        const jobConfig = {};
        const incData = [];

        for (let i = 0; i < 120; i += 1) {
            incData.push({ some: 'data' });
        }

        const sender = esSender.newProcessor(context, opConfig, jobConfig);
        Promise.resolve()
            .then(() =>  sender(incData, logger))
            .then((val) => {
                expect(val.length).toEqual(2);
                // length to index is off by 1
                expect(val[0].body.length).toEqual(101);
                expect(val[1].body.length).toEqual(19);
            })
            .catch(fail)
            .finally(done);
    });

    it('it splits the array up properly when there are delete operations (not a typical doubling of data)', (done) => {
        const opConfig = { size: 2, multisend: false };
        const jobConfig = {};
        const incData = [{ create: {} }, { some: 'data' }, { update: {} }, { other: 'data' }, { delete: {} }, { index: {} }, { final: 'data' }];
        const copy = incData.slice();

        const sender = esSender.newProcessor(context, opConfig, jobConfig);
        Promise.resolve()
            .then(() =>  sender(incData, logger))
            .then((val) => {
                expect(val.length).toEqual(2);
                // length to index is off by 1
                expect(JSON.stringify(val[0].body)).toEqual(JSON.stringify(copy.slice(0, 5)));
                expect(JSON.stringify(val[1].body)).toEqual(JSON.stringify(copy.slice(5)));
            })
            .catch(fail)
            .finally(done);
    });

    it('multisend will send based off of _id ', (done) => {
        const opConfig = {
            size: 5,
            multisend: true,
            connection_map: {
                a: 'default'
            }
        };

        const jobConfig = {};
        const incData = [{ create: { _id: 'abc' } }, { some: 'data' }, { update: { _id: 'abc' } }, { other: 'data' }, { delete: { _id: 'abc' } }, { index: { _id: 'abc' } }, { final: 'data' }];
        const copy = incData.slice();


        const sender = esSender.newProcessor(context, opConfig, jobConfig);
        Promise.resolve()
            .then(() =>  sender(incData, logger))
            .then((val) => {
                expect(val.length).toEqual(1);
                // length to index is off by 1
                expect(JSON.stringify(val[0].body)).toEqual(JSON.stringify(copy));
            })
            .catch(fail)
            .finally(done);
    });

    it('it can multisend to several places', (done) => {
        const opConfig = {
            size: 5,
            multisend: true,
            connection_map: {
                a: 'default',
                b: 'otherConnection'
            }
        };
        // multisend_index_append
        const jobConfig = {};
        const incData = [{ create: { _id: 'abc' } }, { some: 'data' }, { update: { _id: 'abc' } }, { other: 'data' }, { delete: { _id: 'bc' } }, { index: { _id: 'bc' } }, { final: 'data' }];
        const copy = incData.slice();


        const sender = esSender.newProcessor(context, opConfig, jobConfig);
        Promise.resolve()
            .then(() =>  sender(incData, logger))
            .then((val) => {
                expect(val.length).toEqual(2);
                // length to index is off by 1
                expect(JSON.stringify(val[0].body)).toEqual(JSON.stringify(copy.slice(0, 4)));
                expect(JSON.stringify(val[1].body)).toEqual(JSON.stringify(copy.slice(4)));
            })
            .catch(fail)
            .finally(done);
    });

    it('multisend_index_append will change outgoing _id ', (done) => {
        const opConfig = {
            size: 5,
            multisend: true,
            multisend_index_append: 'hello',
            connection_map: {
                a: 'default'
            }
        };

        const jobConfig = {};
        const incData = [{ create: { _id: 'abc' } }, { some: 'data' }, { update: { _id: 'abc' } }, { other: 'data' }, { delete: { _id: 'abc' } }, { index: { _id: 'abc' } }, { final: 'data' }];
        const copy = incData.slice();


        const sender = esSender.newProcessor(context, opConfig, jobConfig);
        Promise.resolve()
            .then(() =>  sender(incData, logger))
            .then((val) => {
                expect(val.length).toEqual(1);
                // length to index is off by 1
                expect(JSON.stringify(val[0].body)).toEqual(JSON.stringify(copy));
            })
            .catch(fail)
            .finally(done);
    });

    it('crossValidation makes sure connection_map is configured in sysconfig', () => {
        const badJob = {
            operations: [{
                _op: 'elasticsearch_bulk',
                multisend: true,
                connection_map: { a: 'connectionA', z: 'connectionZ' }
            }]
        };
        const goodJob = {
            operations: [{
                _op: 'elasticsearch_bulk',
                multisend: true,
                connection_map: { a: 'connectionA', b: 'connectionB' }
            }]
        };

        const sysconfig = {
            terafoundation: {
                connectors: {
                    elasticsearch: {
                        connectionA: 'connection Config',
                        connectionB: 'otherConnection Config'
                    }
                }
            }
        };
        context.sysconfig = sysconfig;
        const errorString = 'elasticsearch_bulk connection_map specifies a connection for [connectionZ] but is not found in the system configuration [terafoundation.connectors.elasticsearch]';

        expect(() => {
            opConfig = badJob.operations[0];
            esSender.crossValidation(context, badJob);
        }).toThrowError(errorString);

         expect(() => {
            opConfig = goodJob.operations[0];
            esSender.crossValidation(context, goodJob);
        }).not.toThrow();
    });
});
