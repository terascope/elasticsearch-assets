export const legacySchema = {
    host: {
        doc: 'A list of hosts to connect to',
        default: ['127.0.0.1:9200']
    },
    sniffOnStart: {
        doc: 'Sniff hosts on start up',
        default: false
    },
    sniffOnConnectionFault: {
        doc: 'Sniff hosts on connection failure',
        default: false
    },
    apiVersion: {
        describe: 'The API version, currently we only support 5.6, 6.5 and 7.0',
        default: '6.5'
    },
    requestTimeout: {
        doc: 'Request timeout',
        default: 120000,
        format: 'duration'
    },
    deadTimeout: {
        doc: 'Timeout before marking a connection as "dead"',
        default: 30000,
        format: 'duration'
    },
    maxRetries: {
        doc: 'Maximum retries for a failed request',
        default: 3
    }
};
