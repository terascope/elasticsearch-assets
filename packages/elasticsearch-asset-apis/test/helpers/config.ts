const { TEST_INDEX_PREFIX = 'es_assets_', ELASTICSEARCH_HOST = 'http://localhost:9200' } = process.env;
let version = process.env.ELASTICSEARCH_VERSION;
if (!version || version.charAt(0) === '6') version = '6.5';
if (version.charAt(0) === '7') version = '7.x';
if (version.charAt(0) === '5') version = '5.6';
const ELASTICSEARCH_VERSION = version;
export { TEST_INDEX_PREFIX, ELASTICSEARCH_HOST, ELASTICSEARCH_VERSION };
