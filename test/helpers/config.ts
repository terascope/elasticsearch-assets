const {
    TEST_INDEX_PREFIX = 'es_assets_',
    ELASTICSEARCH_HOST = 'http://localhost:9200',
    ELASTICSEARCH_API_VERSION = '6.5',
    ELASTICSEARCH_VERSION = '6.8.6',
    OPENSEARCH_PORT = '9200',
    OPENSEARCH_HOSTNAME = `http://localhost:${OPENSEARCH_PORT}`,
    OPENSEARCH_USER = 'admin',
    OPENSEARCH_PASSWORD = 'admin',
    OPENSEARCH_VERSION = '1.3.0',
    OPENSEARCH_HOST = `http://${OPENSEARCH_USER}:${OPENSEARCH_PASSWORD}@${OPENSEARCH_HOSTNAME}:${OPENSEARCH_PORT}`
} = process.env;

export {
    TEST_INDEX_PREFIX,
    ELASTICSEARCH_API_VERSION,
    ELASTICSEARCH_HOST,
    ELASTICSEARCH_VERSION,
    OPENSEARCH_VERSION,
    OPENSEARCH_HOST
};
