import {
    Logger, hasOwn, isString, isEmpty
} from '@terascope/utils';
import got, { OptionsOfJSONResponseBody, HTTPSOptions } from 'got';
import * as open from '@opensearch-project/opensearch';
import * as elastic from '@elastic/elasticsearch';
import { ConnectionOptions } from 'tls';
import { logWrapper } from './log-wrapper';

interface MetadataResponse {
    name: string;
    cluster_name: string;
    cluster_uuid: string;
    version: {
        distribution: string;
        number: string;
        build_type: string;
        build_hash: string;
        build_date: string;
        build_snapshot: boolean,
        lucene_version: string;
        minimum_wire_compatibility_version: string;
        minimum_index_compatibility_version: string;
    },
    tagline: string;
}

interface AgentOptions {
    keepAlive?: boolean;
    keepAliveMsecs?: number;
    maxSockets?: number;
    maxFreeSockets?: number;
}

interface NodeOptions {
    url: string;
    id?: string;
    agent?: AgentOptions;
    ssl?: ConnectionOptions;
    headers?: Record<string, any>;
    roles?: {
        master: boolean;
        data: boolean;
        ingest: boolean;
    }
}

export interface EndpointOptions extends open.ClientOptions {
    host?: string | string[];
    password?: string;
    username?: string;
}

function isNodeOptions(input: any): input is NodeOptions {
    if (input) {
        if (input?.url) {
            return true;
        }
    }
    return false;
}

function getUrl(config: EndpointOptions): string {
    if (config.host) {
        if (isString(config.host)) {
            return config.host;
        }

        if (Array.isArray(config.host)) {
            return config.host[0];
        }
    }

    if (config.node && isString(config.node)) {
        return config.node;
    }

    if (isNodeOptions(config.node)) {
        return config.node.url;
    }

    if (Array.isArray(config.node)) {
        const firstNode = config.node[0];
        if (isNodeOptions(firstNode)) {
            return firstNode.url;
        }
    }

    if (Array.isArray(config.nodes)) {
        const firstNode = config.nodes[0];
        if (isNodeOptions(firstNode)) {
            return firstNode.url;
        }
    }

    throw new Error('Could not find url in config');
}

interface AuthOptions {
    password?: string;
    username?: string;
    https?: HTTPSOptions;
}

function getAuth(config: EndpointOptions): AuthOptions {
    let password: string | undefined;
    let username: string | undefined;
    const https: HTTPSOptions = {};

    if (hasOwn(config, 'password')) {
        password = config.password;
    }

    if (hasOwn(config, 'username')) {
        username = config.username;
    }

    if (config.auth) {
        password = config.auth.password;
        username = config.auth.username;
    }

    if (config.cloud) {
        // TODO: might need id of cloud here??
        password = config.cloud.password;
        username = config.cloud.username;
    }

    if (config.ssl) {
        if (hasOwn(config, 'ssl.rejectUnauthorized')) {
            https.rejectUnauthorized = config.ssl.rejectUnauthorized;
        }

        if (config.ssl.cert) {
            https.certificate = config.ssl.cert;
        }

        if (config.ssl.key) {
            https.key = config.ssl.key;
        }
    }

    if (isNodeOptions(config.node) && config.node.ssl) {
        if (hasOwn(config, 'node.ssl.rejectUnauthorized')) {
            https.rejectUnauthorized = config.node.ssl.rejectUnauthorized;
        }

        if (config.node.ssl.cert) {
            https.certificate = config.node.ssl.cert;
        }

        if (config.node.ssl.key) {
            https.key = config.node.ssl.key;
        }
    }

    if (Array.isArray(config.node)) {
        const firstNode = config.node[0];
        if (isNodeOptions(firstNode) && firstNode.ssl) {
            if (hasOwn(config, 'node.ssl.rejectUnauthorized')) {
                https.rejectUnauthorized = firstNode.ssl.rejectUnauthorized;
            }

            if (firstNode.ssl.cert) {
                https.certificate = firstNode.ssl.cert;
            }

            if (firstNode.ssl.key) {
                https.key = firstNode.ssl.key;
            }
        }
    }

    if (config.nodes) {
        const firstNode = config.nodes[0];
        if (isNodeOptions(firstNode) && firstNode.ssl) {
            if (hasOwn(config, 'node.ssl.rejectUnauthorized')) {
                https.rejectUnauthorized = firstNode.ssl.rejectUnauthorized;
            }

            if (firstNode.ssl.cert) {
                https.certificate = firstNode.ssl.cert;
            }

            if (firstNode.ssl.key) {
                https.key = firstNode.ssl.key;
            }
        }
    }

    if ((password && username == null) || (password == null && username)) {
        throw new Error('Parameters username and password must be specified together');
    }

    return {
        ...(password && { password }),
        ...(username && { username }),
        ...(!isEmpty(https) && { https })
    };
}

function getHeaders(config: EndpointOptions): Record<string, any> {
    let headers: Record<string, any> = {};

    if (config.headers) {
        headers = {
            ...config.headers
        };
    }

    if (isNodeOptions(config.node) && config.node.headers) {
        headers = {
            ...config.node.headers
        };
    }

    if (Array.isArray(config.node)) {
        const firstNode = config.node[0];
        if (isNodeOptions(firstNode) && firstNode.headers) {
            headers = {
                ...firstNode.headers
            };
        }
    }

    if (Array.isArray(config.nodes)) {
        const firstNode = config.nodes[0];
        if (isNodeOptions(firstNode) && firstNode.headers) {
            headers = {
                ...firstNode.headers
            };
        }
    }

    return headers;
}

// TODO: handle cloud endpoint
export function createEndpointQuery(config: EndpointOptions): OptionsOfJSONResponseBody {
    let endpoint = getUrl(config);
    const { username, password, https } = getAuth(config);
    const headers = getHeaders(config);

    // make sure url includes a protocol, follow link for why
    // https://nodejs.org/api/url.html#special-schemes
    if (!endpoint.includes('http')) {
        endpoint = `http://${endpoint}`;
    }

    const url = new URL(endpoint);

    if (password && username) {
        url.username = username;
        url.password = password;
    }

    const gotConfig: OptionsOfJSONResponseBody = {
        url: url.href,
        responseType: 'json',
        ...(https && { https }),
        ...{ headers }
    };

    return gotConfig;
}

export async function createClient(config: Record<string, any>, logger: Logger) {
    const gotOptions = createEndpointQuery(config);

    try {
        const { body } = await got<MetadataResponse>(gotOptions);

        if (body.version.distribution === 'opensearch') {
            // TODO: clean this up
            const openConfig = {
                ...config,
                node: gotOptions.url as string
            };
            const client = new open.Client(openConfig);
            const meta = {
                client_type: 'opensearch',
                version: body.version.number
            };
            // @ts-expect-error
            client.__meta = meta;

            return {
                client,
                log: logWrapper(logger),
            };
        }

        const elasticConfig = {
            ...config,
            node: gotOptions.url as string
        };
        const client = new elastic.Client(elasticConfig);
        const meta = {
            client_type: 'elasticsearch',
            version: body.version.number
        };
        // @ts-expect-error
        client.__meta = meta;
        return {
            client,
            log: logWrapper(logger)
        };
    } catch (err) {
        throw new Error(`Could not connect to ${gotOptions.url} to determine the correct elasticsearch client type`);
    }
}
