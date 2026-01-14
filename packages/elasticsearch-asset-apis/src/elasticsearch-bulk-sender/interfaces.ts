export interface ElasticsearchSenderConfig {
    size: number;
    _connection: string;
    index: string;
    delete?: boolean;
    update?: boolean;
    update_retry_on_conflict?: number;
    update_fields?: string[];
    upsert?: boolean;
    create?: boolean;
    script_file?: string;
    script?: string;
    script_params?: Record<string, any>;
    _key?: string;
}

export interface ScriptConfig {
    file?: string;
    source?: string;
    params?: Record<string, any>;
}

export interface UpdateConfig {
    upsert?: Record<string, any>;
    doc?: Record<string, any>;
    script?: ScriptConfig;
}
