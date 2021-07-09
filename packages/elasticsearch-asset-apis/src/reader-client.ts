import type { DataFrame } from '@terascope/data-mate';
import type { AnyObject, DataEntity } from '@terascope/utils';
import type {
    SearchParams
} from 'elasticsearch';
import { DataTypeConfig } from '@terascope/types';

/**
 * This is used for as the internal elasticsearch
 * client in reader, this is designed as an abstraction
 * so that spaces client will work with it is own specific
 * optimizations
*/
export interface ReaderClient {
    /**
     * Counts the number of documents for a given query
    */
    count(query: SearchParams): Promise<number>;

    /**
     * Searches for documents for a given query
    */
    search(
        query: SearchParams,
        useDataFrames: false,
        typeConfig?: DataTypeConfig
    ): Promise<DataEntity[]>;
    search(
        query: SearchParams,
        useDataFrames: true,
        typeConfig: DataTypeConfig
    ): Promise<DataFrame>;
    search(
        query: SearchParams,
        useDataFrames: boolean,
        typeConfig?: DataTypeConfig
    ): Promise<DataEntity[]|DataFrame>;

    /**
     * Used to make a custom search request
     *
     * @note this API is subject to change
    */
    _searchRequest(query: SearchParams, fullResponse?: false): Promise<DataEntity[]>;
    _searchRequest(query: SearchParams, fullResponse: true): Promise<unknown>;
    _searchRequest(query: SearchParams, fullResponse?: boolean): Promise<DataEntity[]|unknown>;

    /**
     * Gets the elasticsearch major server version,
     * this will be used to format the search parameters
    */
    getESVersion(): number;

    /**
     * Verify that the cluster is up,
     * internally this will use esClient.version() probably
    */
    verify(): Promise<void>;

    /**
     * Used to determine the max window size
    */
    getSettings(index: string): Promise<SettingResults>;
}

export interface SettingResults {
    [key: string]: {
        settings: {
            'index.max_result_window': number
        },
        defaults: AnyObject
    }
}
