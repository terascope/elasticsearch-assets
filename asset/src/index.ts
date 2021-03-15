// TODO: types are wrong for AssetRepository
// import { AssetRepository } from '@terascope/job-components';

import ESBulkSender from '../src/elasticsearch_bulk/processor';
import ESBulkSenderSchema from '../src/elasticsearch_bulk/schema';

import ESDataGenFetcher from '../src/elasticsearch_data_generator/fetcher';
import ESDataGenSlicer from '../src/elasticsearch_data_generator/slicer';
import ESDataGenSchema from '../src/elasticsearch_data_generator/schema';

import ESDateFetcher from '../src/elasticsearch_reader/fetcher';
import ESDateSlicer from '../src/elasticsearch_reader/slicer';
import ESDateSchema from '../src/elasticsearch_reader/schema';

import ESReaderAPI from '../src/elasticsearch_reader_api/api';
import ESReaderAPISchema from '../src/elasticsearch_reader_api/schema';

import ESSenderAPI from '../src/elasticsearch_sender_api/api';
import ESSenderAPISchema from '../src/elasticsearch_sender_api/schema';

import SpacesReaderAPI from '../src/spaces_reader_api/api';
import SpacesReaderAPISchema from '../src/spaces_reader_api/schema';

import SpacesFetcher from '../src/spaces_reader/fetcher';
import SpacesSlicer from '../src/spaces_reader/slicer';
import SpacesSchema from '../src/spaces_reader/schema';

import IDFetcher from '../src/id_reader/fetcher';
import IDSlicer from '../src/id_reader/slicer';
import IDSchema from '../src/id_reader/schema';

import ESStateStorageAPI from '../src/elasticsearch_state_storage/api';
import ESStateStorageSchema from '../src/elasticsearch_state_storage/schema';

const assetRepo = {
    ASSETS: {
        elasticsearch_bulk: {
            Processor: ESBulkSender,
            Schema: ESBulkSenderSchema
        },
        elasticsearch_data_generator: {
            Fetcher: ESDataGenFetcher,
            Slicer: ESDataGenSlicer,
            Schema: ESDataGenSchema
        },
        elasticsearch_reader: {
            Fetcher: ESDateFetcher,
            Slicer: ESDateSlicer,
            Schema: ESDateSchema
        },
        spaces_reader: {
            Fetcher: SpacesFetcher,
            Slicer: SpacesSlicer,
            Schema: SpacesSchema
        },
        id_reader: {
            Fetcher: IDFetcher,
            Slicer: IDSlicer,
            Schema: IDSchema
        },
        elasticsearch_reader_api: {
            API: ESReaderAPI,
            Schema: ESReaderAPISchema
        },
        elasticsearch_sender_api: {
            API: ESSenderAPI,
            Schema: ESSenderAPISchema
        },
        spaces_reader_api: {
            API: SpacesReaderAPI,
            Schema: SpacesReaderAPISchema
        },
        elasticsearch_state_storage: {
            API: ESStateStorageAPI,
            Schema: ESStateStorageSchema
        },
    }
};

export default assetRepo;
