import ESBulkSender from '../src/elasticsearch_bulk/processor.js';
import ESBulkSenderSchema from '../src/elasticsearch_bulk/schema.js';

import ESDateFetcher from '../src/elasticsearch_reader/fetcher.js';
import ESDateSlicer from '../src/elasticsearch_reader/slicer.js';
import ESDateSchema from '../src/elasticsearch_reader/schema.js';

import ESReaderAPI from '../src/elasticsearch_reader_api/api.js';
import ESReaderAPISchema from '../src/elasticsearch_reader_api/schema.js';

import ESSenderAPI from '../src/elasticsearch_sender_api/api.js';
import ESSenderAPISchema from '../src/elasticsearch_sender_api/schema.js';

import SpacesReaderAPI from '../src/spaces_reader_api/api.js';
import SpacesReaderAPISchema from '../src/spaces_reader_api/schema.js';

import SpacesFetcher from '../src/spaces_reader/fetcher.js';
import SpacesSlicer from '../src/spaces_reader/slicer.js';
import SpacesSchema from '../src/spaces_reader/schema.js';

import IDFetcher from '../src/id_reader/fetcher.js';
import IDSlicer from '../src/id_reader/slicer.js';
import IDSchema from '../src/id_reader/schema.js';

import ESStateStorageAPI from '../src/elasticsearch_state_storage/api.js';
import ESStateStorageSchema from '../src/elasticsearch_state_storage/schema.js';

// Teraslice Asset Registry
// This was added to enable esbuild based bundled assets.
export const ASSETS = {
    elasticsearch_bulk: {
        Processor: ESBulkSender,
        Schema: ESBulkSenderSchema
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
};
