import ESBulkSender from './elasticsearch_bulk/processor.js';
import ESBulkSenderSchema from './elasticsearch_bulk/schema.js';

import ESDateFetcher from './elasticsearch_reader/fetcher.js';
import ESDateSlicer from './elasticsearch_reader/slicer.js';
import ESDateSchema from './elasticsearch_reader/schema.js';

import ESReaderAPI from './elasticsearch_reader_api/api.js';
import ESReaderAPISchema from './elasticsearch_reader_api/schema.js';

import ESSenderAPI from './elasticsearch_sender_api/api.js';
import ESSenderAPISchema from './elasticsearch_sender_api/schema.js';

import SpacesReaderAPI from './spaces_reader_api/api.js';
import SpacesReaderAPISchema from './spaces_reader_api/schema.js';

import SpacesFetcher from './spaces_reader/fetcher.js';
import SpacesSlicer from './spaces_reader/slicer.js';
import SpacesSchema from './spaces_reader/schema.js';

import IDFetcher from './id_reader/fetcher.js';
import IDSlicer from './id_reader/slicer.js';
import IDSchema from './id_reader/schema.js';

import ESStateStorageAPI from './elasticsearch_state_storage/api.js';
import ESStateStorageSchema from './elasticsearch_state_storage/schema.js';

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
