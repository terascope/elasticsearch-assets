import {
    ConvictSchema,
    ValidatedJobConfig,
    getOpConfig,
    toNumber,
    AnyObject,
    getTypeOf,
    isString,
    isNil,
    isNumber,
    isNotNil
} from '@terascope/job-components';
import elasticAPI from '@terascope/elasticsearch-api';
import moment from 'moment';
// @ts-expect-error
import dateMath from 'datemath-parser';
import { ESReaderConfig } from './interfaces';
import { dateOptions } from '../elasticsearch_reader_api/elasticsearch_date_slicer/helpers';
import { IDType } from '../id_reader/interfaces';
import { DEFAULT_API_NAME } from '../elasticsearch_reader_api/interfaces';

export function checkIndex(index: string|undefined): void {
    if (!isString(index)) throw new Error('Invalid index parameter, must be of type string');
    if (index.length === 0) throw new Error('Invalid index parameter, must not be an empty string');
    if (index.match(/[A-Z]/)) throw new Error('Invalid index parameter, must be lowercase');
}

export const schema = {
    index: {
        doc: 'Which index to read from',
        default: null,
        format(val: unknown): void {
            if (isNotNil(val)) checkIndex(val as any);
        }
    },
    field: {
        doc: 'field to use for id_slicer if subslice_by_key is set to true',
        default: '',
        format: 'optional_String'
    },
    size: {
        doc: 'The limit to the number of docs pulled in a chunk, if the number of docs retrieved by the interval exceeds this number, it will cause the function to recurse to provide a smaller batch',
        default: 5000,
        format(val: unknown): void {
            if (!isNumber(val)) throw new Error(`Invalid parameter size, it must be of type number, was given ${getTypeOf(val)}`);
            if (isNaN(val)) throw new Error('Invalid size parameter, must be a number');
            if (val <= 0) throw new Error('Invalid size parameter, must be greater than zero');
        }
    },
    start: {
        doc: 'The start date (ISOString or in ms) to which it will read from ',
        default: null,
        format(val: unknown): void {
            if (val) {
                if (typeof val === 'string' || typeof val === 'number') {
                    if (!moment(new Date(val)).isValid()) {
                        try {
                            dateMath.parse(val);
                        } catch (err) {
                            throw new Error(`Invalid start parameter, value: "${val}" cannot be coerced into a proper date`);
                        }
                    }
                } else {
                    throw new Error('Invalid start parameter, must be a string or number IF specified');
                }
            }
        }
    },
    end: {
        doc: 'The end date (ISOString or in ms) to which it will read to',
        default: null,
        format(val: unknown): void {
            if (val) {
                if (typeof val === 'string' || typeof val === 'number') {
                    if (!moment(new Date(val)).isValid()) {
                        try {
                            dateMath.parse(val);
                        } catch (err) {
                            throw new Error(`Invalid end parameter, value: "${val}" cannot be coerced into a proper date`);
                        }
                    }
                } else {
                    throw new Error('Invalid end parameter, must be a string or number IF specified');
                }
            }
        }
    },
    interval: {
        doc: 'The time interval in which it will read from, the number must be separated from the unit of time by an underscore. The unit of time may be months, weeks, days, hours, minutes, seconds, milliseconds or their appropriate abbreviations',
        default: 'auto',
        format(val: unknown): void {
            if (!isString(val)) throw new Error(`Invalid parameter interval, it must be of type string, was given ${getTypeOf(val)}`);
            if (val === 'auto') return;
            const regex = /(\d+)(\D+)/i;
            const interval = regex.exec(val);
            if (!interval) throw new Error('Invalid date interval, it is not formatted correctly');
            dateOptions(interval[2]);
        }
    },
    date_field_name: {
        doc: 'field name where the date of the doc is located',
        default: null,
        format: 'optional_String'
    },
    query: {
        doc: 'You may place a lucene query here, and the slicer will use it when making slices',
        default: '',
        format: 'optional_String'
    },
    fields: {
        doc: 'used to only return fields that you are interested in',
        default: null,
        format(val: unknown): void {
            if (isNotNil(val)) {
                if (!Array.isArray(val)) throw new Error('Fields parameter must be an array');
                if (!val.every(isString)) throw new Error('Invalid fields paramter, the values listed in the fields array must be of type string');
            }
        }
    },
    delay: {
        doc: 'used for persistent',
        default: '30s',
        format(val: unknown): void {
            if (!isString(val)) throw new Error(`Invalid parameter interval, it must be of type string, was given ${getTypeOf(val)}`);
            if (val === 'auto') return;
            const regex = /(\d+)(\D+)/i;
            const interval = regex.exec(val);
            if (!interval) throw new Error('Invalid date interval, it is not formatted correctly');
            dateOptions(interval[2]);
        }
    },
    subslice_by_key: {
        doc: 'determine if slice should be further divided up by id if slice is to too big',
        default: false,
        format: Boolean
    },
    subslice_key_threshold: {
        doc: 'After sub-slicing as far as possible, the docs threshold to initiate division by keys',
        default: 50000,
        format(val: unknown): void {
            if (!isNumber(val)) throw new Error(`Invalid parameter subslice_key_threshold, it must be of type number, was given ${getTypeOf(val)}`);
            if (isNaN(val)) throw new Error('Invalid subslice_key_threshold parameter, must be a number');
            if (val <= 0) throw new Error('Invalid subslice_key_threshold parameter, must be greater than zero');
        }
    },
    key_type: {
        doc: 'The type of id used in index',
        default: 'base64url',
        format: Object.keys(IDType)
    },
    type: {
        doc: 'The type of the records in the index, only used if subslice_by_key is set to true and in elasticsearch <= v5 ',
        default: null,
        format: 'optional_String'
    },
    time_resolution: {
        doc: 'indicate if data reading has second or millisecond resolutions',
        default: 's',
        format(val: unknown): string {
            const obj = {
                seconds: 's',
                second: 's',
                s: 's',
                milliseconds: 'ms',
                millisecond: 'ms',
                ms: 'ms'
            };
            if (!isString(val)) throw new Error(`Invalid parameter time_resolution, it must be of type string, was given ${getTypeOf(val)}`);
            if (!obj[val]) throw new Error('Invalid time_resolution,  must be set in either "s"[seconds] or "ms"[milliseconds]');

            return obj[val];
        }
    },
    geo_field: {
        doc: 'field name where the geolocation data is located',
        default: '',
        format: 'optional_String'
    },
    geo_box_top_left: {
        doc: 'used for a bounding box query',
        default: '',
        format: geoPointValidation
    },
    geo_box_bottom_right: {
        doc: 'used for a bounding box query',
        default: '',
        format: geoPointValidation
    },
    geo_point: {
        doc: 'used for a geo distance query',
        default: '',
        format: geoPointValidation
    },
    geo_distance: {
        doc: 'used for a geo distance query',
        default: '',
        format: validGeoDistance
    },
    geo_sort_point: {
        doc: 'used for sorting geo queries',
        default: '',
        format: geoPointValidation
    },
    geo_sort_order: {
        doc: 'used for sorting geo queries',
        default: '',
        format: (val: unknown): void => {
            if (val) {
                const options = { asc: true, desc: true };
                if (typeof val !== 'string') throw new Error('Invalid geo_sort_order parameter, must be a string IF specified');
                if (!options[val]) throw new Error('If geo_sort_order is specified it must be either "asc" or "desc"');
            }
        }
    },
    geo_sort_unit: {
        doc: 'used for sorting geo queries',
        default: '',
        format: checkUnits
    },
    connection: {
        doc: 'Name of the elasticsearch connection to use when sending data.',
        default: 'default',
        format: 'optional_String'
    },
    api_name: {
        doc: 'name of api to be used by elasticsearch reader',
        default: DEFAULT_API_NAME,
        format: (val: unknown): void => {
            if (!isString(val)) throw new Error(`Invalid parameter api_name, it must be of type string, was given ${getTypeOf(val)}`);
            if (!val.includes(DEFAULT_API_NAME)) throw new Error('Invalid parameter api_name, it must be an elasticsearch_reader_api');
        }
    }
};

export default class Schema extends ConvictSchema<ESReaderConfig> {
    validateJob(job: ValidatedJobConfig): void {
        const { logger } = this.context;
        const opConfig = getOpConfig(job, 'elasticsearch_reader');
        if (opConfig == null) throw new Error('Could not find elasticsearch_reader operation in jobConfig');

        elasticAPI({}, logger).validateGeoParameters(opConfig);

        if (job.lifecycle === 'persistent') {
            if (opConfig.interval === 'auto') {
                throw new Error('Invalid interval parameter, must be manually set while job is in persistent mode');
            }

            if (opConfig.delay === 'auto') {
                throw new Error('Invalid delay parameter, must be manually set while job is in persistent mode');
            }
        }

        const {
            api_name, ...newConfig
        } = opConfig;
        if (!Array.isArray(job.apis)) job.apis = [];
        const ElasticReaderAPI = job.apis.find((jobApi) => jobApi._name === api_name);

        if (isNil(ElasticReaderAPI)) {
            checkIndex(opConfig.index);
            if (isNil(opConfig.date_field_name)) throw new Error(`Invalid parameter date_field_name, must be of type string, got ${getTypeOf(opConfig.date_field_name)}`);

            job.apis.push({
                _name: DEFAULT_API_NAME,
                ...newConfig
            });
        }

        const opConnection = ElasticReaderAPI ? ElasticReaderAPI.connection : opConfig.connection;
        const subsliceByKey = ElasticReaderAPI
            ? ElasticReaderAPI.subslice_by_key
            : opConfig.subslice_by_key;

        const configField = ElasticReaderAPI
            ? ElasticReaderAPI.field
            : opConfig.field;

        const { connectors } = this.context.sysconfig.terafoundation;
        const endpointConfig = connectors.elasticsearch[opConnection];
        const apiVersion = endpointConfig.apiVersion
            ? toNumber(endpointConfig.apiVersion.charAt(0))
            : 6;

        if (subsliceByKey) {
            const configType = ElasticReaderAPI ? ElasticReaderAPI.type : opConfig.type;
            if (apiVersion <= 5 && (configType == null || !isString(configType) || configType.length === 0)) throw new Error(`For elasticsearch apiVersion ${endpointConfig.apiVersion}, a type must be specified`);
            if (apiVersion > 5 && (configField == null || !isString(configField) || configField.length === 0)) throw new Error('If subslice_by_key is set to true, the field parameter of the documents must also be set');
        }
    }

    build(): AnyObject {
        return schema;
    }
}

function geoPointValidation(point: string | null):void {
    if (point) {
        if (typeof point !== 'string') throw new Error('Invalid geo_point, must be a string IF specified');

        const pieces = point.split(',');
        if (pieces.length !== 2) throw new Error(`Invalid geo_point, received ${point}`);
        const latitude = toNumber(pieces[0]);
        const longitude = toNumber(pieces[1]);

        if (latitude > 90 || latitude < -90) throw new Error(`Invalid latitude parameter, was given ${latitude}, should be >= -90 and <= 90`);
        if (longitude > 180 || longitude < -180) throw new Error(`Invalid longitude parameter, was given ${longitude}, should be >= -180 and <= 180`);
    }
}

function checkUnits(unit: string | null):void {
    if (unit) {
        const unitOptions = {
            mi: true,
            yd: true,
            ft: true,
            km: true,
            m: true
        };
        if (typeof unit !== 'string') throw new Error('Invalid parameter, must be a string IF specified');
        if (!unitOptions[unit]) throw new Error('Invalid unit type, did not have a proper unit of measurement (ie m, km, yd, ft)');
    }
}

function validGeoDistance(distance: string | null):void {
    if (distance) {
        if (typeof distance !== 'string') throw new Error('Invalid geo_distance parameter, must be a string IF specified');
        const matches = distance.match(/(\d+)(.*)$/);
        if (!matches) throw new Error('Invalid geo_distance paramter, is formatted incorrectly');

        const number = matches[1];
        if (!number) throw new Error('Invalid geo_distance paramter, it must include a number');

        const unit = matches[2];
        checkUnits(unit);
    }
}
