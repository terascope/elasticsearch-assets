import {
    ConvictSchema, AnyObject, ValidatedJobConfig,
    toNumber, isString, isNumber, getTypeOf,
    isNotNil, has
} from '@terascope/job-components';
import elasticAPI from '@terascope/elasticsearch-api';
import moment from 'moment';
// @ts-expect-error
import dateMath from 'datemath-parser';
import { dateOptions, IDType } from '@terascope/elasticsearch-asset-apis';
import { ElasticsearchReaderAPIConfig, DEFAULT_API_NAME } from './interfaces';
import { isValidIndex } from '../__lib/schema';

export const schema = {
    index: {
        doc: 'Which index to read from',
        default: null,
        format(val: unknown): void {
            isValidIndex(val);
        }
    },
    errorOnSizeTooBig: {
        doc: 'changes behavior of slice size expansion during fetch, setting this to false will make failing queries succeed but could result in missed data',
        default: true,
        format: Boolean
    },
    field: {
        doc: 'DEPRECATED: USE "id_field_name" INSTEAD. field to use for id_slicer if subslice_by_key is set to true',
        default: null,
        format: 'optional_String'
    },
    id_field_name: {
        doc: 'field to use for id_slicer',
        default: null,
        format: 'optional_String'
    },
    size: {
        doc: 'The limit to the number of docs pulled in a chunk, if the number of docs retrieved by the interval exceeds this number, it will cause the function to recurse to provide a smaller batch',
        default: 5000,
        format(val: unknown): void {
            if (!isNumber(val)) throw new Error(`Invalid parameter size, it must be of type number, was given ${getTypeOf(val)}`);
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
    key_range: {
        doc: 'if provided, slicer will only recurse on these given keys',
        default: null,
        format(val: unknown): void {
            if (val) {
                if (Array.isArray(val)) {
                    if (val.length === 0) throw new Error('Invalid key_range parameter, must be an array with length > 0');
                    if (!val.every(isString)) throw new Error('Invalid key_range parameter, must be an array of strings');
                } else {
                    throw new Error('Invalid key_range parameter, must be an array of strings');
                }
            }
        }
    },
    starting_key_depth: {
        doc: 'if provided, slicer will only produce keys with minimum length determined by this setting',
        default: 0,
        format(val: unknown): void {
            if (val) {
                if (isNumber(val)) {
                    if (val <= 0) {
                        throw new Error('Invalid starting_key_depth parameter, must be greater than zero');
                    }
                } else {
                    throw new Error('Invalid starting_key_depth parameter, must be a number');
                }
            }
        }
    },
};

export default class Schema extends ConvictSchema<ElasticsearchReaderAPIConfig> {
    validateJob(job: ValidatedJobConfig): void {
        const { logger } = this.context;

        const apiConfigs = job.apis.filter((config) => {
            const apiName = config._name;
            return apiName === DEFAULT_API_NAME || apiName.startsWith(`${DEFAULT_API_NAME}:`);
        });

        apiConfigs.forEach((apiConfig: AnyObject) => {
            if (apiConfig.field) {
                this.context.logger.warn(`For api "${apiConfig._name}", parameter "field" is deprecated and will be removed in later versions, please use "id_field_name" instead`);
                apiConfig.id_field_name = apiConfig.field;
                delete apiConfig.field;
            }

            const { connection, id_field_name, subslice_by_key } = apiConfig;

            const { connectors } = this.context.sysconfig.terafoundation;
            const endpointConfig = connectors['elasticsearch-next'][connection];

            if (endpointConfig == null) {
                throw new Error(`Could not find elasticsearch-next endpoint configuration for connection ${connection}`);
            }

            elasticAPI({}, logger).validateGeoParameters(apiConfig);

            if (subslice_by_key) {
                if (
                    id_field_name == null
                    || !isString(id_field_name)
                    || id_field_name.length === 0
                ) {
                    throw new Error('If subslice_by_key is set to true, the id_field_name parameter of the documents must also be set');
                }
            }

            if (apiConfig.key_range && job.slicers > apiConfig.key_range.length) {
                throw new Error('The number of slicers specified on the job cannot be more the length of key_range');
            }

            if (apiConfig.key_type === 'base64url') {
                if (job.slicers > 64) {
                    throw new Error('The number of slicers specified on the job cannot be more than 64');
                }
            }

            if (apiConfig.key_type === 'hexadecimal' || apiConfig.key_type === 'HEXADECIMAL') {
                if (job.slicers > 16) {
                    throw new Error('The number of slicers specified on the job cannot be more than 16');
                }
            }
        });
    }

    build(): AnyObject {
        return schema;
    }
}

function geoPointValidation(point: string | null):void {
    if (!point) return;

    if (typeof point !== 'string') throw new Error('Invalid geo_point, must be a string IF specified');

    const pieces = point.split(',');
    if (pieces.length !== 2) throw new Error(`Invalid geo_point, received ${point}`);
    const latitude = toNumber(pieces[0]);
    const longitude = toNumber(pieces[1]);

    if (latitude > 90 || latitude < -90) throw new Error(`Invalid latitude parameter, was given ${latitude}, should be >= -90 and <= 90`);
    if (longitude > 180 || longitude < -180) throw new Error(`Invalid longitude parameter, was given ${longitude}, should be >= -180 and <= 180`);
}

function checkUnits(unit: string | null):void {
    if (!unit) return;
    if (!isString(unit)) throw new Error('Invalid parameter, must be a string IF specified');

    const unitOptions = {
        mi: true,
        yd: true,
        ft: true,
        km: true,
        m: true
    };

    if (!has(unitOptions, unit)) throw new Error('Invalid unit type, did not have a proper unit of measurement (ie m, km, yd, ft)');
}

function validGeoDistance(distance: string | null):void {
    if (!distance) return;

    if (typeof distance !== 'string') throw new Error('Invalid geo_distance parameter, must be a string IF specified');

    const matches = distance.match(/(\d+)(.*)$/);
    if (!matches) throw new Error('Invalid geo_distance parameter, is formatted incorrectly');

    const number = matches[1];
    if (!number) throw new Error('Invalid geo_distance parameter, it must include a number');

    const unit = matches[2];
    checkUnits(unit);
}
