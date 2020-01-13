import {
    ConvictSchema, ValidatedJobConfig, getOpConfig, toNumber,
} from '@terascope/job-components';
import elasticApi from '@terascope/elasticsearch-api';
import moment from 'moment';
// @ts-ignore
import dateMath from 'datemath-parser';
import { ESReaderConfig } from './interfaces';
import { dateOptions } from '../helpers';
import { IDType } from '../id_reader/interfaces';

export default class Schema extends ConvictSchema<ESReaderConfig> {
    validateJob(job: ValidatedJobConfig) {
        const { logger } = this.context;
        const opConfig = getOpConfig(job, 'elasticsearch_reader');
        if (opConfig == null) throw new Error('Could not find elasticsearch_reader operation in jobConfig');

        if (opConfig.subslice_by_key) {
            if (!opConfig.field) {
                throw new Error('If subslice_by_key is set to true, the field parameter of the documents must also be set');
            }
        }

        elasticApi({}, logger).validateGeoParameters(opConfig);

        if (job.lifecycle === 'persistent') {
            if (opConfig.interval === 'auto') {
                throw new Error('Invalid interval parameter, must be manually set while job is in persistent mode');
            }

            if (opConfig.delay === 'auto') {
                throw new Error('Invalid delay parameter, must be manually set while job is in persistent mode');
            }
        }
    }

    build() {
        return {
            index: {
                doc: 'Which index to read from',
                default: '',
                format(val: any) {
                    if (typeof val !== 'string') {
                        throw new Error('Invalid index parameter, must be of type string');
                    }

                    if (val.length === 0) {
                        throw new Error('Invalid index parameter, must not be an empty string');
                    }

                    if (val.match(/[A-Z]/)) {
                        throw new Error('Invalid index parameter, must be lowercase');
                    }
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
                format(val: any) {
                    if (isNaN(val)) {
                        throw new Error('Invalid size parameter, must be a number');
                    } else if (val <= 0) {
                        throw new Error('Invalid size parameter, must be greater than zero');
                    }
                }
            },
            start: {
                doc: 'The start date (ISOstring or in ms) to which it will read from ',
                default: null,
                format(val: any) {
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
                doc: 'The end date (ISOstring or in ms) to which it will read to',
                default: null,
                format(val: any) {
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
                doc: 'The time interval in which it will read from, the number must be separated from the unit of time by an underscore. The unit of time may be months, weeks, days, hours, minutes, seconds, millesconds or their appropriate abbreviations',
                default: 'auto',
                format(val: any) {
                    if (val === 'auto') return;
                    const regex = /(\d+)(\D+)/i;
                    const interval = regex.exec(val);
                    if (!interval) throw new Error('Invalid date interval, it is not formatted correctly');
                    dateOptions(interval[2]);
                }
            },
            preserve_id: {
                doc: 'Set to true to add the _id field of the doc set to the hidden metadata on the documents returned',
                default: false,
                format: Boolean
            },
            date_field_name: {
                doc: 'field name where the date of the doc is located',
                default: '',
                format: 'required_String'
            },
            query: {
                doc: 'You may place a lucene query here, and the slicer will use it when making slices',
                default: '',
                format: 'optional_String'
            },
            fields: {
                doc: 'used to only return fields that you are interested in',
                default: null,
                format(val: any) {
                    function isString(elem: any) {
                        return typeof elem === 'string';
                    }
                    if (val === null) {
                        return true;
                    }
                    if (!Array.isArray(val)) {
                        throw new Error('Fields parameter must be an array');
                    }
                    if (!val.every(isString)) {
                        throw new Error('Invalid fields paramter, the values listed in the fields array must be of type string');
                    }
                    return true;
                }
            },
            delay: {
                doc: 'used for persistent',
                default: '30s',
                format: 'optional_String'
            },
            subslice_by_key: {
                doc: 'determine if slice should be further divided up by id if slice is to too big',
                default: false,
                format: Boolean
            },
            subslice_key_threshold: {
                doc: 'After subslicing as far as possible, the docs threshold to initiate division by keys',
                default: 50000,
                format(val: any) {
                    if (isNaN(val)) {
                        throw new Error('Invalid subslice_key_threshold parameter, must be a number');
                    } else if (val <= 0) {
                        throw new Error('Invalid subslice_key_threshold parameter, must be greater than zero');
                    }
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
                format(val: any) {
                    const obj = {
                        seconds: 's',
                        second: 's',
                        s: 's',
                        milliseconds: 'ms',
                        millisecond: 'ms',
                        ms: 'ms'
                    };
                    if (!obj[val]) {
                        throw new Error('Invalid time_resolution,  must be set in either "s"[seconds] or "ms"[milliseconds]');
                    } else {
                        return obj[val];
                    }
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
                format: (val: any) => {
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
                default: 'default'
            }
        };
    }
}

function geoPointValidation(point: string | null) {
    if (point) {
        if (typeof point !== 'string') throw new Error('Invalid geo_point, must be a string IF specified');

        const pieces = point.split(',');
        if (pieces.length !== 2) throw new Error(`Invalid geo_point, received ${point}`);
        const latitude = toNumber(pieces[0]);
        const longitutde = toNumber(pieces[1]);

        if (latitude > 90 || latitude < -90) throw new Error(`Invalid latitude parameter, was given ${latitude}, should be >= -90 and <= 90`);
        if (longitutde > 180 || longitutde < -180) throw new Error(`Invalid longitutde parameter, was given ${longitutde}, should be >= -180 and <= 180`);
    }
}

function checkUnits(unit: string | null) {
    if (unit) {
        const unitOptions = {
            mi: true,
            yd: true,
            ft: true,
            km: true,
            m: true
        };
        if (typeof unit !== 'string') throw new Error('Invalid parameter, must be a string IF specified');
        if (!unitOptions[unit]) throw new Error('Invalid unit type, did not have a proper unit of measuerment (ie m, km, yd, ft)');
    }
}

function validGeoDistance(distance: string | null) {
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
