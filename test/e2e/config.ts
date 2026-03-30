import { SchemaValidator } from '@terascope/core-utils';
import { Terafoundation } from '@terascope/types';

interface E2eEnv {
    ASSET_ZIP_PATH: string;
    TERASLICE_HOST: string;
    TEST_TERASLICE: boolean;
    TEST_OPENSEARCH: boolean;
    [key: string]: any;
}

const e2eEnvSchema: Terafoundation.Schema<any> = {
    ASSET_ZIP_PATH: {
        default: undefined,
        format: 'required_string',
    },
    TERASLICE_HOST: {
        default: undefined,
        format: 'required_string',
    },
    TEST_TERASLICE: {
        default: undefined,
        format: Boolean,
    },
    TEST_OPENSEARCH: {
        default: undefined,
        format: Boolean,
    },
};

const validator = new SchemaValidator<E2eEnv>(
    e2eEnvSchema,
    'e2eEnvSchema',
    undefined,
    'allow'
);

const envConfig = validator.validate(process.env);
const { ASSET_ZIP_PATH, TERASLICE_HOST, TEST_TERASLICE } = envConfig;

export {
    ASSET_ZIP_PATH,
    TERASLICE_HOST,
    TEST_TERASLICE,
};
