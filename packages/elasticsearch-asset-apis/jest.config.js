export default {
    verbose: true,
    testEnvironment: 'node',
    setupFilesAfterEnv: ['jest-extended/all'],
    collectCoverage: true,
    coverageReporters: ['json', 'lcov', 'text', 'html'],
    coverageDirectory: 'coverage',
    extensionsToTreatAsEsm: ['.ts'],
    testMatch: [
        '<rootDir>/test/**/*-spec.{ts,js}',
        '<rootDir>/test/*-spec.{ts,js}',
    ],
    moduleNameMapper: {
        '^(\\.{1,2}/.*)\\.js$': '$1',
    },
    transform: {
        '\\.[jt]sx?$': ['ts-jest',
            {
                isolatedModules: true,
                tsconfig: './tsconfig.json',
                diagnostics: true,
                pretty: true,
                useESM: true
            }]
    },
    testTimeout: 60 * 1000
};
