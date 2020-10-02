import {
    APIConfig,
    APIFactoryRegistry,
    AnyObject,
    LifeCycle,
    SlicerRecoveryData
} from '@terascope/job-components';
import Reader from './elasticsearch-api';
import WindowState from './window-state';
import { ESReaderOptions } from '../elasticsearch_reader/interfaces';
import { IDType } from '../id_reader/interfaces';

export const DEFAULT_API_NAME = 'elasticsearch_reader_api';

export interface ElasticsearchReaderAPIConfig extends ESReaderOptions, APIConfig {}
export type ElasticReaderFactoryAPI = APIFactoryRegistry<Reader, ElasticsearchReaderAPIConfig>
export interface DateSlicerArgs {
    lifecycle: LifeCycle,
    slicerID: number,
    numOfSlicers: number,
    recoveryData?: SlicerRecoveryData[];
    windowState?: WindowState,
    startTime?: Date | string
    hook?: (args: AnyObject) => Promise<void>
}

export interface DateSlicerConfig {
    lifecycle: LifeCycle,
    slicerID: number,
    numOfSlicers: number,
    recoveryData: SlicerRecoveryData[],
    windowState?: WindowState,
    startTime?: Date | string,
    hook?: (args: AnyObject) => Promise<void>
}

export interface IDSlicerArgs {
    lifecycle: LifeCycle,
    slicerID: number,
    numOfSlicers: number,
    recoveryData?: SlicerRecoveryData[];
    keyType: IDType;
    keyRange?: string[];
    startingKeyDepth: number;
    IDFieldName: string | null;
}

export interface IDSlicerConfig {
    lifecycle: LifeCycle,
    slicerID: number,
    numOfSlicers: number,
    recoveryData: SlicerRecoveryData[],
    keyType: IDType;
    keyRange?: string[];
    startingKeyDepth: number,
    IDFieldName: string | null;
}
