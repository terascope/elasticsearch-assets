import { DataEntity } from '@terascope/utils';
import { SliceResults } from 'teraslice-test-harness';

export function getListOfIds(data: any[], key: string, depth = 1): Map<string, number> {
    const map = new Map<string, number>();

    for (const record of data) {
        const keyValue = DataEntity.isDataEntity(record) ? record.getKey() : record[key];
        const newKey = keyValue.slice(0, depth) as string;

        if (map.has(newKey)) {
            const oldNum = map.get(newKey) as number;
            map.set(newKey, oldNum + 1);
        } else {
            map.set(newKey, 1);
        }
    }

    return map;
}

export function getTotalSliceCounts(sliceResults: SliceResults[]): number {
    return sliceResults.reduce((count, list) => {
        const newCount = count + list.data.length;
        return newCount;
    }, 0);
}
