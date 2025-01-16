import { times } from '@terascope/utils';

interface WindowMeta {
    hasCalled: boolean;
    canRestart: boolean;
}

/**
 * This is used track the slicer state when running persistent mode
*/
export class WindowState {
    _windowState = new Map<number, WindowMeta>();

    constructor(numOfSlicers: number) {
        times(numOfSlicers, (id) => {
            this._windowState.set(id, { hasCalled: false, canRestart: false });
        });
    }

    private _checkState(value: boolean) {
        return this._windowState.values().every((meta) => meta.hasCalled === value);
    }

    /**
     * Call this with the slicer id to ensure that
     * the slicer is correctly processing. Returns true
     * if all of the slicers are complete and need to restart
     * processing
    */
    checkin(id: number): boolean {
        const meta = this._windowState.get(id);
        if (!meta) {
            throw new Error(`Window metadata for id ${id} is not defined`);
        }
        let bool = false;
        meta.hasCalled = true;

        const allDone = this._checkState(true);

        if (allDone) {
            for (const value of this._windowState.values()) {
                value.canRestart = true;
                value.hasCalled = false;
            }
        }

        if (meta.canRestart) {
            meta.canRestart = false;
            meta.hasCalled = true;
            bool = true;
        }

        return bool;
    }
}
