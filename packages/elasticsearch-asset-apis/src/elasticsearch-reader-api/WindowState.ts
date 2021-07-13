import { times } from '@terascope/utils';

interface WindowMeta {
    hasCalled: boolean;
    canRestart: boolean;
}

/**
 * This is used track the slicer state when running persistent mode
*/
export class WindowState {
    _windowState: Record<number, WindowMeta> = {};

    constructor(numOfSlicers: number) {
        times(numOfSlicers, (id) => {
            this._windowState[id] = { hasCalled: false, canRestart: false };
        });
    }

    private _checkState(value: boolean) {
        return Object.values(this._windowState).every((meta) => meta.hasCalled === value);
    }

    /**
     * Call this with the slicer id to ensure that
     * the slicer is correctly processing. Returns true
     * if all of the slicers are complete and need to restart
     * processing
    */
    checkin(id: number): boolean {
        const meta = this._windowState[id];
        let bool = false;
        meta.hasCalled = true;

        const allDone = this._checkState(true);

        if (allDone) {
            for (const key of Object.keys(this._windowState)) {
                this._windowState[key].canRestart = true;
                this._windowState[key].hasCalled = false;
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
