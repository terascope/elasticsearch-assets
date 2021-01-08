import { times } from '@terascope/utils';

interface WindowMeta {
    hasCalled: boolean;
    canRestart: boolean;
}

export class WindowState {
    _windowState: Record<number, WindowMeta> = {};

    constructor(numOfSlicers: number) {
        const keys = times(numOfSlicers);
        keys.forEach((key) => {
            this._windowState[key] = { hasCalled: false, canRestart: false };
        });
    }

    private _checkState(value: boolean) {
        return Object.values(this._windowState).every((meta) => meta.hasCalled === value);
    }

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
