import { times } from '@terascope/job-components';

export default class WindowState {
    _windowState: Record<number, boolean> = {};
    _allReachedLimit = false;

    constructor(numOfSlicers: number) {
        const keys = times(numOfSlicers);
        keys.forEach((key) => {
            this._windowState[key] = false;
        });
    }

    private _checkState(value: boolean) {
        return Object.values(this._windowState).every((bool) => bool === value);
    }

    checkin(id: number): boolean {
        if (!this._allReachedLimit) {
            this._windowState[id] = true;
            const allDone = this._checkState(true);
            if (allDone) {
                this._allReachedLimit = true;
                return true;
            }
        }
        return false;
    }

    slicerIsRestarting(id: number): void {
        this._windowState[id] = false;
        const allRestarted = this._checkState(false);
        if (allRestarted) {
            this._allReachedLimit = false;
        }
    }
}
