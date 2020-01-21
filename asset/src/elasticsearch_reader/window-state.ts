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
        if (!this._windowState[id]) {
            this._windowState[id] = true;

            const allDone = this._checkState(true);

            if (allDone) {
                for (const key of Object.keys(this._windowState)) {
                    this._windowState[key] = false;
                }
            }
            return true;
        }
        return false;
    }
}
