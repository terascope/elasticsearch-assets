import 'jest-extended';
import WindowState from '../../asset/src/elasticsearch_reader/window-state';

describe('WindowState', () => {
    it('can instantiate', () => {
        const numOfSlicers = 3;
        const state = new WindowState(numOfSlicers);

        expect(Object.keys(state._windowState)).toBeArrayOfSize(numOfSlicers);
        expect(state._allReachedLimit).toEqual(false);
        expect(state.checkin).toBeDefined();
    });

    it('can checkin for one slicer', () => {
        const numOfSlicers = 1;
        const state = new WindowState(numOfSlicers);

        expect(state.checkin(0)).toEqual(true);
        // as soon as one is done, it may continue
        expect(state.checkin(0)).toEqual(true);
    });

    it('can checkin for two slicer', () => {
        const numOfSlicers = 2;
        const state = new WindowState(numOfSlicers);

        expect(state.checkin(0)).toEqual(true);
        expect(state._allReachedLimit).toEqual(false);
        // We have not yet reset
        expect(state.checkin(0)).toEqual(false);

        expect(state.checkin(1)).toEqual(true);
        // we reset now
        expect(state.checkin(0)).toEqual(true);
        expect(state.checkin(1)).toEqual(true);
    });
});
