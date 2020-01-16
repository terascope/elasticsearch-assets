import 'jest-extended';
import WindowState from '../../asset/src/elasticsearch_reader/window-state';

describe('WindowState', () => {
    it('can instantiate', () => {
        const numOfSlicers = 3;
        const state = new WindowState(numOfSlicers);

        expect(Object.keys(state._windowState)).toBeArrayOfSize(numOfSlicers);
        expect(state._allReachedLimit).toEqual(false);
        expect(state.checkin).toBeDefined();
        expect(state.isRestarting).toBeDefined();
    });

    it('can checkin for one slicer', () => {
        const numOfSlicers = 1;
        const state = new WindowState(numOfSlicers);

        expect(state.checkin(0)).toEqual(true);
        expect(state._allReachedLimit).toEqual(true);
        // We have not yet reset
        expect(state.checkin(0)).toEqual(false);
    });

    it('can checkin for two slicer', () => {
        const numOfSlicers = 2;
        const state = new WindowState(numOfSlicers);

        expect(state.checkin(0)).toEqual(false);
        expect(state._allReachedLimit).toEqual(false);
        // We have not yet reset
        expect(state.checkin(0)).toEqual(false);

        expect(state.checkin(1)).toEqual(true);
        expect(state._allReachedLimit).toEqual(true);
        // We have not yet reset
        expect(state.checkin(1)).toEqual(false);
    });

    it('can restart', () => {
        const numOfSlicers = 2;
        const state = new WindowState(numOfSlicers);

        expect(state.checkin(0)).toEqual(false);
        expect(state._allReachedLimit).toEqual(false);
        // We have not yet reset
        expect(state.checkin(0)).toEqual(false);
        expect(state._windowState[0]).toEqual(true);

        expect(state.checkin(1)).toEqual(true);
        expect(state._allReachedLimit).toEqual(true);
        // We have not yet reset
        expect(state.checkin(1)).toEqual(false);
        expect(state.checkin(0)).toEqual(false);

        state.slicerIsRestarting(0);
        expect(state._windowState[0]).toEqual(false);
        expect(state._allReachedLimit).toEqual(true);

        state.slicerIsRestarting(1);
        expect(state._windowState[1]).toEqual(false);
        expect(state._allReachedLimit).toEqual(false);
    });
});
