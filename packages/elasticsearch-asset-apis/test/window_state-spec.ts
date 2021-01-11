import 'jest-extended';
import { WindowState } from '../src';

describe('WindowState', () => {
    it('can instantiate', () => {
        const numOfSlicers = 3;
        const state = new WindowState(numOfSlicers);

        expect(Object.keys(state._windowState)).toBeArrayOfSize(numOfSlicers);
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
        // 0 reached limit, should not continue
        expect(state.checkin(0)).toEqual(false);
        // the other slicer has not been called yet
        expect(state.checkin(0)).toEqual(false);
        // we reset now, can immediately continue 1
        expect(state.checkin(1)).toEqual(true);
        // cannot continue until 0 is called
        expect(state.checkin(1)).toEqual(false);
        // we reset which means I can immediately continue
        expect(state.checkin(0)).toEqual(true);
        // 1 is done we we are complete, can continue
        expect(state.checkin(1)).toEqual(true);
        expect(state.checkin(1)).toEqual(false);
        // we reset again
        expect(state.checkin(0)).toEqual(true);
    });
});
