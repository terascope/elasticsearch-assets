import { SplitKeyTracker, IDType } from '../../src/index.js';

describe('id-helpers', () => {
    describe('SplitKeyTracker', () => {
        it('can create a tracker', () => {
            const tracker = new SplitKeyTracker(IDType.hexadecimal);

            expect(tracker).toBeDefined();
            expect(tracker.split).toBeFunction();
        });

        it('will throw if using a wrong tracker type', () => {
            expect(
                () => new SplitKeyTracker('something' as unknown as IDType)
            ).toThrow();
        });

        describe(`${IDType.hexadecimal}`, () => {
            it('can correctly split in batches', () => {
                const tracker = new SplitKeyTracker(IDType.hexadecimal);

                const batch1 = tracker.split(5);
                const batch2 = tracker.split(5);
                const batch3 = tracker.split(6);
                const batch4 = tracker.split(5);

                expect(batch1).toEqual('0-4');
                expect(batch2).toEqual('5-9');
                expect(batch3).toEqual('a-f');
                expect(batch4).toEqual('');
            });

            it('can correctly split across alphanumeric types', () => {
                const tracker = new SplitKeyTracker(IDType.hexadecimal);

                const batch1 = tracker.split(4);
                const batch2 = tracker.split(4);
                const batch3 = tracker.split(4);
                const batch4 = tracker.split(4);
                const batch5 = tracker.split(4);

                expect(batch1).toEqual('0-3');
                expect(batch2).toEqual('4-7');
                expect(batch3).toEqual('8-9a-b');
                expect(batch4).toEqual('c-f');
                expect(batch5).toEqual('');
            });
        });

        describe(`${IDType.HEXADECIMAL}`, () => {
            it('can correctly split in batches', () => {
                const tracker = new SplitKeyTracker(IDType.HEXADECIMAL);

                const batch1 = tracker.split(5);
                const batch2 = tracker.split(5);
                const batch3 = tracker.split(6);
                const batch4 = tracker.split(5);

                expect(batch1).toEqual('0-4');
                expect(batch2).toEqual('5-9');
                expect(batch3).toEqual('A-F');
                expect(batch4).toEqual('');
            });

            it('can correctly split across alphanumeric types', () => {
                const tracker = new SplitKeyTracker(IDType.HEXADECIMAL);

                const batch1 = tracker.split(4);
                const batch2 = tracker.split(4);
                const batch3 = tracker.split(4);
                const batch4 = tracker.split(4);
                const batch5 = tracker.split(4);

                expect(batch1).toEqual('0-3');
                expect(batch2).toEqual('4-7');
                expect(batch3).toEqual('8-9A-B');
                expect(batch4).toEqual('C-F');
                expect(batch5).toEqual('');
            });
        });

        describe(`${IDType.base64url}`, () => {
            it('can correctly split in batches', () => {
                const tracker = new SplitKeyTracker(IDType.base64url);

                const batch1 = tracker.split(26);
                const batch2 = tracker.split(26);
                const batch3 = tracker.split(10);
                const batch4 = tracker.split(2);
                const batch5 = tracker.split(4);

                expect(batch1).toEqual('A-Z');
                expect(batch2).toEqual('a-z');
                expect(batch3).toEqual('0-9');
                expect(batch4).toEqual('-_');
                expect(batch5).toEqual('');
            });

            it('can correctly split across alphanumeric types', () => {
                const tracker = new SplitKeyTracker(IDType.base64url);

                const batch1 = tracker.split(10);
                const batch2 = tracker.split(10);
                const batch3 = tracker.split(10);
                const batch4 = tracker.split(10);
                const batch5 = tracker.split(10);
                const batch6 = tracker.split(10);
                const batch7 = tracker.split(10);
                const batch8 = tracker.split(10);

                expect(batch1).toEqual('A-J');
                expect(batch2).toEqual('K-T');
                expect(batch3).toEqual('U-Za-d');
                expect(batch4).toEqual('e-n');
                expect(batch5).toEqual('o-x');
                expect(batch6).toEqual('y-z0-7');
                expect(batch7).toEqual('8-9-_');
                expect(batch8).toEqual('');
            });
        });

        describe(`${IDType.base64}`, () => {
            it('can correctly split in batches', () => {
                const tracker = new SplitKeyTracker(IDType.base64);

                const batch1 = tracker.split(26);
                const batch2 = tracker.split(26);
                const batch3 = tracker.split(10);
                const batch4 = tracker.split(4);
                const batch5 = tracker.split(4);

                expect(batch1).toEqual('A-Z');
                expect(batch2).toEqual('a-z');
                expect(batch3).toEqual('0-9');
                expect(batch4).toEqual('-_+/');
                expect(batch5).toEqual('');
            });

            it('can correctly split across alphanumeric types', () => {
                const tracker = new SplitKeyTracker(IDType.base64);

                const batch1 = tracker.split(10);
                const batch2 = tracker.split(10);
                const batch3 = tracker.split(10);
                const batch4 = tracker.split(10);
                const batch5 = tracker.split(10);
                const batch6 = tracker.split(10);
                const batch7 = tracker.split(10);
                const batch8 = tracker.split(10);

                expect(batch1).toEqual('A-J');
                expect(batch2).toEqual('K-T');
                expect(batch3).toEqual('U-Za-d');
                expect(batch4).toEqual('e-n');
                expect(batch5).toEqual('o-x');
                expect(batch6).toEqual('y-z0-7');
                expect(batch7).toEqual('8-9-_+/');
                expect(batch8).toEqual('');
            });
        });
    });
});
