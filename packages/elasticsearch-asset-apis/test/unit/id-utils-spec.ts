import {
    KeyChunker, SpecialKeyChunker, IDType,
    lowerCaseChars, base64SpecialChars, SplitKeyManager
} from '../../src/index.js';

describe('id-utils', () => {
    describe('SplitKeyManager', () => {
        it('can create a tracker', () => {
            const tracker = new SplitKeyManager(IDType.hexadecimal);

            expect(tracker).toBeDefined();
            expect(tracker.split).toBeFunction();
        });

        it('will throw if using a wrong tracker type', () => {
            expect(
                () => new SplitKeyManager('[something' as unknown as IDType)
            ).toThrow();
        });

        it('can create a range of keys', () => {
            const tracker = new SplitKeyManager(IDType.hexadecimal);
            const batch = tracker.split(5);

            expect(batch).toEqual('[0-4]');
        });

        it('will not create more ranges unless a commit is called', () => {
            const tracker = new SplitKeyManager(IDType.hexadecimal);
            const batch = tracker.split(5);
            const batch2 = tracker.split(5);

            expect(batch).toEqual('[0-4]');
            expect(batch2).toEqual('[0-4]');

            tracker.commit();

            const batch3 = tracker.split(5);
            expect(batch3).toEqual('[5-9]');
        });

        describe(`${IDType.hexadecimal}`, () => {
            it('can correctly split in batches', () => {
                const tracker = new SplitKeyManager(IDType.hexadecimal);

                const batch1 = tracker.split(5);
                tracker.commit();

                const batch2 = tracker.split(5);
                tracker.commit();

                const batch3 = tracker.split(6);
                tracker.commit();

                const batch4 = tracker.split(5);
                tracker.commit();

                expect(batch1).toEqual('[0-4]');
                expect(batch2).toEqual('[5-9]');
                expect(batch3).toEqual('[a-f]');
                expect(batch4).toEqual('');
            });

            it('can correctly split across alphanumeric types', () => {
                const tracker = new SplitKeyManager(IDType.hexadecimal);

                const batch1 = tracker.split(4);
                tracker.commit();

                const batch2 = tracker.split(4);
                tracker.commit();

                const batch3 = tracker.split(4);
                tracker.commit();

                const batch4 = tracker.split(4);
                tracker.commit();

                const batch5 = tracker.split(4);
                tracker.commit();

                expect(batch1).toEqual('[0-3]');
                expect(batch2).toEqual('[4-7]');
                expect(batch3).toEqual('[8-9a-b]');
                expect(batch4).toEqual('[c-f]');
                expect(batch5).toEqual('');
            });
        });

        describe(`${IDType.HEXADECIMAL}`, () => {
            it('can correctly split in batches', () => {
                const tracker = new SplitKeyManager(IDType.HEXADECIMAL);

                const batch1 = tracker.split(5);
                tracker.commit();

                const batch2 = tracker.split(5);
                tracker.commit();

                const batch3 = tracker.split(6);
                tracker.commit();

                const batch4 = tracker.split(5);
                tracker.commit();

                expect(batch1).toEqual('[0-4]');
                expect(batch2).toEqual('[5-9]');
                expect(batch3).toEqual('[A-F]');
                expect(batch4).toEqual('');
            });

            it('can correctly split across alphanumeric types', () => {
                const tracker = new SplitKeyManager(IDType.HEXADECIMAL);

                const batch1 = tracker.split(4);
                tracker.commit();

                const batch2 = tracker.split(4);
                tracker.commit();

                const batch3 = tracker.split(4);
                tracker.commit();

                const batch4 = tracker.split(4);
                tracker.commit();

                const batch5 = tracker.split(4);
                tracker.commit();

                expect(batch1).toEqual('[0-3]');
                expect(batch2).toEqual('[4-7]');
                expect(batch3).toEqual('[8-9A-B]');
                expect(batch4).toEqual('[C-F]');
                expect(batch5).toEqual('');
            });
        });

        describe(`${IDType.base64url}`, () => {
            it('can correctly split in batches', () => {
                const tracker = new SplitKeyManager(IDType.base64url);

                const batch1 = tracker.split(26);
                tracker.commit();

                const batch2 = tracker.split(26);
                tracker.commit();

                const batch3 = tracker.split(10);
                tracker.commit();

                const batch4 = tracker.split(2);
                tracker.commit();

                const batch5 = tracker.split(4);
                tracker.commit();

                expect(batch1).toEqual('[A-Z]');
                expect(batch2).toEqual('[a-z]');
                expect(batch3).toEqual('[0-9]');
                expect(batch4).toEqual('[-_]');
                expect(batch5).toEqual('');
            });

            it('can correctly split across alphanumeric types', () => {
                const tracker = new SplitKeyManager(IDType.base64url);

                const batch1 = tracker.split(10);
                tracker.commit();

                const batch2 = tracker.split(10);
                tracker.commit();

                const batch3 = tracker.split(10);
                tracker.commit();

                const batch4 = tracker.split(10);
                tracker.commit();

                const batch5 = tracker.split(10);
                tracker.commit();

                const batch6 = tracker.split(10);
                tracker.commit();

                const batch7 = tracker.split(10);
                tracker.commit();

                const batch8 = tracker.split(10);
                tracker.commit();

                expect(batch1).toEqual('[A-J]');
                expect(batch2).toEqual('[K-T]');
                expect(batch3).toEqual('[U-Za-d]');
                expect(batch4).toEqual('[e-n]');
                expect(batch5).toEqual('[o-x]');
                expect(batch6).toEqual('[y-z0-7]');
                expect(batch7).toEqual('[8-9-_]');
                expect(batch8).toEqual('');
            });
        });

        describe(`${IDType.base64}`, () => {
            it('can correctly split in batches', () => {
                const tracker = new SplitKeyManager(IDType.base64);

                const batch1 = tracker.split(26);
                tracker.commit();

                const batch2 = tracker.split(26);
                tracker.commit();

                const batch3 = tracker.split(10);
                tracker.commit();

                const batch4 = tracker.split(4);
                tracker.commit();

                const batch5 = tracker.split(4);
                tracker.commit();

                expect(batch1).toEqual('[A-Z]');
                expect(batch2).toEqual('[a-z]');
                expect(batch3).toEqual('[0-9]');
                expect(batch4).toEqual('[-_+/]');
                expect(batch5).toEqual('');
            });

            it('can correctly split across alphanumeric types', () => {
                const tracker = new SplitKeyManager(IDType.base64);

                const batch1 = tracker.split(10);
                tracker.commit();

                const batch2 = tracker.split(10);
                tracker.commit();

                const batch3 = tracker.split(10);
                tracker.commit();

                const batch4 = tracker.split(10);
                tracker.commit();

                const batch5 = tracker.split(10);
                tracker.commit();

                const batch6 = tracker.split(10);
                tracker.commit();

                const batch7 = tracker.split(10);
                tracker.commit();

                const batch8 = tracker.split(10);
                tracker.commit();

                expect(batch1).toEqual('[A-J]');
                expect(batch2).toEqual('[K-T]');
                expect(batch3).toEqual('[U-Za-d]');
                expect(batch4).toEqual('[e-n]');
                expect(batch5).toEqual('[o-x]');
                expect(batch6).toEqual('[y-z0-7]');
                expect(batch7).toEqual('[8-9-_+/]');
                expect(batch8).toEqual('');
            });
        });
    });

    describe('KeyChunker', () => {
        it('can return a chunk of keys', () => {
            const chunker = new KeyChunker(lowerCaseChars);
            const chunk = chunker.split(5);

            expect(chunk).toMatchObject({ range: 'a-e', took: 5 });
        });

        it('will not progress to next chunk until committed', () => {
            const chunker = new KeyChunker(lowerCaseChars);
            const chunk = chunker.split(5);
            const chunk2 = chunker.split(5);

            expect(chunk).toMatchObject({ range: 'a-e', took: 5 });
            expect(chunk2).toMatchObject({ range: 'a-e', took: 5 });

            chunker.commit();

            const chunk3 = chunker.split(5);
            expect(chunk3).toMatchObject({ range: 'f-j', took: 5 });
        });

        it('will chunk until the end', () => {
            const chunker = new KeyChunker(lowerCaseChars);
            const chunk = chunker.split(10);
            chunker.commit();

            const chunk2 = chunker.split(10);
            chunker.commit();

            const chunk3 = chunker.split(6);
            chunker.commit();

            expect(chunk).toMatchObject({ range: 'a-j', took: 10 });
            expect(chunk2).toMatchObject({ range: 'k-t', took: 10 });
            expect(chunk3).toMatchObject({ range: 'u-z', took: 6 });

            expect(chunker.isDone).toBeTrue();

            const chunk5 = chunker.split(6);
            expect(chunk5).toMatchObject({ range: '', took: 0 });
        });

        it('can handle oversized chunk values', () => {
            const chunker = new KeyChunker(lowerCaseChars);
            const chunk = chunker.split(10);
            chunker.commit();

            const chunk2 = chunker.split(10);
            chunker.commit();

            const chunk3 = chunker.split(10);
            chunker.commit();

            expect(chunk).toMatchObject({ range: 'a-j', took: 10 });
            expect(chunk2).toMatchObject({ range: 'k-t', took: 10 });
            expect(chunk3).toMatchObject({ range: 'u-z', took: 6 });

            expect(chunker.isDone).toBeTrue();

            const chunk5 = chunker.split(6);
            expect(chunk5).toMatchObject({ range: '', took: 0 });
        });

        it('can take all in one go', () => {
            const chunker = new KeyChunker(lowerCaseChars);
            const chunk = chunker.split(30);
            chunker.commit();

            expect(chunker.isDone).toBeTrue();

            const chunk2 = chunker.split(10);
            chunker.commit();

            expect(chunk).toMatchObject({ range: 'a-z', took: 26 });
            expect(chunk2).toMatchObject({ range: '', took: 0 });
        });
    });

    describe('SpecialKeyChunker', () => {
        it('can return a chunk of keys', () => {
            const chunker = new SpecialKeyChunker(base64SpecialChars);
            const chunk = chunker.split(2);

            expect(chunk).toMatchObject({ range: '-_', took: 2 });
        });

        it('will not progress to next chunk until committed', () => {
            const chunker = new SpecialKeyChunker(base64SpecialChars);
            const chunk = chunker.split(1);
            const chunk2 = chunker.split(1);

            expect(chunk).toMatchObject({ range: '-', took: 1 });
            expect(chunk2).toMatchObject({ range: '-', took: 1 });

            chunker.commit();

            const chunk3 = chunker.split(1);
            expect(chunk3).toMatchObject({ range: '_', took: 1 });
        });

        it('will chunk until the end', () => {
            const chunker = new SpecialKeyChunker(base64SpecialChars);
            const chunk = chunker.split(2);
            chunker.commit();

            expect(chunk).toMatchObject({ range: '-_', took: 2 });

            const chunk2 = chunker.split(2);
            chunker.commit();

            expect(chunker.isDone).toBeTrue();
            expect(chunk2).toMatchObject({ range: '+/', took: 2 });
        });

        it('can handle oversized chunk values', () => {
            const chunker = new SpecialKeyChunker(base64SpecialChars);
            const chunk = chunker.split(2);
            chunker.commit();

            expect(chunk).toMatchObject({ range: '-_', took: 2 });

            const chunk2 = chunker.split(4);
            chunker.commit();

            expect(chunker.isDone).toBeTrue();
            expect(chunk2).toMatchObject({ range: '+/', took: 2 });

            const chunk3 = chunker.split(4);
            chunker.commit();

            expect(chunk3).toMatchObject({ range: '', took: 0 });
        });

        it('can take all in one go', () => {
            const chunker = new SpecialKeyChunker(base64SpecialChars);
            const chunk = chunker.split(30);
            chunker.commit();

            expect(chunker.isDone).toBeTrue();

            const chunk2 = chunker.split(10);
            chunker.commit();

            expect(chunk).toMatchObject({ range: '-_+/', took: 4 });
            expect(chunk2).toMatchObject({ range: '', took: 0 });
        });
    });
});
