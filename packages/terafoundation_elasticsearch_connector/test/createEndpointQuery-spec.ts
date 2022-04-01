import { createEndpointQuery } from '../src/create-client';

describe('createEndpointQuery', () => {
    const node = 'localhost:9210';
    const username = 'foo';
    const password = 'bar';

    describe('legacy support', () => {
        it('can make a basic valid url', () => {
            const endpoint = createEndpointQuery({ host: node });
            expect(endpoint).toEqual('http://localhost:9210/');
        });

        // TODO: make sure any other legacy properties map over
    });

    describe('auth', () => {
        it('can make a valid url with password/username at top level', () => {
            const endpoint = createEndpointQuery({ node, password, username });
            expect(endpoint).toEqual('https://foo:bar@localhost:9210/');
        });

        it('can make a valid url with password/username in auth object', () => {
            const endpoint = createEndpointQuery({ node, auth: { username, password } });
            expect(endpoint).toEqual('https://foo:bar@localhost:9210/');
        });
    });
});
