import { DataTypeFields, FieldType } from '@terascope/types';
import { cloneDeep } from '@terascope/utils';

// These records are meant to fall within the following range
// start: '2019-04-26T15:00:23.201Z',
// end: '2019-04-26T15:00:23.207Z',
// changes:
// uuid: `deadbeef` suffix
// ip: ends in 18
// ipv6: ends in :1818
// url: 18 appended to end of domain name
// created: randomly selected milliseconds that fall within range
const data = [
    {
        ip: '72.8.102.18',
        userAgent: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_3)  AppleWebKit/536.1.0 (KHTML, like Gecko) Chrome/17.0.855.0 Safari/536.1.0',
        url: 'https://billie18.biz',
        uuid: '5d085b16-ef14-4f23-b118-d8c4deadbeef',
        created: '2019-04-26T15:00:23.202+00:00',
        ipv6: 'a3fc:ae59:d97f:c8fa:c5a6:8210:925d:1818',
        location: '-72.7229, -178.84325',
        bytes: 887820
    },
    {
        ip: '179.104.247.18',
        userAgent: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_5_2 rv:2.0; CO) AppleWebKit/536.2.1 (KHTML, like Gecko) Version/6.0.6 Safari/536.2.1',
        url: 'http://makayla18.net',
        uuid: '39d471cf-f478-4324-91bf-ef5bdeadbeef',
        created: '2019-04-26T15:00:23.206+00:00',
        ipv6: '3eae:b483:7dff:317d:f28c:1002:f213:1818',
        location: '-24.41098, 105.53778',
        bytes: 2129260
    },
    {
        ip: '123.137.14.18',
        userAgent: 'Mozilla/5.0 (Windows; U; Windows NT 5.0) AppleWebKit/534.2.2 (KHTML, like Gecko) Chrome/25.0.883.0 Safari/534.2.2',
        url: 'https://marisa18.name',
        uuid: 'dcdd9999-761c-44e7-a312-91bbdeadbeef',
        created: '2019-04-26T15:00:23.204+00:00',
        ipv6: '7f70:9cb7:be35:f55a:4b3e:0f98:383e:1818',
        location: '87.16682, -125.64349',
        bytes: 5233485
    },
    {
        ip: '240.44.7.18',
        userAgent: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_6_9 rv:2.0; SK) AppleWebKit/536.1.0 (KHTML, like Gecko) Version/6.1.6 Safari/536.1.0',
        url: 'https://darron18.net',
        uuid: '02033bfc-7de8-4c78-a2ce-6b4ddeadbeef',
        created: '2019-04-26T15:00:23.202+00:00',
        ipv6: 'ab88:805e:55db:0750:b143:61ce:e07a:1818',
        location: '89.30019, -158.5777',
        bytes: 802825
    }
];

const types: DataTypeFields = {
    ip: { type: FieldType.IP },
    userAgent: { type: FieldType.Keyword },
    url: { type: FieldType.Keyword },
    uuid: { type: FieldType.Keyword },
    created: { type: FieldType.Date },
    ipv6: { type: FieldType.Keyword },
    location: { type: FieldType.GeoPoint },
    bytes: { type: FieldType.Integer }
};

export = {
    data: cloneDeep(data),
    types,
    index: 'even_spread'
}
