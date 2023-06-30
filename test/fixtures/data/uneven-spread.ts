import { ElasticsearchTestHelpers } from 'elasticsearch-store';

const { data, UnevenDataTypeFields } = ElasticsearchTestHelpers.UnevenDateData;

export = {
    data,
    dataType: UnevenDataTypeFields,
    index: 'uneven_spread'
}
