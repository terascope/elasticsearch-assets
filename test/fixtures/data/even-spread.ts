import { ElasticsearchTestHelpers } from 'elasticsearch-store';

const { data, EvenDataType } = ElasticsearchTestHelpers.EvenDateData;

export = {
    data,
    dataType: EvenDataType,
    index: 'even_spread'
}
