import { DataFrame, DataTransformerInfo } from '../../types';
import { DataTransformerID } from './ids';
import { isFinite } from 'lodash';
import { ArrayVector } from '../../vector';

export interface MappingOptions {
  byFields?: string[];
}

const DEFAULT_KEY_FIELD = ['Time'];

export const mappingTransformer: DataTransformerInfo<MappingOptions> = {
  id: DataTransformerID.mapping,
  name: 'Map some field into another value',
  description: 'mapping',
  defaultOptions: {
    byFields: DEFAULT_KEY_FIELD,
  },
  transformer: options => (data: DataFrame[]) => {
    return data.map(d => {
      d.fields = d.fields.map(field => {
        console.log(field.name, options.byFields);
        if (options.byFields?.includes(field.name)) {
          console.log(field.values);
          field.values = new ArrayVector(field.values.toArray().map(v => (!isFinite(v) || v < 0 ? 0 : v)));
        }
        return field;
      });
      return d;
    });
  },
};
