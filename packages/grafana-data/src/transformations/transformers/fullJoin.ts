import { DataFrame, DataTransformerInfo, Field } from '../../types';
import { DataTransformerID } from './ids';
import { MutableDataFrame } from '../../dataframe';
import { ArrayVector } from '../../vector';

export interface FullJoinOptions {
  byFields?: string[];
}

interface Dimension {
  keys: string[];
  vals: string[];
}
interface Table {
  [key: string]: any[];
}

const DEFAULT_KEY_FIELD = ['Time'];

export const fullJoinTransformer: DataTransformerInfo<FullJoinOptions> = {
  id: DataTransformerID.fullJoin,
  name: 'Series as columns',
  description: 'Groups series by field and returns values as columns',
  defaultOptions: {
    byField: DEFAULT_KEY_FIELD,
  },
  transformer: options => (data: DataFrame[]) => {
    const keyFields = options.byFields || DEFAULT_KEY_FIELD;
    const dimMap: { [key: string]: { dim: Dimension; times: number } } = {};

    // check each frame has all join fields
    for (let frameIndex = 0; frameIndex < data.length; frameIndex++) {
      const frame = data[frameIndex];
      const dims: Field[] = [];
      for (let fieldIndex = 0; fieldIndex < keyFields.length; fieldIndex++) {
        const field = findKeyField(frame, keyFields[fieldIndex]);
        if (!field) {
          return data;
        }
        dims.push(field);
      }
      const dimensions = getDimensionSets(dims);
      for (const d of dimensions) {
        const key = d.vals.join(',');
        if (key in dimMap) {
          dimMap[key].times += 1;
          continue;
        }
        dimMap[key] = {
          dim: d,
          times: 1,
        };
      }
    }

    const joinedDimentionSet: Dimension[] = [];
    Object.keys(dimMap).forEach(key => {
      const element = dimMap[key];
      if (element.times === data.length) {
        joinedDimentionSet.push(element.dim);
      }
    });

    const resultFrame = new MutableDataFrame();
    const df = data.reduce((prev, now) => {
      return joinDataFrame(prev, now, joinedDimentionSet);
    }, resultFrame);

    return [df];
  },
};

function getDimensionSets(fields: Field[]): Dimension[] {
  const dims: Dimension[] = [];
  let maxLen = 0;
  for (const f of fields) {
    if (f.values.length > maxLen) {
      maxLen = f.values.length;
    }
  }
  const mp: { [key: string]: number } = {};
  for (let i = 0; i < maxLen; i++) {
    let keys: string[] = [];
    let vals: string[] = [];
    for (const f of fields) {
      const k = f.name;
      const v = f.values.get(i);
      keys.push(k);
      vals.push(v ? v : '');
    }
    if (vals.join(',') in mp) {
      continue;
    }
    dims.push({
      keys: keys,
      vals: vals,
    });
    mp[vals.join(',')] = 1;
  }
  return dims;
}

function getFieldSize(fields: Field[]): number {
  for (const f of fields) {
    return f.values.length;
  }
  return 0;
}

function matchDimension(df: DataFrame, dimension: Dimension, index: number): boolean {
  let allMatch = true;
  for (let i = 0; i < dimension.keys.length; i++) {
    for (const field of df.fields) {
      if (field.name === dimension.keys[i] && field.values.get(index) !== dimension.vals[i]) {
        return false;
      }
    }
  }
  return allMatch;
}

function findMatchedFields(df: DataFrame, dimension: Dimension): Table {
  const fields: Table = {};
  df.fields.forEach(field => (fields[field.name] = []));
  const size = getFieldSize(df.fields);
  for (let i = 0; i < size; i++) {
    if (matchDimension(df, dimension, i)) {
      for (const field of df.fields) {
        fields[field.name].push(field.values.get(i));
      }
    }
  }
  return fields;
}

function expansion(table: Table, factor: number): Table {
  if (factor <= 1) {
    return table;
  }
  Object.keys(table).forEach(key => {
    const old = table[key];
    const values: any[] = [];

    for (let i = 0; i < factor; i++) {
      for (const v of old) {
        values.push(v);
      }
    }
    table[key] = values;
  });

  return table;
}

function tableLength(table: Table): number {
  for (const key of Object.keys(table)) {
    return table[key].length;
  }
  return 0;
}

function gcd(num1: number, num2: number): number {
  if (num1 === 0 || num2 === 0) {
    return 0;
  }
  if (num1 === num2) {
    return num1;
  }
  while (num2) {
    var t = num2;
    num2 = num1 % num2;
    num1 = t;
  }
  return num1;
}

function mergeTable(left: Table, right: Table): Table {
  let leftSize = tableLength(left);
  let rightSize = tableLength(right);
  const factor = gcd(leftSize, rightSize);
  leftSize /= factor;
  rightSize /= factor;
  left = expansion(left, rightSize);
  right = expansion(right, leftSize);
  // FIX? need to dedupe?
  for (const key of Object.keys(right)) {
    left[key] = right[key];
  }
  return left;
}

function joinDataFrame(prev: DataFrame, now: DataFrame, dims: Dimension[]): DataFrame {
  if (prev.length === 0) {
    // deep clone
    const newFrame = new MutableDataFrame();
    for (const field of now.fields) {
      const values = field.values.toArray();
      newFrame.addField({
        ...field,
        values: new ArrayVector(values),
      });
    }
    return newFrame;
  }
  // assume all fields' values have same length.
  const final: Table = {};
  for (const dimension of dims) {
    const left = findMatchedFields(prev, dimension);
    const right = findMatchedFields(now, dimension);
    const newTable = mergeTable(left, right);
    for (const key of Object.keys(newTable)) {
      if (key in final) {
        final[key] = final[key].concat(newTable[key]);
      } else {
        final[key] = newTable[key];
      }
    }
  }
  // console.log(final)
  for (const key of Object.keys(final)) {
    let found = false;
    for (const field of prev.fields) {
      if (field.name === key) {
        field.values = new ArrayVector(final[key]);
        found = true;
      }
    }
    if (found) {
      continue;
    }
    // console.log('add new', key)
    const field = now.fields.find(field => field.name === key);
    if (!field) {
      continue;
    }
    prev.fields.push({
      ...field,
      name: field.name,
      config: field.config,
      type: field.type,
      values: new ArrayVector(final[key]),
    });
  }
  for (const field of prev.fields) {
    field.values = new ArrayVector(final[field.name]);
  }
  // console.log('after merge', prev)
  return prev;
}

function findKeyField(frame: DataFrame, name: string): Field | null {
  for (const field of frame.fields) {
    if (field.name === name) {
      return field;
    }
  }

  return null;
}
