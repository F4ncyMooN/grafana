import { DataFrame, DataTransformerInfo, Field } from '../../types';
import { DataTransformerID } from './ids';
import { MutableDataFrame } from '../../dataframe';
import { ArrayVector } from '../../vector';

export interface FullJoinOptions {
  byFields?: string[];
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

    const resultFrame = new MutableDataFrame();
    const df = data.reduce((prev, now) => {
      return joinDataFrame(prev, now, keyFields);
    }, resultFrame);

    return [df];
  },
};

function intersection<T>(...datum: T[][]): T[] {
  if (!datum || datum.length === 0) {
    return [];
  }
  return datum.reduce((prev: T[], now: T[]) => {
    if (prev === null || prev === undefined) {
      return now;
    }
    return prev.filter(d => now.indexOf(d) > -1);
  });
}
function union<T>(...datum: T[][]): T[] {
  if (!datum || datum.length === 0) {
    return [];
  }
  return datum.reduce((prev: T[], now: T[]) => {
    return prev.concat(now.filter(v => !(prev.indexOf(v) > -1)));
  }, []);
}

function rowSize(df: DataFrame): number {
  for (const field of df.fields) {
    return field.values.length;
  }
  return 0;
}
function findMatchedIndex(df: DataFrame, kvs: { [key: string]: any }) {
  const idx: number[][] = [];
  df.fields.forEach(field => {
    if (field.name in kvs) {
      const vals = field.values;
      const matchedIndex: number[] = [];
      for (let i = 0; i < vals.length; i++) {
        if (vals.get(i) === kvs[field.name]) {
          matchedIndex.push(i);
        }
      }
      idx.push(matchedIndex);
    }
  });
  return intersection(...idx);
}

function joinDataFrame(prev: MutableDataFrame, now: MutableDataFrame, on: string[]): MutableDataFrame {
  if (prev.length === 0) {
    // deep clone
    return new MutableDataFrame(now);
  }
  // console.log('before merge', prev);
  // assume all fields' values have same length.
  const prevFields = prev.fields.map(v => v.name);
  const nowFields = now.fields.map(v => v.name);

  const keys = intersection(on, prevFields, nowFields);
  // console.log(keys, prevFields, nowFields, on);
  const final: Table = {};
  const length = rowSize(prev);
  union(prevFields, nowFields).forEach(key => (final[key] = []));

  for (let i = 0; i < length; i++) {
    const kvs: { [key: string]: any } = {};
    prev.fields.forEach(field => {
      if (keys.includes(field.name)) {
        kvs[field.name] = field.values.get(i);
      }
    });
    const matchedIndex = findMatchedIndex(now, kvs);
    // console.log('matched index', kvs, matchedIndex);
    if (matchedIndex.length === 0) {
      continue;
    }
    // put driver table fields into new table
    for (const key of prevFields) {
      const field = findKeyField(prev, key);
      if (field != null) {
        final[key].push(field.values.get(i));
      }
    }
    // put target table fields into new table
    matchedIndex.forEach(j => {
      for (const key of nowFields) {
        const field = findKeyField(now, key);
        // if some field is duplicated, only use left
        if (field != null && !prevFields.includes(key)) {
          final[key].push(field.values.get(j));
        }
      }
    });
  }

  // console.log(final);
  for (const key of Object.keys(final)) {
    if (findKeyField(prev, key) !== null) {
      continue;
    }
    // console.log('add new', key)
    const field = findKeyField(now, key);
    if (!field) {
      continue;
    }
    prev.addField({
      name: field.name,
      config: field.config,
      type: field.type,
      values: new ArrayVector(),
    });
  }
  for (const field of prev.fields) {
    field.values = new ArrayVector(final[field.name]);
  }
  // console.log('after merge', prev);
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
