import React, { useCallback, useMemo } from 'react';
import {
  DataTransformerID,
  SelectableValue,
  standardTransformers,
  TransformerRegistyItem,
  TransformerUIProps,
} from '@grafana/data';
import { getAllFieldNamesFromDataFrames } from './OrganizeFieldsTransformerEditor';
import { Select } from '@grafana/ui';

import { FullJoinOptions } from '@grafana/data/src/transformations/transformers/fullJoin';

export const FullJoinFieldsTransformerEditor: React.FC<TransformerUIProps<FullJoinOptions>> = ({
  input,
  options,
  onChange,
}) => {
  const fieldNames = useMemo(() => getAllFieldNamesFromDataFrames(input), [input]);
  const fieldNameOptions = fieldNames.map((item: string) => ({ label: item, value: item }));

  const onSelectField = useCallback(
    (value: SelectableValue<string>) => {
      onChange({
        ...options,
        byField: value.value,
      });
    },
    [onChange, options]
  );

  return (
    <div className="gf-form-inline">
      <div className="gf-form gf-form--grow">
        <div className="gf-form-label width-8">Field name</div>
        <Select
          options={fieldNameOptions}
          value={options.byField}
          onChange={onSelectField}
          isClearable
          menuPlacement="bottom"
        />
      </div>
    </div>
  );
};

export const fullJoinTransformerRegistryItem: TransformerRegistyItem<FullJoinOptions> = {
  id: DataTransformerID.fullJoin,
  editor: FullJoinFieldsTransformerEditor,
  transformation: standardTransformers.fullJoinTransformer,
  name: 'Full join',
  description:
    'Joins many time series/tables by a field. This can be used to outer join multiple time series on the _time_ field to show many time series in one table.',
};
