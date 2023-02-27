package com.alibaba.feathub.flink.udf.aggregation.firstlastvalue;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import com.alibaba.feathub.flink.udf.aggregation.PreAggFunc;

public class FirstLastValuePreAggFunc<T>
        implements PreAggFunc<T, Row, FirstLastValuePreAccumulator<T>> {
    private final DataType inDataType;
    private final boolean isFirstValue;

    public FirstLastValuePreAggFunc(DataType inDataType, boolean isFirstValue) {
        this.inDataType = inDataType;
        this.isFirstValue = isFirstValue;
    }

    @Override
    public void add(FirstLastValuePreAccumulator<T> accumulator, T value, long timestamp) {
        if (isFirstValue && accumulator.timestamp != Long.MIN_VALUE) {
            return;
        }

        accumulator.value = value;
        accumulator.timestamp = timestamp;
    }

    @Override
    public Row getResult(FirstLastValuePreAccumulator<T> accumulator) {
        Row row = Row.withNames();
        row.setField("value", accumulator.value);
        row.setField("timestamp", accumulator.timestamp);
        return row;
    }

    @Override
    public DataType getResultDatatype() {
        return DataTypes.ROW(
                DataTypes.FIELD("value", inDataType),
                DataTypes.FIELD("timestamp", DataTypes.BIGINT()));
    }

    @Override
    public FirstLastValuePreAccumulator<T> createAccumulator() {
        return new FirstLastValuePreAccumulator<>();
    }

    @Override
    public TypeInformation getAccumulatorTypeInformation() {
        return Types.POJO(FirstLastValuePreAccumulator.class);
    }
}
