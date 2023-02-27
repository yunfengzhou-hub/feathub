package com.alibaba.feathub.flink.udf.aggregation.valuecounts;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import com.alibaba.feathub.flink.udf.aggregation.PreAggFunc;

import java.util.Map;

public class ValueCountsPreAggFunc
        implements PreAggFunc<Object, Map<Object, Long>, ValueCountsAccumulator> {
    private final DataType inDataType;

    public ValueCountsPreAggFunc(DataType inDataType) {
        this.inDataType = inDataType;
    }

    @Override
    public void add(ValueCountsAccumulator accumulator, Object value, long timestamp) {
        accumulator.valueCounts.put(value, accumulator.valueCounts.getOrDefault(value, 0L) + 1);
    }

    @Override
    public Map<Object, Long> getResult(ValueCountsAccumulator accumulator) {
        if (accumulator.valueCounts.isEmpty()) {
            return null;
        }
        return accumulator.valueCounts;
    }

    @Override
    public DataType getResultDatatype() {
        return DataTypes.MAP(inDataType, DataTypes.BIGINT());
    }

    @Override
    public ValueCountsAccumulator createAccumulator() {
        return new ValueCountsAccumulator();
    }

    @Override
    public TypeInformation<ValueCountsAccumulator> getAccumulatorTypeInformation() {
        return Types.POJO(ValueCountsAccumulator.class);
    }
}
