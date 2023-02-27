package com.alibaba.feathub.flink.udf.aggregation.count;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import com.alibaba.feathub.flink.udf.aggregation.PreAggFunc;

public class CountPreAggFunc implements PreAggFunc<Object, Long, CountAccumulator> {
    @Override
    public void add(CountAccumulator accumulator, Object value, long timestamp) {
        accumulator.cnt += 1;
    }

    @Override
    public Long getResult(CountAccumulator accumulator) {
        return accumulator.cnt;
    }

    @Override
    public DataType getResultDatatype() {
        return DataTypes.BIGINT();
    }

    @Override
    public CountAccumulator createAccumulator() {
        return new CountAccumulator();
    }

    @Override
    public TypeInformation<CountAccumulator> getAccumulatorTypeInformation() {
        return Types.POJO(CountAccumulator.class);
    }
}
