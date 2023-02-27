package com.alibaba.feathub.flink.udf.aggregation.avg;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import com.alibaba.feathub.flink.udf.aggregation.PreAggFunc;
import com.alibaba.feathub.flink.udf.aggregation.count.CountAccumulator;
import com.alibaba.feathub.flink.udf.aggregation.count.CountPreAggFunc;
import com.alibaba.feathub.flink.udf.aggregation.sum.SumAggFunc;

public class AvgPreAggFunc<IN_T extends Number, ACC_T>
        implements PreAggFunc<IN_T, Row, Tuple2<CountAccumulator, ACC_T>> {
    private final CountPreAggFunc countAggFunc;
    private final SumAggFunc<IN_T, ACC_T> sumAggFunc;

    public AvgPreAggFunc(SumAggFunc<IN_T, ACC_T> sumAggFunc) {
        this.countAggFunc = new CountPreAggFunc();
        this.sumAggFunc = sumAggFunc;
    }

    @Override
    public void add(Tuple2<CountAccumulator, ACC_T> accumulator, IN_T value, long timestamp) {
        countAggFunc.add(accumulator.f0, value, timestamp);
        sumAggFunc.add(accumulator.f1, value, timestamp);
    }

    @Override
    public Row getResult(Tuple2<CountAccumulator, ACC_T> accumulator) {
        return Row.of(countAggFunc.getResult(accumulator.f0), sumAggFunc.getResult(accumulator.f1));
    }

    @Override
    public DataType getResultDatatype() {
        return DataTypes.ROW(
                DataTypes.FIELD("count", countAggFunc.getResultDatatype()),
                DataTypes.FIELD("sum", sumAggFunc.getResultDatatype()));
    }

    @Override
    public Tuple2<CountAccumulator, ACC_T> createAccumulator() {
        return Tuple2.of(countAggFunc.createAccumulator(), sumAggFunc.createAccumulator());
    }

    @Override
    public TypeInformation<Tuple2<CountAccumulator, ACC_T>> getAccumulatorTypeInformation() {
        return Types.TUPLE(
                countAggFunc.getAccumulatorTypeInformation(),
                sumAggFunc.getAccumulatorTypeInformation());
    }
}
