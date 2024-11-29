package org.apache.fink.table.udf;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.dataview.ListView;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.runtime.functions.aggregate.BuiltInAggregateFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import static org.apache.flink.table.types.utils.DataTypeUtils.toInternalDataType;

public final class CollectList<T>
        extends BuiltInAggregateFunction<ArrayData, CollectList.CollectAccumulator<T>> {

    private static final long serialVersionUID = -5860934997657147837L;

    private final transient DataType elementDataType;

    public CollectList(LogicalType elementType) {
        this.elementDataType = toInternalDataType(elementType);
    }

    // --------------------------------------------------------------------------------------------
    // Planning
    // --------------------------------------------------------------------------------------------

    @Override
    public List<DataType> getArgumentDataTypes() {
        return Collections.singletonList(elementDataType);
    }

    @Override
    public DataType getAccumulatorDataType() {
        return DataTypes.STRUCTURED(
                CollectAccumulator.class,
                DataTypes.FIELD(
                        "list",
                        ListView.newListViewDataType(elementDataType.notNull())));
    }

    @Override
    public DataType getOutputDataType() {
        return DataTypes.ARRAY(elementDataType).bridgedTo(ArrayData.class);
    }

    // --------------------------------------------------------------------------------------------
    // Runtime
    // --------------------------------------------------------------------------------------------

    /** Accumulator for COLLECT. */
    public static class CollectAccumulator<T> {
        public ListView<T> list;

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CollectAccumulator<?> that = (CollectAccumulator<?>) o;
            return Objects.equals(list, that.list);
        }
    }

    public CollectAccumulator<T> createAccumulator() {
        final CollectAccumulator<T> acc = new CollectAccumulator<>();
        acc.list = new ListView<>();
        return acc;
    }

    public void resetAccumulator(CollectAccumulator<T> accumulator) {
        accumulator.list.clear();
    }

    public void accumulate(CollectAccumulator<T> accumulator, T value) throws Exception {
        if (value != null) {
            accumulator.list.add(value);
        }
    }

    public void retract(CollectAccumulator<T> accumulator, T value) throws Exception {
        if (value != null) {
            accumulator.list.remove(value);
        }
    }

    public void merge(CollectAccumulator<T> accumulator, Iterable<CollectAccumulator<T>> others)
            throws Exception {
        for (CollectAccumulator<T> other : others) {
            Iterable<T> iter = other.list.get();
            while (iter.iterator().hasNext()) {
                T value = iter.iterator().next();
                accumulator.list.add(value);
            }
        }
    }

    @Override
    public ArrayData getValue(CollectAccumulator<T> accumulator) {
        return new GenericArrayData(accumulator.list.getList().toArray());
    }
}