package org.apache.fink.table.udf;

import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

public class GetFieldByIndex extends ScalarFunction {
    public Object eval(Row row, int index) {
        if (row == null || index < 0 || index >= row.getArity()) {
            return null;
        }
        return row.getField(index);
    }
}
