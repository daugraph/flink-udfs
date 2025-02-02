package org.apache.fink.table.udf;

import org.apache.flink.table.functions.ScalarFunction;

public class SumFunction extends ScalarFunction {

    public Integer eval(Integer a, Integer b) {
        return a + b;
    }

    public Integer eval(String a, String b) {
        return Integer.parseInt(a) + Integer.parseInt(b);
    }

    public Integer eval(Double... d) {
        double result = 0;
        for (double value : d)
            result += value;
        return (int) result;
    }
}