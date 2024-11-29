package org.apache.flink.table.udf;

import org.apache.fink.table.udf.Transpose;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TransposeTest {

    @Test
    public void test() {

        // Example DataType instances
        DataType intNotNullDataType = DataTypes.INT().notNull();
        DataType intNullableDataType = DataTypes.INT();
        DataType stringDataType = DataTypes.STRING();

        System.out.println(intNotNullDataType.getLogicalType().getTypeRoot());


//        List<String> a = new ArrayList<>();
//        a.add("a");
//        List<Integer> b = new ArrayList<>();
//        b.add(1);
//        Row row = Row.of(a, b);
//        System.out.println(row);
//
//        Transpose transpose = new Transpose();
//        List<RowData> input1 = Arrays.asList(
//                Row.of(67034544014L, 3014, 1732783319),
//                Row.of(67034938059L, 3010, 1732783205),
//                Row.of(67034902162L, 3010, 1732783121)
//        );
//        Row output1 = transpose.eval(input1);
//
//        System.out.println(output1);
//
//        List<Row> input2 = Arrays.asList(
//                Row.of(67034902162L, 3010, 1732783121)
//        );
//        Row output2 = transpose.eval(input2);
//
//        System.out.println(output2);

    }

}
