package org.apache.fink.table.udf;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.CollectionDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.inference.ArgumentCount;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.ConstantArgumentCount;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.Signature;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class Transpose extends ScalarFunction {

    private List<DataType> dataTypes;

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return TypeInference.newBuilder()
                .inputTypeStrategy(
                        new InputTypeStrategy() {
                            @Override
                            public ArgumentCount getArgumentCount() {
                                return ConstantArgumentCount.of(1);
                            }

                            @Override
                            public Optional<List<DataType>> inferInputTypes(
                                    CallContext callContext, boolean throwOnFailure) {
                                final List<DataType> args = callContext.getArgumentDataTypes();
                                final DataType dataType = args.get(0);
                                if (dataType.getLogicalType().getTypeRoot() != LogicalTypeRoot.ARRAY) {
                                    return callContext.fail(throwOnFailure, "Input argument must be ARRAY type.");
                                }
                                return Optional.of(Arrays.asList(dataType.bridgedTo(List.class)));
                            }

                            @Override
                            public List<Signature> getExpectedSignatures(
                                    FunctionDefinition definition) {
                                return Arrays.asList(
                                        Signature.of(Signature.Argument.ofGroup(LogicalTypeRoot.ARRAY))
                                );
                            }
                        })
                .outputTypeStrategy(
                        callContext -> {
                            final List<DataType> args = callContext.getArgumentDataTypes();
                            final DataType dataType = args.get(0);
                            if (dataType instanceof CollectionDataType) {
                                DataType elementType = ((CollectionDataType) dataType).getElementDataType();
                                if (elementType instanceof FieldsDataType) {
                                    dataTypes = elementType.getChildren();
                                    return Optional.of(DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.STRING())));
                                }
                            }
                            return Optional.empty();
                        })
                .build();
    }

    public String[][] eval(List<Row> input) {
        int n = dataTypes.size();
        if (n <= 0) {
            throw new IllegalArgumentException("Can't process a row with zero arity");
        }

        if (input == null || input.isEmpty()) {
            return new String[n][0];
        }

        String[][] output = new String[n][];

        for (int i = 0; i < n; i++) {
            DataType dataType = dataTypes.get(i);
            if (dataType.equals(DataTypes.BIGINT()) || dataType.equals(DataTypes.BIGINT().notNull())) {
                String[] longArr = new String[input.size()];
                for (int j = 0; j < input.size(); j++) {
                    Row row = input.get(j);
                    long value = row.getFieldAs(i);
                    longArr[j] = String.valueOf(value);
                }
                output[i] = longArr;
            } else {
                throw new UnsupportedOperationException("Unsupported DataType found: " + dataType.toString());
            }
        }
        return output;
    }
}
