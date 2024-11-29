package org.apache.fink.table.udf;

import org.apache.fink.table.utility.Conversions;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.CollectionDataType;
import org.apache.flink.table.types.DataType;
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
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class SortAndTruncate extends ScalarFunction {

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return TypeInference.newBuilder()
                .inputTypeStrategy(
                        new InputTypeStrategy() {
                            @Override
                            public ArgumentCount getArgumentCount() {
                                return ConstantArgumentCount.of(3);
                            }

                            @Override
                            public Optional<List<DataType>> inferInputTypes(
                                    CallContext callContext, boolean throwOnFailure) {
                                final List<DataType> args = callContext.getArgumentDataTypes();
                                final DataType first = args.get(0);
                                if (first.getLogicalType().getTypeRoot() != LogicalTypeRoot.MULTISET) {
                                    return callContext.fail(throwOnFailure, "The first position must be of MULTISET type.");
                                }
                                final DataType second = args.get(1);
                                if (second.getLogicalType().getTypeRoot() != LogicalTypeRoot.INTEGER) {
                                    return callContext.fail(throwOnFailure, "The second position must be of INTEGER type.");
                                }
                                final DataType third = args.get(2);
                                if (third.getLogicalType().getTypeRoot() != LogicalTypeRoot.INTEGER) {
                                    return callContext.fail(throwOnFailure, "The third position must be of INTEGER type.");
                                }
                                return Optional.of(
                                        Arrays.asList(
                                                first.bridgedTo(Map.class),
                                                second.bridgedTo(Integer.class),
                                                third.bridgedTo(Integer.class)
                                        ));
                            }

                            @Override
                            public List<Signature> getExpectedSignatures(
                                    FunctionDefinition definition) {
                                return Arrays.asList(
                                        Signature.of(Signature.Argument.ofGroup(LogicalTypeRoot.MULTISET)),
                                        Signature.of(Signature.Argument.ofGroup(LogicalTypeRoot.INTEGER)),
                                        Signature.of(Signature.Argument.ofGroup(LogicalTypeRoot.INTEGER))
                                );
                            }
                        })
                .outputTypeStrategy(
                        callContext -> {
                            final List<DataType> args = callContext.getArgumentDataTypes();
                            final DataType dataType = args.get(0);
                            if (dataType instanceof CollectionDataType) {
                                DataType elementType = ((CollectionDataType) dataType).getElementDataType();
                                return Optional.of(DataTypes.ARRAY(elementType));
                            }
                            return Optional.empty();
                        })
                .build();
    }

    public <K, V> K[] eval(Map<K, V> map, Integer sortIndex, Integer limit) {
        List<K> sorted = map.keySet().stream().sorted((k1, k2) -> {
            if (k1 == null) {
                return -1;
            }
            if (k2 == null) {
                return 1;
            }
            if (k1 instanceof Row && k2 instanceof Row) {
                Row row1 = (Row) k1;
                Row row2 = (Row) k2;
                // Access the field by name
                Comparable<Object> value1 = (Comparable<Object>) row1.getField(sortIndex);
                Comparable<Object> value2 = (Comparable<Object>) row2.getField(sortIndex);

                return value2.compareTo(value1);
            }
            Comparable<Object> o1 = (Comparable<Object>) k1;
            Comparable<Object> o2 = (Comparable<Object>) k2;
            return o2.compareTo(o1);
        }).collect(Collectors.toList());

        // Apply limit (if provided)
        if (limit != null && limit < sorted.size()) {
            sorted = sorted.subList(0, limit);
        }

        return Conversions.listToArray(sorted);
    }
}
