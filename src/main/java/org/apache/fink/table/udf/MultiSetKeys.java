package org.apache.fink.table.udf;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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

public class MultiSetKeys extends ScalarFunction {

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
                                if (dataType.getLogicalType().getTypeRoot() != LogicalTypeRoot.MULTISET) {
                                    return callContext.fail(throwOnFailure, "One multiset argument expected.");
                                }
                                return Optional.of(Collections.singletonList(dataType.bridgedTo(Map.class)));
                            }

                            @Override
                            public List<Signature> getExpectedSignatures(
                                    FunctionDefinition definition) {
                                return Collections.singletonList(
                                        Signature.of(Signature.Argument.ofGroup(LogicalTypeRoot.MULTISET)));
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

    public <K, V> K[] eval(Map<K, V> map) {
        List<K> keyList = new ArrayList<>(map.keySet());
        return Conversions.listToArray(keyList);
    }

}

