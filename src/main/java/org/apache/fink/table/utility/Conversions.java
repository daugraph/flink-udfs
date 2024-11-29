package org.apache.fink.table.utility;

import java.lang.reflect.Array;
import java.util.List;

public class Conversions {
    @SuppressWarnings("unchecked")
    public static <K> K[] listToArray(List<K> list) {
        if (list == null || list.isEmpty()) {
            return null;
        }
        K[] array = (K[]) Array.newInstance(list.get(0).getClass(), list.size());
        return list.toArray(array);
    }
}
