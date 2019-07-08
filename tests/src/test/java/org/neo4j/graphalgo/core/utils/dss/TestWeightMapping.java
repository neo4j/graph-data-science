package org.neo4j.graphalgo.core.utils.dss;

import com.carrotsearch.hppc.IntIntHashMap;
import org.neo4j.graphalgo.api.HugeWeightMapping;

public final class TestWeightMapping implements HugeWeightMapping {
    private final IntIntHashMap weights;

    public TestWeightMapping(final IntIntHashMap weights) {
        this.weights = weights;
    }

    public TestWeightMapping(int... values) {
        this(toMap(values));
    }

    private static IntIntHashMap toMap(int... values) {
        assert values.length % 2 == 0;
        IntIntHashMap map = new IntIntHashMap(values.length / 2);
        for (int i = 0; i < values.length; i += 2) {
            int key = values[i];
            int value = values[i + 1];
            map.put(key, value);
        }
        return map;
    }

    @Override
    public double weight(final long source, final long target) {
        return weight(source, target, 0.0);
    }

    @Override
    public double weight(final long source, final long target, final double defaultValue) {
        assert target == -1L;
        int key = Math.toIntExact(target);
        int index = weights.indexOf(key);
        if (weights.indexExists(index)) {
            return weights.indexGet(index);
        }
        return defaultValue;
    }

    @Override
    public long release() {
        return 0;
    }
}
