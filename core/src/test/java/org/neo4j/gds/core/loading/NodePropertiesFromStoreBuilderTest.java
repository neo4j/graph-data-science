/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.gds.core.loading;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.TestSupport;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.loading.nodeproperties.NodePropertiesFromStoreBuilder;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.neo4j.gds.TestSupport.idMap;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

final class NodePropertiesFromStoreBuilderTest {

    @Test
    void testEmptyDoubleProperties() {
        var nodeCount = 100_000;
        var properties = NodePropertiesFromStoreBuilder.of(
            DefaultValue.of(42.0D),
            1
        ).build(idMap(nodeCount));

        assertEquals(0L, properties.nodeCount());
        assertEquals(OptionalDouble.empty(), properties.getMaxDoublePropertyValue());
        assertEquals(42.0, properties.doubleValue(0));
    }

    @Test
    void testEmptyLongProperties() {
        var nodeCount = 100_000;
        var properties = NodePropertiesFromStoreBuilder.of(
            DefaultValue.of(42L),
            1
        ).build(idMap(nodeCount));

        assertEquals(0L, properties.nodeCount());
        assertEquals(OptionalLong.empty(), properties.getMaxLongPropertyValue());
        assertEquals(42, properties.longValue(0));
    }

    @Test
    void returnsValuesThatHaveBeenSet() {
        var properties = createNodeProperties(2L, 42.0, b -> b.set(1, Values.of(1.0)));

        assertEquals(1.0, properties.doubleValue(1));
        assertEquals(42.0, properties.doubleValue(0));
    }

    @Test
    void shouldReturnLongArrays() {
        long[] data = {42L, 1337L};
        long[] defaultValue = new long[2];
        NodePropertyValues properties = createNodeProperties(
            2L,
            defaultValue,
            b -> b.set(1, Values.of(data))
        );

        assertArrayEquals(data, properties.longArrayValue(1));
        assertArrayEquals(defaultValue, properties.longArrayValue(0));
    }

    @Test
    void shouldReturnDoubleArrays() {
        double[] data = {42.2D, 1337.1D};
        double[] defaultValue = new double[2];
        NodePropertyValues properties = createNodeProperties(
            2L,
            defaultValue,
            b -> b.set(1, Values.of(data))
        );

        assertArrayEquals(data, properties.doubleArrayValue(1));
        assertArrayEquals(defaultValue, properties.doubleArrayValue(0));
    }

    @Test
    void shouldReturnFloatArrays() {
        float[] data = {42.2F, 1337.1F};
        float[] defaultValue = new float[2];
        NodePropertyValues properties = createNodeProperties(
            2L,
            defaultValue,
            b -> b.set(1, Values.of(data))
        );

        assertArrayEquals(data, properties.floatArrayValue(1));
        assertArrayEquals(defaultValue, properties.floatArrayValue(0));
    }

    @Test
    void shouldCastFromFloatArrayToDoubleArray() {
        float[] floatData = {42.2F, 1337.1F};
        double[] doubleData = {42.2D, 1337.1D};
        float[] defaultValue = new float[2];
        NodePropertyValues properties = createNodeProperties(
            2L,
            defaultValue,
            b -> b.set(1, Values.of(floatData))
        );

        assertArrayEquals(floatData, properties.floatArrayValue(1));
        double[] doubleArray = properties.doubleArrayValue(1);
        for (int i = 0; i < floatData.length; i++) {
            assertEquals(doubleData[i], doubleArray[i], 0.0001D);
        }
        assertArrayEquals(defaultValue, properties.floatArrayValue(0));
    }

    @Test
    void shouldCastFromDoubleArrayToFloatArray() {
        double[] doubleData = {42.2D, 1337.1D};
        float[] floatData = {42.2F, 1337.1F};
        double[] defaultValue = new double[2];
        NodePropertyValues properties = createNodeProperties(
            2L,
            defaultValue,
            b -> b.set(1, Values.of(doubleData))
        );

        assertArrayEquals(doubleData, properties.doubleArrayValue(1));
        assertArrayEquals(floatData, properties.floatArrayValue(1));
        assertArrayEquals(defaultValue, properties.doubleArrayValue(0));
    }

    static Stream<Arguments> unsupportedValues() {
        return Stream.of(
            arguments(Values.stringValue("42L")),
            arguments(Values.shortArray(new short[]{(short) 42})),
            arguments(Values.byteArray(new byte[]{(byte) 42})),
            arguments(Values.booleanValue(true)),
            arguments(Values.charValue('c'))
        );
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.gds.core.loading.NodePropertiesFromStoreBuilderTest#unsupportedValues")
    void shouldFailOnUnSupportedTypes(Value data) {
        UnsupportedOperationException ex = assertThrows(
            UnsupportedOperationException.class,
            () -> createNodeProperties(
                2L,
                null,
                b -> b.set(1, data)
            )
        );

        assertThat(ex.getMessage(), containsString("Loading of values of type"));
    }

    private static Stream<Arguments> invalidValueTypeCombinations() {
        Supplier<Stream<Arguments>> scalarValues = () -> Stream.of(2L, 2D).map(Arguments::of);
        Supplier<Stream<Arguments>> arrayValues = () -> Stream.of(new double[]{1D}, new long[]{1L}).map(Arguments::of);

        return Stream.concat(
            TestSupport.crossArguments(scalarValues, arrayValues),
            TestSupport.crossArguments(arrayValues, scalarValues)
        );
    }

    @ParameterizedTest
    @MethodSource("invalidValueTypeCombinations")
    void failOnInvalidDefaultType(Object defaultValue, Object propertyValue) {
        Assertions.assertThatThrownBy(() -> createNodeProperties(1L, defaultValue, b -> {
            b.set(0, Values.of(propertyValue));
        })).isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining(formatWithLocale("Expected type of default value to be `%s`.", propertyValue.getClass().getSimpleName()));
    }

    @Test
    void returnsDefaultOnMissingEntries() {
        var expectedImplicitDefault = 42.0;
        var properties = createNodeProperties(2L, expectedImplicitDefault, b -> {});

        assertEquals(expectedImplicitDefault, properties.doubleValue(2));
    }

    @Test
    void returnNaNIfItWasSet() {
        var properties = createNodeProperties(2L, 42.0, b -> b.set(1, Values.of(Double.NaN)));

        assertEquals(42.0, properties.doubleValue(0));
        assertEquals(Double.NaN, properties.doubleValue(1));
    }

    @Test
    void trackMaxValue() {
        var properties = createNodeProperties(2L, 0.0, b -> {
            b.set(0, Values.of(42));
            b.set(1, Values.of(21));
        });
        var maxPropertyValue = properties.getMaxLongPropertyValue();
        assertTrue(maxPropertyValue.isPresent());
        assertEquals(42, maxPropertyValue.getAsLong());
    }

    @Test
    void hasSize() {
        var properties = createNodeProperties(2L, 0.0, b -> {
            b.set(0, Values.of(42.0));
            b.set(1, Values.of(21.0));
        });
        assertEquals(2, properties.nodeCount());
    }

    @Test
    void shouldHandleNullValues() {
        var nodeCount = 100;

        var builder = NodePropertiesFromStoreBuilder.of(
            DefaultValue.DEFAULT,
            1
        );

        builder.set(0, null);
        builder.set(1, Values.longValue(42L));

        var properties = builder.build(idMap(nodeCount));

        assertEquals(ValueType.LONG, properties.valueType());
        assertEquals(DefaultValue.LONG_DEFAULT_FALLBACK, properties.longValue(0L));
        assertEquals(42L, properties.longValue(1L));
    }

    @Test
    void threadSafety() throws InterruptedException {
        var pool = Executors.newFixedThreadPool(2);
        var nodeSize = 100_000;
        var builder = NodePropertiesFromStoreBuilder.of(
            DefaultValue.of(Double.NaN),
            1
        );

        var phaser = new Phaser(3);
        pool.execute(() -> {
            // wait for start signal
            phaser.arriveAndAwaitAdvance();
            // first task, set the value 2 on every other node, except for 1338 which is set to 2^41
            // the idea is that the maxValue set will read the currentMax of 2, decide to update to 2^41 and write
            // that value, while the other thread will write 2^42 in the meantime. If that happens,
            // this thread would overwrite a new maxValue.
            for (int i = 0; i < nodeSize; i += 2) {
                builder.set(i, Values.of(i == 1338 ? 0x1p41 : 2.0));
            }
        });
        pool.execute(() -> {
            // wait for start signal
            phaser.arriveAndAwaitAdvance();
            // second task, sets the value 1 on every other node, except for 1337 which is set to 2^42
            // Depending on thread scheduling, the write for 2^42 might be overwritten by the first thread
            for (int i = 1; i < nodeSize; i += 2) {
                builder.set(i, Values.of(i == 1337 ? 0x1p42 : 1.0));
            }
        });

        phaser.arriveAndAwaitAdvance();

        pool.shutdown();
        pool.awaitTermination(10, TimeUnit.SECONDS);

        var properties = builder.build(idMap(nodeSize));
        for (int i = 0; i < nodeSize; i++) {
            var expected = i == 1338 ? 0x1p41 : i == 1337 ? 0x1p42 : i % 2 == 0 ? 2.0 : 1.0;
            assertEquals(expected, properties.doubleValue(i), "" + i);
        }
        assertEquals(nodeSize, properties.nodeCount());
        var maxPropertyValue = properties.getMaxDoublePropertyValue();
        assertTrue(maxPropertyValue.isPresent());

        // If write were correctly ordered, this is always true
        // If, however, the write to maxValue were to be non-atomic
        // e.g. `this.maxValue = Math.max(value, this.maxValue);`
        // this would occasionally be 2^41.
        assertEquals(1L << 42, maxPropertyValue.getAsDouble());
    }

    static NodePropertyValues createNodeProperties(long size, Object defaultValue, Consumer<NodePropertiesFromStoreBuilder> buildBlock) {
        var builder = NodePropertiesFromStoreBuilder.of(
            DefaultValue.of(defaultValue),
            1
        );
        buildBlock.accept(builder);
        return builder.build(idMap(size));
    }
}
