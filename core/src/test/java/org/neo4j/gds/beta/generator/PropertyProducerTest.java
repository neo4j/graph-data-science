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
package org.neo4j.gds.beta.generator;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.api.nodeproperties.ValueType;
import org.neo4j.graphalgo.beta.generator.PropertyProducer;

import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

class PropertyProducerTest {

    @Test
    void testFixedPropertyProducer() {
        var producer = PropertyProducer.fixedDouble("foo", 0x13p37);
        assertThat(producer)
            .returns("foo", PropertyProducer::getPropertyName)
            .returns(ValueType.DOUBLE, PropertyProducer::propertyType)
            .satisfies(p -> {
                double[] value = {0};
                p.setProperty(value, 0, new Random());
                assertThat(value[0]).isEqualTo(0x13p37);
            });
    }

    @Test
    void testRandomPropertyProducer() {
        var producer = PropertyProducer.randomDouble("foo", 0.0, 1.0);
        assertThat(producer)
            .returns("foo", PropertyProducer::getPropertyName)
            .returns(ValueType.DOUBLE, PropertyProducer::propertyType)
            .satisfies(p -> {
                var random = new Random();

                random.setSeed(42L);
                double expected = random.nextDouble();

                random.setSeed(42L);
                double[] value = {0};
                p.setProperty(value, 0, random);
                assertThat(value[0]).isEqualTo(expected);
            });
    }

    @Test
    void testRandomEmbeddingPropertyProducer() {
        var producer = PropertyProducer.randomEmbedding("foo", 7, 0.0F, 1.0F);
        assertThat(producer)
            .returns("foo", PropertyProducer::getPropertyName)
            .returns(ValueType.FLOAT_ARRAY, PropertyProducer::propertyType)
            .satisfies(p -> {
                var random = new Random();

                random.setSeed(42L);
                var expected = new float[7];
                for (int i = 0; i < expected.length; i++) {
                    expected[i] = random.nextFloat();
                }

                random.setSeed(42L);
                float[][] value = {null};
                p.setProperty(value, 0, random);
                assertThat(value[0]).containsExactly(expected);
            });
    }

    @Test
    void testRandomLongPropertyProducer() {
        var producer = PropertyProducer.randomLong("foo", 5, 10);
        assertThat(producer)
            .returns("foo", PropertyProducer::getPropertyName)
            .returns(ValueType.LONG, PropertyProducer::propertyType)
            .satisfies(p -> {
                var random = new Random();
                long[] value = {0};

                var seedProducingPositiveNumber = 4096L;
                random.setSeed(seedProducingPositiveNumber);
                p.setProperty(value, 0, random);
                assertThat(value[0]).isEqualTo(6);

                var seedProducingNegativeNumber = 1;
                random.setSeed(seedProducingNegativeNumber);
                p.setProperty(value, 0, random);
                assertThat(value[0]).isEqualTo(6);
            });
    }
}
