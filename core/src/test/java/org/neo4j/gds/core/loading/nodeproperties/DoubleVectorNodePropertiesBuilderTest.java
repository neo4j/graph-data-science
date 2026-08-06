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
package org.neo4j.gds.core.loading.nodeproperties;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.NodeIdMapper;
import org.neo4j.gds.api.properties.nodes.DoubleVectorNodePropertyValues;
import org.neo4j.gds.config.ConcurrencyConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DoubleVectorNodePropertiesBuilderTest {

    @Test
    void determinesDimensionCorrectly() {
        DoubleVectorNodePropertiesBuilder builder = new DoubleVectorNodePropertiesBuilder(DefaultValue.of(new double[]{1.0, 2.0, 3.0}), ConcurrencyConfig.TYPED_DEFAULT_CONCURRENCY);
        DoubleVectorNodePropertyValues values = builder.build(2, NodeIdMapper.IDENTITY, 1);
        assertThat(values.vectorDimension()).isEqualTo(3);
    }

    @Test
    void setsDimensionWhenNotSetYet() {
        DoubleVectorNodePropertiesBuilder builder = new DoubleVectorNodePropertiesBuilder(DefaultValue.forDoubleArray(), ConcurrencyConfig.TYPED_DEFAULT_CONCURRENCY);
        builder.set(0, new double[]{1.0, 2.0});
        DoubleVectorNodePropertyValues values = builder.build(2, NodeIdMapper.IDENTITY, 1);
        assertThat(values.vectorDimension()).isEqualTo(2);
    }

    @Test
    void throwsOnSetValueWithDifferentDimension() {
        DoubleVectorNodePropertiesBuilder builder = new DoubleVectorNodePropertiesBuilder(DefaultValue.of(new double[]{1.0, 2.0, 3.0}), ConcurrencyConfig.TYPED_DEFAULT_CONCURRENCY);
        assertThatThrownBy(() -> builder.set(0, new double[]{1.0, 2.0})).isInstanceOf(IllegalArgumentException.class);
    }

}
