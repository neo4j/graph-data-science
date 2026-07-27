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
package org.neo4j.gds.core.write;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.properties.nodes.FloatArrayNodePropertyValues;
import org.neo4j.gds.api.properties.nodes.NodePropertyRecord;
import org.neo4j.values.storable.Float32Vector;
import org.neo4j.values.storable.Values;

import static org.assertj.core.api.Assertions.assertThat;

class Neo4jNodePropertyTest {

    private static FloatArrayNodePropertyValues floatEmbedding() {
        return new FloatArrayNodePropertyValues() {
            @Override
            public float[] floatArrayValue(long nodeId) {
                return new float[]{1F, 2F, 3F};
            }

            @Override
            public long nodeCount() {
                return 1;
            }
        };
    }

    @Test
    void writesVectorValueWhenRecordRequestsIt() {
        var record = NodePropertyRecord.of("embedding", floatEmbedding(), true);

        var neo4jNodeProperty = Neo4jNodeProperty.of(record);

        assertThat(neo4jNodeProperty.values().neo4jValue(0))
            .isInstanceOf(Float32Vector.class)
            .isEqualTo(Values.float32Vector(1F, 2F, 3F));
    }

    @Test
    void writesPlainArrayByDefault() {
        var record = NodePropertyRecord.of("embedding", floatEmbedding());

        var neo4jNodeProperty = Neo4jNodeProperty.of(record);

        assertThat(neo4jNodeProperty.values().neo4jValue(0))
            .isEqualTo(Values.floatArray(new float[]{1F, 2F, 3F}));
    }
}
