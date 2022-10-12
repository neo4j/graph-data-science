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
import org.neo4j.gds.config.ConcurrencyConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.TestSupport.idMap;

public class LongNodePropertiesBuilderTest {

    @Test
    void singleLabelAssignmentWithNonDirectMapping() {
        int nodeCount = 10;
        var defaultValue = DefaultValue.of(10L);

        var originalIds = new long[nodeCount];
        for (int i = 0; i < nodeCount; i++) {
            originalIds[i] = i * 42L;
        }

        var idMap = idMap(originalIds);

        var builder = LongNodePropertiesBuilder.of(
            defaultValue,
            ConcurrencyConfig.DEFAULT_CONCURRENCY
        );

        for (int i = 0; i < nodeCount; i++) {
            builder.set(originalIds[i], i * 1337L);
        }

        var longNodeProperties = builder.build(10, idMap, idMap.highestOriginalId());

        for (int i = 0; i < nodeCount; i++) {
            assertThat(longNodeProperties.longValue(i)).isEqualTo(i * 1337L);
        }
    }
}
