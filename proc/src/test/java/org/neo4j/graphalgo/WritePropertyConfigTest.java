/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.compat.MapUtil;
import org.neo4j.graphalgo.config.AlgoBaseConfig;
import org.neo4j.graphalgo.config.WritePropertyConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphdb.Result;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public interface WritePropertyConfigTest<CONFIG extends WritePropertyConfig & AlgoBaseConfig, RESULT> extends AlgoBaseProcTest<CONFIG, RESULT> {

    @Test
    default void testMissingWritePropertyFails() {
        CypherMapWrapper mapWrapper =
            createMinimalConfig(CypherMapWrapper.empty())
                .withoutEntry("writeProperty");

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> createConfig(mapWrapper)
        );
        assertEquals(
            "No value specified for the mandatory configuration parameter `writeProperty`",
            exception.getMessage()
        );
    }

    @Test
    default void testEmptyWritePropertyValues() {
        CypherMapWrapper mapWrapper = CypherMapWrapper.create(MapUtil.map("writeProperty", null));
        assertThrows(IllegalArgumentException.class, () -> createConfig(mapWrapper));
    }

    @Test
    default void testWriteConfig() {
        CypherMapWrapper mapWrapper = CypherMapWrapper.create(MapUtil.map(
            "writeProperty", "writeProperty",
            "writeConcurrency", 3
        ));
        CONFIG config = createConfig(createMinimalConfig(mapWrapper));
        assertEquals("writeProperty", config.writeProperty());
        assertEquals(3, config.writeConcurrency());
    }

    default void checkMillisSet(Result.ResultRow row) {
        assertTrue(row.getNumber("createMillis").intValue() >= 0, "load time not set");
        assertTrue(row.getNumber("computeMillis").intValue() >= 0, "compute time not set");
        assertTrue(row.getNumber("writeMillis").intValue() >= 0, "write time not set");
    }
}
