/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.newapi.BaseAlgoConfig;
import org.neo4j.graphalgo.newapi.WriteConfig;
import org.neo4j.helpers.collection.MapUtil;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public interface WriteConfigTests<CONFIG extends WriteConfig & BaseAlgoConfig, RESULT> extends BaseProcTests<CONFIG, RESULT> {

    @Test
    default void testMissingWritePropertyFails() {
        CypherMapWrapper mapWrapper =
            createMinimallyValidConfig(CypherMapWrapper.empty())
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
    default void testWriteConfig() {
        CypherMapWrapper mapWrapper = CypherMapWrapper.create(MapUtil.map(
            "writeProperty", "writeProperty",
            "writeConcurrency", 42
        ));
        CONFIG config = createConfig(createMinimallyValidConfig(mapWrapper));
        assertEquals("writeProperty", config.writeProperty());
        assertEquals(42, config.writeConcurrency());
    }
}
