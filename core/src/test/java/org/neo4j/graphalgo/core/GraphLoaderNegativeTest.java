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
package org.neo4j.graphalgo.core;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.StoreLoaderBuilder;
import org.neo4j.graphalgo.core.loading.HugeGraphFactory;

import static org.junit.jupiter.api.Assertions.assertThrows;

final class GraphLoaderNegativeTest extends RandomGraphTestCase {

    @Test
    void shouldThrowForNonExistingStringLabel() {
        assertThrows(
            IllegalArgumentException.class,
            () -> new StoreLoaderBuilder()
                .api(RandomGraphTestCase.db)
                .addNodeLabel("foo")
                .build()
                .load(HugeGraphFactory.class),
            "Node label not found: 'foo'"
        );
    }

    @Test
    void shouldThrowForNonExistingStringRelType() {
        assertThrows(
            IllegalArgumentException.class,
            () -> new StoreLoaderBuilder()
                .api(RandomGraphTestCase.db)
                .addRelationshipType("foo")
                .build()
                .load(HugeGraphFactory.class),
            ("Relationship type(s) not found: 'foo'")
        );
    }

    @Test
    void shouldThrowForNonExistingNodeProperty() {
        assertThrows(
            IllegalArgumentException.class,
            () -> new StoreLoaderBuilder().api(RandomGraphTestCase.db)
                .addNodeProperty(PropertyMapping.of("foo", 0.0))
                .build().load(HugeGraphFactory.class),
            "Node properties not found: 'foo'"
        );
    }
}
