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
package org.neo4j.graphalgo.core;

import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.TestSupport.AllGraphTypesWithoutCypherTest;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;

import static org.junit.jupiter.api.Assertions.assertThrows;

final class GraphLoaderNegativeTest extends RandomGraphTestCase {

    @AllGraphTypesWithoutCypherTest
    void shouldThrowForNonExistingStringLabel(Class<? extends GraphFactory> graphImpl) {
        assertThrows(
                IllegalArgumentException.class,
                () -> new GraphLoader(RandomGraphTestCase.db).withLabel("foo").load(graphImpl),
                "Node label not found: 'foo'"
        );
    }

    @AllGraphTypesWithoutCypherTest
    void shouldThrowForNonExistingLabel(Class<? extends GraphFactory> graphImpl) {
        assertThrows(
                IllegalArgumentException.class,
                () -> new GraphLoader(RandomGraphTestCase.db).withLabel(Label.label("foo")).load(graphImpl),
                "Node label not found: 'foo'"
        );
    }

    @AllGraphTypesWithoutCypherTest
    void shouldThrowForNonExistingStringRelType(Class<? extends GraphFactory> graphImpl) {
        assertThrows(
                IllegalArgumentException.class,
                () -> new GraphLoader(RandomGraphTestCase.db).withRelationshipType("foo").load(graphImpl),
                ("Relationship type(s) not found: 'foo'")
        );
    }

    @AllGraphTypesWithoutCypherTest
    void shouldThrowForNonExistingRelType(Class<? extends GraphFactory> graphImpl) {
        assertThrows(
                IllegalArgumentException.class,
                () -> new GraphLoader(RandomGraphTestCase.db).withRelationshipType(RelationshipType.withName("foo")).load(graphImpl),
                "Relationship type(s) not found: 'foo'"
        );
    }

    @AllGraphTypesWithoutCypherTest
    void shouldThrowForNonExistingNodeProperty(Class<? extends GraphFactory> graphImpl) {
        assertThrows(
                IllegalArgumentException.class,
                () -> new GraphLoader(RandomGraphTestCase.db)
                        .withOptionalNodeProperties(new PropertyMapping("foo", "foo", 0.0))
                        .load(graphImpl),
                "Node properties not found: 'foo'"
        );
    }
}
