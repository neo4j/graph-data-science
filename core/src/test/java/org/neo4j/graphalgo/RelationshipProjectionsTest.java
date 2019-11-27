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

import java.util.Arrays;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.neo4j.graphalgo.core.DeduplicationStrategy.SINGLE;
import static org.neo4j.helpers.collection.MapUtil.map;

class RelationshipProjectionsTest {

    @Test
    void shouldParse() {
        Map<String, Object> noProperties = map(
            "MY_TYPE", map(
                "type", "T",
                "projection", "NATURAL",
                "aggregation", "SINGLE"
            ),
            "ANOTHER", map(
                "type", "FOO",
                "properties", Arrays.asList(
                    "prop1", "prop2"
                )
            )
        );

        RelationshipProjections projections = RelationshipProjections.fromObject(noProperties);
        assertThat(projections.allFilters(), hasSize(2));
        assertThat(
            projections.getFilter(ElementIdentifier.of("MY_TYPE")),
            equalTo(RelationshipProjection.of("T", Projection.NATURAL, SINGLE))
        );
        assertThat(
            projections.getFilter(ElementIdentifier.of("ANOTHER")),
            equalTo(RelationshipProjection
                .builder()
                .type("FOO")
                .properties(PropertyMappings.of(PropertyMapping.of("prop1", Double.NaN), PropertyMapping.of("prop2", Double.NaN))
            ))
        );
        assertThat(projections.typeFilter(), equalTo("T|FOO"));
    }

}
