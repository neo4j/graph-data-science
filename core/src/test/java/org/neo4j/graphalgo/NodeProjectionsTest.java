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
import java.util.Optional;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.neo4j.helpers.collection.MapUtil.map;

class NodeProjectionsTest {

    @Test
    void shouldParseWithoutProperties() {
        Map<String, Object> noProperties = map(
            "MY_LABEL", map(
                "label", "A"
            )
        );

        NodeProjections projections = NodeProjections.fromObject(noProperties);
        assertThat(projections.allProjections(), hasSize(1));
        assertThat(
            projections.getProjection(ElementIdentifier.of("MY_LABEL")),
            equalTo(NodeProjection.of("A", PropertyMappings.of()))
        );
        assertThat(projections.labelProjection(), equalTo(Optional.of("A")));
    }

    @Test
    void shouldParseWithProperties() {
        Map<String, Object> withProperties = map(
            "MY_LABEL", map(
                "label", "A",
                "properties", Arrays.asList(
                    "prop1", "prop2"
                )
            )
        );

        NodeProjections projections = NodeProjections.fromObject(withProperties);
        assertThat(
            projections.getProjection(ElementIdentifier.of("MY_LABEL")),
            equalTo(NodeProjection
                .builder()
                .label("A")
                .properties(PropertyMappings
                    .builder()
                    .addMapping(PropertyMapping.of("prop1", Double.NaN))
                    .addMapping(PropertyMapping.of("prop2", Double.NaN))
                    .build()
                )
                .build()
            )
        );
        assertThat(projections.labelProjection(), equalTo(Optional.of("A")));
    }
}