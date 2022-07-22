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
package org.neo4j.gds.config;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.gdl.GdlFactory;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RelationshipWeightConfigTest {

    @Test
    void assertRelationshipWeightPropertyCanBeConfigured() {
        var relationshipWeightConfig = CypherMapWrapper.empty().withString("relationshipWeightProperty", "foo");
        var algoConfig = TestRelationshipWeightConfig.of(relationshipWeightConfig);
        assertThat(algoConfig.relationshipWeightProperty()).isEqualTo(Optional.of("foo"));
    }

    @Test
    void assertDefaultRelationshipWeightPropertyIsNotPresent() {
        var algoConfig = TestRelationshipWeightConfig.of(CypherMapWrapper.empty());
        assertThat(algoConfig.relationshipWeightProperty()).isNotPresent();
    }

    @Test
    void assertBlankRelationshipWeightPropertyIsNull() {
        var relationshipWeightConfig = CypherMapWrapper.empty().withString("relationshipWeightProperty", "  ");
        assertThatThrownBy(() -> TestRelationshipWeightConfig.of(relationshipWeightConfig))
            .hasMessage("`relationshipWeightProperty` must not end or begin with whitespace characters, but got `  `.");
    }

    @Test
    void assertEmptyRelationshipWeightPropertyIsNull() {
        var configAsMap = CypherMapWrapper.empty().toMap();
        configAsMap.put("relationshipWeightProperty", null);
        var relationshipWeightConfig = CypherMapWrapper.create(configAsMap);
        var algoConfig = TestRelationshipWeightConfig.of(relationshipWeightConfig);
        assertThat(algoConfig.relationshipWeightProperty()).isNotPresent();
    }


    @Test
    void assertRelationshipWeightPropertyIsValid() {
        var graphStore = GdlFactory
            .of("()-[:A {rrw: 4}]->()-[:A {rw: 3}]->(), ()-[:A {rw: 2}]->(), ()-[:A {rw: 1}]->()")
            .build();

        var relationshipWeightConfig = CypherMapWrapper.empty().withString("relationshipWeightProperty", "rw");
        var algoConfig = TestRelationshipWeightConfig.of(relationshipWeightConfig);


        assertThatCode(() -> algoConfig.graphStoreValidation(graphStore, graphStore.nodeLabels(), graphStore.relationshipTypes())).doesNotThrowAnyException();
    }

    @Test
    void assertRelationshipWeightPropertyIsInvalid() {
        var graphStore = GdlFactory.of("()-[:A {foo: 1}]->()").build();
        var relationshipWeightConfig = CypherMapWrapper.empty().withString("relationshipWeightProperty", "bar");
        var algoConfig = TestRelationshipWeightConfig.of(relationshipWeightConfig);

        assertThatThrownBy(() -> algoConfig.graphStoreValidation(graphStore, graphStore.nodeLabels(), graphStore.relationshipTypes()))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage(
                "Relationship weight property `bar` not found in relationship types ['A']. Properties existing on all relationship types: ['foo']"
            );
    }

    @Configuration
    interface TestRelationshipWeightConfig extends AlgoBaseConfig, RelationshipWeightConfig {
        static TestRelationshipWeightConfig of(CypherMapWrapper map) {
            return new TestRelationshipWeightConfigImpl(map);
        }
    }
}
