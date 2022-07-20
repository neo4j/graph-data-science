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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;

import java.util.List;

@GdlExtension
class FeaturePropertiesConfigTest {

    @GdlGraph
    static final String GDL_GRAPH = "(:A {a: 1}), (:B {b: 1}), (:AB {a: 1, b: 1})";

    @Inject
    GraphStore graphStore;

    @Test
    void validateMustExistOnAllLabels() {
        var config = TestAllLabelsFeatureConfigImpl.builder().featureProperties(List.of("a", "b")).build();

        Assertions.assertThatThrownBy(() -> config.validateFeatureProperties(graphStore, List.of(NodeLabel.of("A"), NodeLabel.of("AB")), List.of()))
            .hasMessage("The feature properties ['b'] are not present for all requested labels. " +
                        "Requested labels: ['A', 'AB']. Properties available on all requested labels: ['a']");

    }

    @Test
    void validateMustExistOnAtLeastOneLabel() {
        var config = TestSomeLabelsFeatureConfigImpl.builder().featureProperties(List.of("a", "c")).build();

        Assertions.assertThatThrownBy(() -> config.validateFeatureProperties(graphStore, List.of(NodeLabel.of("A"), NodeLabel.of("AB")), List.of()))
            .hasMessage("The feature properties ['c'] are not present for any of the requested labels. " +
                        "Requested labels: ['A', 'AB']. Properties available on the requested labels: ['a', 'b']");
    }

    @Configuration
    interface TestAllLabelsFeatureConfig extends FeaturePropertiesConfig {
        @Override
        @Configuration.Ignore
        default boolean propertiesMustExistForEachNodeLabel() {
            return true;
        }
    }

    @Configuration
    interface TestSomeLabelsFeatureConfig extends FeaturePropertiesConfig {
        @Override
        @Configuration.Ignore
        default boolean propertiesMustExistForEachNodeLabel() {
            return false;
        }
    }

}
