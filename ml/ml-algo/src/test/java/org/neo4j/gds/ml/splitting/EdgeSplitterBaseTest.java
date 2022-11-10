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
package org.neo4j.gds.ml.splitting;

import org.apache.commons.lang3.mutable.MutableInt;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.Relationships;

import java.util.Collection;

import static org.assertj.core.api.Assertions.assertThat;

abstract class EdgeSplitterBaseTest {
    void assertRelExists(Relationships.Topology topology, long source, long... targets) {
        var cursor = topology.adjacencyList().adjacencyCursor(source);
        for (long target : targets) {
            assertThat(cursor.nextVLong()).isEqualTo(target);
        }
    }

    void assertRelProperties(
        Relationships.Properties properties,
        long source,
        double... values
    ) {
        var cursor = properties.propertiesList().propertyCursor(source);
        for (double property : values) {
            assertThat(Double.longBitsToDouble(cursor.nextLong())).isEqualTo(property);
        }
    }

    void assertRelSamplingProperties(Relationships selectedRels, Graph inputGraph) {
        MutableInt positiveCount = new MutableInt();
        inputGraph.forEachNode(source -> {
            var targetNodeCursor = selectedRels.topology().adjacencyList().adjacencyCursor(source);
            var propertyCursor = selectedRels.properties().get().propertiesList().propertyCursor(source);
            while (targetNodeCursor.hasNextVLong()) {
                boolean edgeIsPositive = Double.longBitsToDouble(propertyCursor.nextLong()) == EdgeSplitter.POSITIVE;
                if (edgeIsPositive) positiveCount.increment();
                assertThat(edgeIsPositive)
                    .isEqualTo(inputGraph.exists(source, targetNodeCursor.nextVLong()));
            }
            assertThat(propertyCursor.hasNextLong()).isFalse();
            return true;
        });

        assertThat(selectedRels.topology().elementCount()).isEqualTo(positiveCount.longValue());
    }

    void assertNodeLabelFilter(Relationships.Topology topology, Collection<NodeLabel> sourceLabels, Collection<NodeLabel> targetLabels, IdMap idmap) {
        idmap.forEachNode(sourceNode -> {
            var targetNodeCursor = topology.adjacencyList().adjacencyCursor(sourceNode);
            if (targetNodeCursor.hasNextVLong()) { assertThat(idmap.nodeLabels(sourceNode).stream().filter(sourceLabels::contains)).isNotEmpty(); }
            while (targetNodeCursor.hasNextVLong()) {
                assertThat(idmap.nodeLabels(targetNodeCursor.nextVLong()).stream().filter(targetLabels::contains)).isNotEmpty();
            }
            return true;
        });
    }

    void assertRelInGraph(Relationships relationships, Graph inputGraph) {
        inputGraph.forEachNode(source -> {
            var targetNodeCursor = relationships.topology().adjacencyList().adjacencyCursor(source);
            var propertyCursor = relationships.properties().map(p -> p.propertiesList().propertyCursor(source));
            while (targetNodeCursor.hasNextVLong()) {
                var targetNode = targetNodeCursor.nextVLong();
                assertThat(inputGraph.exists(source, targetNode)).isTrue();
                propertyCursor.ifPresent(cursor -> {
                    assertThat(inputGraph.relationshipProperty(source, targetNode)).isEqualTo(Double.longBitsToDouble(cursor.nextLong()));
                });

            }
            return true;
        });
    }
}
