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
package org.neo4j.gds.pregel.proc;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.logging.NullLog;

import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@GdlExtension
class PregelBaseProcTest {


    @GdlGraph
    @GdlGraph(graphNamePrefix = "undirected", orientation = Orientation.UNDIRECTED)
    public static String GDL = "CREATE " + "   ()-[:REL]->()," + "   ()-[:REL2]->(),";


    @Inject
    GraphStore graphStore;

    @Inject
    GraphStore undirectedGraphStore;

    static Stream<Arguments> relTypeCombinations() {
        var rel = RelationshipType.of("REL");
        var rel2 = RelationshipType.of("REL2");
        return Stream.of(Arguments.arguments(List.of(rel)),
            Arguments.arguments(List.of(rel2)),
            Arguments.arguments(List.of(rel, rel2))
        );
    }

    @ParameterizedTest
    @MethodSource("relTypeCombinations")
    void shouldGenerateInverseIndexes(List<RelationshipType> relTypes) {
        PregelBaseProc.ensureInverseIndexesExist(graphStore,
            relTypes,
            4,
            NullLog.getInstance(),
            TaskRegistryFactory.empty()
        );
        assertThat(graphStore.inverseIndexedRelationshipTypes()).containsExactlyElementsOf(relTypes);
    }

    @Test
    void shouldThrowOnUndirectedRelTypes() {
        assertThatThrownBy(() -> PregelBaseProc.ensureDirectedRelationships(
            undirectedGraphStore,
            RelationshipType.listOf("REL", "REL2")
        )).hasMessage(
            "This algorithm requires a directed graph, but the following configured relationship types are undirected: ['REL', 'REL2']."
        );
    }
}
