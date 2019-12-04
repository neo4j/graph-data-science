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

import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.provider.Arguments;
import org.neo4j.graphalgo.core.loading.GraphCatalog;
import org.neo4j.graphalgo.louvain.LouvainStreamProc;
import org.neo4j.graphalgo.louvain.LouvainWriteProc;
import org.neo4j.graphalgo.newapi.GraphCatalogProcs;
import org.neo4j.internal.kernel.api.exceptions.KernelException;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.params.provider.Arguments.arguments;

class LouvainProcTestBase extends ProcTestBase implements ProcTestBaseExtensions {

    static final List<List<Long>> RESULT = Arrays.asList(
        Arrays.asList(0L, 1L, 2L, 3L, 4L, 5L, 14L),
        Arrays.asList(6L, 7L, 8L),
        Arrays.asList(9L, 10L, 11L, 12L, 13L)
    );

    @BeforeEach
    void setupGraph() throws KernelException {

        db = TestDatabaseCreator.createTestDatabase();

        @Language("Cypher") String cypher =
            "CREATE" +
            "  (a:Node {seed: 1})" +        // 0
            ", (b:Node {seed: 1})" +        // 1
            ", (c:Node {seed: 1})" +        // 2
            ", (d:Node {seed: 1})" +        // 3
            ", (e:Node {seed: 1})" +        // 4
            ", (f:Node {seed: 1})" +        // 5
            ", (g:Node {seed: 2})" +        // 6
            ", (h:Node {seed: 2})" +        // 7
            ", (i:Node {seed: 2})" +        // 8
            ", (j:Node {seed: 42})" +       // 9
            ", (k:Node {seed: 42})" +       // 10
            ", (l:Node {seed: 42})" +       // 11
            ", (m:Node {seed: 42})" +       // 12
            ", (n:Node {seed: 42})" +       // 13
            ", (x:Node {seed: 1})" +        // 14

            ", (a)-[:TYPE {weight: 1.0}]->(b)" +
            ", (a)-[:TYPE {weight: 1.0}]->(d)" +
            ", (a)-[:TYPE {weight: 1.0}]->(f)" +
            ", (b)-[:TYPE {weight: 1.0}]->(d)" +
            ", (b)-[:TYPE {weight: 1.0}]->(x)" +
            ", (b)-[:TYPE {weight: 1.0}]->(g)" +
            ", (b)-[:TYPE {weight: 1.0}]->(e)" +
            ", (c)-[:TYPE {weight: 1.0}]->(x)" +
            ", (c)-[:TYPE {weight: 1.0}]->(f)" +
            ", (d)-[:TYPE {weight: 1.0}]->(k)" +
            ", (e)-[:TYPE {weight: 1.0}]->(x)" +
            ", (e)-[:TYPE {weight: 0.01}]->(f)" +
            ", (e)-[:TYPE {weight: 1.0}]->(h)" +
            ", (f)-[:TYPE {weight: 1.0}]->(g)" +
            ", (g)-[:TYPE {weight: 1.0}]->(h)" +
            ", (h)-[:TYPE {weight: 1.0}]->(i)" +
            ", (h)-[:TYPE {weight: 1.0}]->(j)" +
            ", (i)-[:TYPE {weight: 1.0}]->(k)" +
            ", (j)-[:TYPE {weight: 1.0}]->(k)" +
            ", (j)-[:TYPE {weight: 1.0}]->(m)" +
            ", (j)-[:TYPE {weight: 1.0}]->(n)" +
            ", (k)-[:TYPE {weight: 1.0}]->(m)" +
            ", (k)-[:TYPE {weight: 1.0}]->(l)" +
            ", (l)-[:TYPE {weight: 1.0}]->(n)" +
            ", (m)-[:TYPE {weight: 1.0}]->(n)";

        registerProcedures(LouvainStreamProc.class, LouvainWriteProc.class, GraphLoadProc.class, GraphCatalogProcs.class);
        runQuery(cypher);
        runQuery("CALL algo.beta.graph.create(" +
                 "    'myGraph'," +
                 "    {" +
                 "      Node: {" +
                 "        label: 'Node'," +
                 "        properties: ['seed']" +
                 "      }" +
                 "    }," +
                 "    {" +
                 "      TYPE: {" +
                 "        type: 'TYPE'," +
                 "        projection: 'UNDIRECTED'" +
                 "      }" +
                 "    }" +
                 ")");
    }

    @AfterEach
    void clearCommunities() {
        db.shutdown();
        GraphCatalog.removeAllLoadedGraphs();
    }

    static Stream<Arguments> graphVariations() {
        return Stream.of(
            arguments("'myGraph', {", "explicit graph"),
            arguments(
                "{" +
                "  nodeProjection: ['Node']," +
                "  relationshipProjection: {" +
                "    TYPE: {" +
                "      type: 'TYPE'," +
                "      projection: 'UNDIRECTED'" +
                "    }" +
                "  }," +
                "  nodeProperties: ['seed'],",
                "implicit graph"
            )
        );
    }
}
