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
package org.neo4j.gds.beta.filter;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.beta.filter.expression.ExpressionParser;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.ExecutorServiceUtil;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.progress.tracking.ProgressTracker;
import org.opencypher.v9_0.parser.javacc.ParseException;

import java.util.ArrayList;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
class NodesFilterTest {

    @GdlGraph(idOffset = 17)
    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:A {p: 1L})" +
        ", (b:A {p: 2L})" +
        ", (c:A {p: 3L})" +
        ", (d:B)";

    @GdlGraph(graphNamePrefix = "vector")
    private static final String VECTOR_DB_CYPHER =
        "CREATE" +
        "  (a:A {p: 1L, emb: vector([1.0D, 2.0D, 3.0D]), embF: vector([1.0F, 2.0F, 3.0F])})" +
        ", (b:A {p: 2L, emb: vector([4.0D, 5.0D, 6.0D]), embF: vector([4.0F, 5.0F, 6.0F])})" +
        ", (c:A {p: 3L, emb: vector([7.0D, 8.0D, 9.0D]), embF: vector([7.0F, 8.0F, 9.0F])})" +
        ", (d:B {emb: vector([0.0D, 0.0D, 0.0D]), embF: vector([0.0F, 0.0F, 0.0F])})";

    @Inject
    private GraphStore graphStore;

    @Inject
    private IdFunction idFunction;

    @Inject
    private GraphStore vectorGraphStore;

    @Inject
    private IdFunction vectorIdFunction;

    @Test
    void basicFiltering() throws ParseException {
        var filteredNodes = NodesFilter.filterNodes(
            graphStore,
            ExpressionParser.parse("n:A AND n.p > 1.0", Map.of()),
            new Concurrency(1),
            Map.of(),
            ExecutorServiceUtil.DEFAULT_SINGLE_THREAD_POOL,
            ProgressTracker.NULL_TRACKER
        );

        var idMap = filteredNodes.idMap();
        var filteredNodeIds = new ArrayList<Long>();

        assertThat(idMap.availableNodeLabels())
            .containsExactly(NodeLabel.of("A"));

        idMap.forEachNode(n -> {
            filteredNodeIds.add(idMap.toOriginalNodeId(n));
            return true;
        });

        assertThat(filteredNodeIds)
            .containsExactlyInAnyOrder(
                idFunction.of("b"),
                idFunction.of("c")
            );
    }

    @Test
    void filterWithDoubleVectorProperty() throws ParseException {
        var filteredNodes = NodesFilter.filterNodes(
            vectorGraphStore,
            ExpressionParser.parse("n:A AND n.p > 1.0", Map.of()),
            new Concurrency(1),
            Map.of(),
            ExecutorServiceUtil.DEFAULT_SINGLE_THREAD_POOL,
            ProgressTracker.NULL_TRACKER
        );

        var idMap = filteredNodes.idMap();

        assertThat(idMap.availableNodeLabels())
            .containsExactly(NodeLabel.of("A"));
        assertThat(idMap.nodeCount()).isEqualTo(2L);

        var embValues = filteredNodes.propertyStores().get("emb").values();

        assertThat(embValues.valueType()).isEqualTo(ValueType.DOUBLE_VECTOR);
        assertThat(embValues.dimension()).contains(3);

        idMap.forEachNode(node -> {
            var originalId = idMap.toOriginalNodeId(node);
            var embedding = embValues.doubleArrayValue(node);

            if (originalId == vectorIdFunction.of("b")) {
                assertThat(embedding).containsExactly(4.0D, 5.0D, 6.0D);
            } else {
                assertThat(embedding).containsExactly(7.0D, 8.0D, 9.0D);
            }
            return true;
        });
    }

    @Test
    void filterWithFloatVectorProperty() throws ParseException {
        var filteredNodes = NodesFilter.filterNodes(
            vectorGraphStore,
            ExpressionParser.parse("n:A AND n.p > 1.0", Map.of()),
            new Concurrency(1),
            Map.of(),
            ExecutorServiceUtil.DEFAULT_SINGLE_THREAD_POOL,
            ProgressTracker.NULL_TRACKER
        );

        var idMap = filteredNodes.idMap();

        assertThat(idMap.nodeCount()).isEqualTo(2L);

        var embValues = filteredNodes.propertyStores().get("embF").values();

        assertThat(embValues.valueType()).isEqualTo(ValueType.FLOAT_VECTOR);
        assertThat(embValues.dimension()).contains(3);

        idMap.forEachNode(node -> {
            var originalId = idMap.toOriginalNodeId(node);
            var embedding = embValues.floatArrayValue(node);

            if (originalId == vectorIdFunction.of("b")) {
                assertThat(embedding).containsExactly(4.0F, 5.0F, 6.0F);
            } else {
                assertThat(embedding).containsExactly(7.0F, 8.0F, 9.0F);
            }
            return true;
        });
    }
}
