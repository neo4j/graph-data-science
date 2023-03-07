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
package org.neo4j.gds.ml.pipeline.node.regression.predict;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.catalog.GraphStreamNodePropertiesProc;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.extension.Neo4jModelCatalogExtension;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.core.Is.isA;

@Neo4jModelCatalogExtension
final class NodeRegressionPipelineMutateProcTest extends BaseProcTest {

    private static final String GRAPH_NAME = "g";

    @Neo4jGraph
    static String GDL =
        "CREATE " +
        "  (n0:N {a: 1.0, b: 0.8, c: 1})" +
        ", (n1:N {a: 2.0, b: 1.0, c: 1})" +
        ", (n2:N {a: 3.0, b: 1.5, c: 1})" +
        ", (n3:N {a: 0.0, b: 2.8, c: 1})" +
        ", (n4:N {a: 1.0, b: 0.9, c: 1})" +
        ", (n1)-[:T]->(n2)" +
        ", (n3)-[:T]->(n4)" +
        ", (n1)-[:T]->(n3)" +
        ", (n2)-[:T]->(n4)";

    @Inject
    private ModelCatalog modelCatalog;

    private static final String SIMPLE_MODEL_NAME = "simpleModel";

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            GraphProjectProc.class,
            GraphStreamNodePropertiesProc.class,
            NodeRegressionPipelineMutateProc.class
        );
        String createQuery = GdsCypher.call(GRAPH_NAME)
            .graphProject()
            .withNodeLabel("N")
            .withRelationshipType("T", Orientation.UNDIRECTED)
            .withNodeProperties(List.of("a", "b", "c"), DefaultValue.DEFAULT)
            .yields();

        runQuery(createQuery);

        modelCatalog.set(NodeRegressionModelTestUtil.createModel(
            getUsername(),
            SIMPLE_MODEL_NAME,
            NodeRegressionModelTestUtil.createModelData(new double[] {0.5, 1.0, -2.0}, 100).data(),
            Stream.of("a", "b", "c")
        ));
    }

    @Test
    void mutate() {
        var graphStore = GraphStoreCatalog.get(getUsername(), DatabaseId.of(db), GRAPH_NAME).graphStore();
        assertThat(graphStore.nodePropertyKeys()).doesNotContain("p");

        assertCypherResult(
            "CALL gds.alpha.pipeline.nodeRegression.predict.mutate($graph, {modelName: $model, mutateProperty: 'p'})",
            Map.of("graph", GRAPH_NAME, "model", SIMPLE_MODEL_NAME),
            List.of(Map.of(
                "computeMillis", greaterThanOrEqualTo(0L),
                "preProcessingMillis", greaterThanOrEqualTo(0L),
                "postProcessingMillis", greaterThanOrEqualTo(0L),
                "mutateMillis", greaterThanOrEqualTo(0L),
                "configuration", isA(Map.class),
                "nodePropertiesWritten", 5L
            ))
        );

        assertThat(graphStore.nodePropertyKeys()).contains("p");
        var propertyValues = graphStore.nodeProperty("p").values();
        assertThat(propertyValues.nodeCount()).isEqualTo(5);

        assertThat(propertyValues.doubleValue(idFunction.of("n0"))).isEqualTo(99.3);
        assertThat(propertyValues.doubleValue(idFunction.of("n1"))).isEqualTo(100.0);
        assertThat(propertyValues.doubleValue(idFunction.of("n2"))).isEqualTo(101.0);
        assertThat(propertyValues.doubleValue(idFunction.of("n3"))).isEqualTo(100.8);
        assertThat(propertyValues.doubleValue(idFunction.of("n4"))).isEqualTo(99.4);
    }

}
