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
package org.neo4j.gds.ml.nodemodels.logisticregression;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.neo4j.graphalgo.catalog.GraphCreateProc;
import org.neo4j.graphalgo.catalog.GraphStreamNodePropertiesProc;
import org.neo4j.graphalgo.compat.GdsGraphDatabaseAPI;
import org.neo4j.graphalgo.core.model.ModelCatalog;
import org.neo4j.graphalgo.datasets.CommunityDbCreator;
import org.neo4j.graphalgo.datasets.Cora;
import org.neo4j.graphalgo.datasets.DatasetManager;
import org.neo4j.graphalgo.functions.AsNodeFunc;

import java.nio.file.Path;
import java.util.Map;
import java.util.stream.Collectors;

import static org.neo4j.graphalgo.QueryRunner.runQuery;
import static org.neo4j.graphalgo.QueryRunner.runQueryWithResultConsumer;
import static org.neo4j.graphalgo.compat.GraphDatabaseApiProxy.registerFunctions;
import static org.neo4j.graphalgo.compat.GraphDatabaseApiProxy.registerProcedures;
import static org.neo4j.graphalgo.datasets.CoraSchema.CITES_TYPE;
import static org.neo4j.graphalgo.datasets.CoraSchema.EXT_ID_NODE_PROPERTY;
import static org.neo4j.graphalgo.datasets.CoraSchema.PAPER_LABEL;
import static org.neo4j.graphalgo.datasets.CoraSchema.SUBJECT_NODE_PROPERTY;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

class NodeClassificationCoraIntegrationTest {

    private static final String GRAPH_CREATE_QUERY;
    private static final String GRAPH_NAME = "g";
    private static final String CLASS_NODE_PROPERTY = "class";

    static {
        var subjectDictionary = Map.of(
            "Neural_Networks", 0,
            "Rule_Learning", 1,
            "Reinforcement_Learning", 2,
            "Probabilistic_Methods", 3,
            "Theory", 4,
            "Genetic_Algorithms", 5,
            "Case_Based", 6
        );

        var caseWhenExpression = subjectDictionary.entrySet()
            .stream()
            .map(entry -> formatWithLocale(
                "WHEN \\\"%s\\\" THEN %d",
                entry.getKey(),
                entry.getValue()
            ))
            .collect(Collectors.joining(" ", formatWithLocale("CASE n.%s ", SUBJECT_NODE_PROPERTY), " ELSE -1 END"));

        var nodeQuery = formatWithLocale(
            "MATCH (n:%s) RETURN id(n) AS id, labels(n) AS labels, %s AS %3$s, n.%4$s AS %4$s, 0 AS class",
            PAPER_LABEL.name(),
            caseWhenExpression,
            SUBJECT_NODE_PROPERTY,
            EXT_ID_NODE_PROPERTY
        );

        var relationshipQuery = formatWithLocale(
            "MATCH (n:%1$s)-[:%2$s]->(m:%1$s) RETURN id(n) AS source, id(m) AS target, \\\"%2$s\\\" AS type",
            PAPER_LABEL.name(),
            CITES_TYPE
        );

        GRAPH_CREATE_QUERY = formatWithLocale(
            "CALL gds.graph.create.cypher('%s', '%s', '%s')",
            GRAPH_NAME,
            nodeQuery,
            relationshipQuery
        );
    }

    private DatasetManager datasetManager;
    private GdsGraphDatabaseAPI cora;

    @BeforeEach
    void setUp(@TempDir Path tempDir) throws Exception {
        this.datasetManager = new DatasetManager(tempDir, CommunityDbCreator.getInstance());
        this.cora = datasetManager.openDb(Cora.ID);

        registerProcedures(
            cora,
            GraphCreateProc.class,
            GraphStreamNodePropertiesProc.class,
            NodeClassificationTrainProc.class,
            NodeClassificationPredictMutateProc.class
        );

        registerFunctions(cora, AsNodeFunc.class);

        runQuery(cora, GRAPH_CREATE_QUERY, Map.of());
    }

    @AfterEach
    void tearDown() {
        ModelCatalog.removeAllLoadedModels();
        datasetManager.closeDb(cora);
    }

    @Test
    void trainAndPredict() {
        var trainOnN = formatWithLocale(
            "CALL gds.alpha.ml.nodeClassification.train('%s', {" +
            "  nodeLabels: ['%s']," +
            "  modelName: 'model'," +
            "  featureProperties: ['%s', '%s'], " +
            "  targetProperty: '%s', " +
            "  metrics: ['F1_WEIGHTED'], " +
            "  holdoutFraction: 0.2, " +
            "  validationFolds: 5, " +
            "  randomSeed: 2," +
            "  params: [" +
            "    {penalty: 0.0625, maxIterations: 1000}, " +
            "    {penalty: 0.125, maxIterations: 1000}, " +
            "    {penalty: 0.25, maxIterations: 1000}, " +
            "    {penalty: 0.5, maxIterations: 1000}, " +
            "    {penalty: 1.0, maxIterations: 1000}, " +
            "    {penalty: 2.0, maxIterations: 1000}, " +
            "    {penalty: 4.0, maxIterations: 1000}" +
            "  ]" +
            "})",
            GRAPH_NAME,
            PAPER_LABEL.name(),
            SUBJECT_NODE_PROPERTY,
            EXT_ID_NODE_PROPERTY,
            CLASS_NODE_PROPERTY
        );
        runQuery(cora, trainOnN);

        var predictOnAll = formatWithLocale(
            "CALL gds.alpha.ml.nodeClassification.predict.mutate('%s', {" +
            "  nodeLabels: ['%s']," +
            "  modelName: 'model'," +
            "  mutateProperty: 'predicted_class', " +
            "  predictedProbabilityProperty: 'predicted_proba'" +
            "})",
            GRAPH_NAME,
            PAPER_LABEL.name()
        );
        runQuery(cora, predictOnAll);

        var predictedClasses = formatWithLocale(
            "CALL gds.graph.streamNodeProperties('%s', ['predicted_class', 'predicted_proba'])" +
            " YIELD nodeId, propertyValue" +
            " WITH nodeId, propertyValue" +
            " WITH nodeId, collect(propertyValue) AS values" +
            " RETURN nodeId, values[0] AS predictedClass, values[1] AS probabilities" +
            " LIMIT 10",
            GRAPH_NAME
        );

        runQueryWithResultConsumer(cora, predictedClasses, Map.of(), result -> System.out.println(result.resultAsString()));
    }
}
