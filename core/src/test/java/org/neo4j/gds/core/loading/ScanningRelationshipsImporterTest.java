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
package org.neo4j.gds.core.loading;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseTest;
import org.neo4j.gds.NodeProjections;
import org.neo4j.gds.PropertyMapping;
import org.neo4j.gds.PropertyMappings;
import org.neo4j.gds.RelationshipProjection;
import org.neo4j.gds.RelationshipProjections;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.AdjacencyList;
import org.neo4j.gds.api.AdjacencyProperties;
import org.neo4j.gds.api.GraphLoaderContext;
import org.neo4j.gds.api.ImmutableGraphLoaderContext;
import org.neo4j.gds.compat.GraphDatabaseApiProxy;
import org.neo4j.gds.config.GraphProjectFromStoreConfig;
import org.neo4j.gds.config.ImmutableGraphProjectFromStoreConfig;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.GraphDimensionsStoreReader;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.huge.DirectIdMap;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.warnings.EmptyUserLogRegistryFactory;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.transaction.TransactionContext;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.logging.NullLog;

import java.util.function.LongToIntFunction;

import static org.assertj.core.api.Assertions.assertThat;

class ScanningRelationshipsImporterTest extends BaseTest {

    @Neo4jGraph
    public static final String DB = "CREATE " +
                                    "(a)-[:R { p: 1.0 }]->(b)," +
                                    "(a)-[:R { p: 2.0 }]->(c)," +
                                    "(a)-[:R { p: 3.0 }]->(d)," +
                                    "(b)-[:R { p: 4.0 }]->(c)," +
                                    "(c)-[:R { p: 5.0 }]->(a)";

    @Inject
    private IdFunction idFunction;

    @Test
    void shouldLoadInverseRelationships() {
        var relationshipType = RelationshipType.of("R");
        var graphProjectConfig = ImmutableGraphProjectFromStoreConfig.builder()
            .graphName("testGraph")
            .nodeProjections(NodeProjections.ALL)
            .relationshipProjections(
                RelationshipProjections.single(
                    relationshipType,
                    RelationshipProjection.builder()
                        .type("R")
                        .indexInverse(true)
                        .properties(PropertyMappings.of(PropertyMapping.of("p")))
                        .build()
                ))
            .build();

        var graphLoaderContext = graphLoaderContext();
        var graphDimensions = graphDimensions(graphProjectConfig, graphLoaderContext);
        var importer = new ScanningRelationshipsImporterBuilder()
            .idMap(new DirectIdMap(graphDimensions.nodeCount()))
            .loadingContext(graphLoaderContext)
            .progressTracker(ProgressTracker.NULL_TRACKER)
            .dimensions(graphDimensions)
            .concurrency(1)
            .graphProjectConfig(graphProjectConfig)
            .build();

        var relationshipsAndProperties = importer.call();

        var singleTypeRelationshipImportResult = relationshipsAndProperties.importResults().get(relationshipType);
        assertThat(singleTypeRelationshipImportResult.inverseTopology()).isPresent();
        assertThat(singleTypeRelationshipImportResult.inverseProperties()).isPresent();

        var adjacencyList = singleTypeRelationshipImportResult.inverseTopology().get().adjacencyList();
        var propertyList = singleTypeRelationshipImportResult.inverseProperties().get()
            .relationshipProperties()
            .get("p")
            .values()
            .propertiesList();

        //@formatter:off
        assertThat(degree("a", adjacencyList)).isEqualTo(1); // (c)-->(a)
        assertThat(degree("b", adjacencyList)).isEqualTo(1); // (a)-->(b)
        assertThat(degree("c", adjacencyList)).isEqualTo(2); // (a)-->(c),(b)-->(c)
        assertThat(degree("d", adjacencyList)).isEqualTo(1); // (a)-->(d)

        assertThat(targets("a", adjacencyList)).isEqualTo(nodeIds("c"));         // (c)-->(a)
        assertThat(targets("b", adjacencyList)).isEqualTo(nodeIds("a"));         // (a)-->(b)
        assertThat(targets("c", adjacencyList)).isEqualTo(nodeIds("a", "b"));    // (a)-->(c),(b)-->(c)
        assertThat(targets("d", adjacencyList)).isEqualTo(nodeIds("a"));         // (a)-->(d)

        assertThat(properties("a", propertyList, adjacencyList::degree)).containsExactly(5.0);       // (c)-[5.0]->(a)
        assertThat(properties("b", propertyList, adjacencyList::degree)).containsExactly(1.0);       // (a)-[1.0]->(b)
        assertThat(properties("c", propertyList, adjacencyList::degree)).containsExactly(2.0, 4.0);  // (a)-[2.0]->(c),(b)-[4.0]->(c)
        assertThat(properties("d", propertyList, adjacencyList::degree)).containsExactly(3.0);       // (a)-[3.0->(d)
        //@formatter:on
    }

    private int degree(String nodeVariable, AdjacencyList adjacencyList) {
        var nodeId = idFunction.of(nodeVariable);
        return adjacencyList.degree(nodeId);
    }

    private long[] targets(String nodeVariable, AdjacencyList adjacencyList) {
        var nodeId = idFunction.of(nodeVariable);
        var degree = adjacencyList.degree(nodeId);
        var targets = new long[degree];
        var cursor = adjacencyList.adjacencyCursor(nodeId);
        int i = 0;
        while (cursor.hasNextVLong()) {
            targets[i++] = cursor.nextVLong();
        }
        return targets;
    }

    private double[] properties(
        String nodeVariable,
        AdjacencyProperties adjacencyProperties,
        LongToIntFunction degrees
    ) {
        var nodeId = idFunction.of(nodeVariable);
        var degree = degrees.applyAsInt(nodeId);
        var properties = new double[degree];
        var cursor = adjacencyProperties.propertyCursor(nodeId);
        int i = 0;
        while (cursor.hasNextLong()) {
            properties[i++] = Double.longBitsToDouble(cursor.nextLong());
        }
        return properties;
    }

    private long[] nodeIds(String... nodeVariables) {
        var nodeIds = new long[nodeVariables.length];
        for (int i = 0; i < nodeVariables.length; i++) {
            nodeIds[i] = idFunction.of(nodeVariables[i]);
        }
        return nodeIds;
    }

    private GraphLoaderContext graphLoaderContext() {
        return ImmutableGraphLoaderContext.builder()
            .graphDatabaseService(db)
            .executor(Pools.DEFAULT)
            .log(NullLog.getInstance())
            .terminationFlag(TerminationFlag.RUNNING_TRUE)
            .transactionContext(TransactionContext.of(db, db.beginTx()))
            .taskRegistryFactory(TaskRegistryFactory.empty())
            .userLogRegistryFactory(EmptyUserLogRegistryFactory.INSTANCE)
            .build();
    }

    private GraphDimensions graphDimensions(
        GraphProjectFromStoreConfig graphProjectConfig,
        GraphLoaderContext graphLoaderContext
    ) {
        return new GraphDimensionsStoreReader(
            graphLoaderContext.transactionContext(),
            graphProjectConfig,
            GraphDatabaseApiProxy.resolveDependency(graphLoaderContext.graphDatabaseService(), IdGeneratorFactory.class)
        ).call();
    }
}
