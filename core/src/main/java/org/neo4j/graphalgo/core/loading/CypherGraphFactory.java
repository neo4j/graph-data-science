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
package org.neo4j.graphalgo.core.loading;

import com.carrotsearch.hppc.ObjectLongMap;
import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.RelationshipTypeMapping;
import org.neo4j.graphalgo.RelationshipTypeMappings;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.api.MultipleRelTypesSupport;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.huge.AdjacencyList;
import org.neo4j.graphalgo.core.huge.AdjacencyOffsets;
import org.neo4j.graphalgo.core.huge.HugeGraph;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.KernelTransaction.Revertable;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.neo4j.internal.kernel.api.security.AccessMode.Static.READ;

public class CypherGraphFactory extends GraphFactory implements MultipleRelTypesSupport {

    public static final String TYPE = "cypher";

    static final String LIMIT = "limit";
    static final String SKIP = "skip";

    private final GraphDatabaseAPI api;
    private final GraphSetup setup;

    public CypherGraphFactory(GraphDatabaseAPI api, GraphSetup setup) {
        super(api, setup, false);
        this.api = api;
        this.setup = setup;
    }

    @Override
    protected void validateTokens() { }

    public final MemoryEstimation memoryEstimation() {
        BatchLoadResult nodeCount = new CountingCypherRecordLoader(setup.nodeLabel(), api, setup).load();
        dimensions.nodeCount(nodeCount.rows());

        BatchLoadResult relCount = new CountingCypherRecordLoader(setup.relationshipType(), api, setup).load();
        dimensions.maxRelCount(relCount.rows());

        return HugeGraphFactory.getMemoryEstimation(setup, dimensions);
    }

    @Override
    public Graph importGraph() {
        RelationshipTypeMappings relationshipTypeIds = dimensions.relationshipTypeMappings();
        if (relationshipTypeIds.isMultipleTypes()) {
            String message = String.format(
                "It is not possible to use multiple relationship types in implicit graph loading. Please use `algo.graph.load()` for this. Found relationship types: %s",
                relationshipTypeIds
                    .stream()
                    .map(RelationshipTypeMapping::typeName)
                    .collect(Collectors.toList()));
            throw new IllegalArgumentException(message);
        }

        return importAllGraphs().getUnion();
    }

    @Override
    public GraphsByRelationshipType importAllGraphs() {
        // Temporarily override the security context to enforce read-only access during load
        try (Revertable revertable = setReadOnlySecurityContext()) {
            BatchLoadResult nodeCount = new CountingCypherRecordLoader(setup.nodeLabel(), api, setup).load();
            IdsAndProperties nodes = new CypherNodeLoader(nodeCount.rows(), api, setup).load();
            Map<String, Map<String, Graph>> graphs = loadRelationships(nodes, this.dimensions, setup.tracker());
            progressLogger.logDone(setup.tracker());
            return GraphsByRelationshipType.of(graphs);
        }
    }

    private Map<String, Map<String, Graph>> loadRelationships(
        IdsAndProperties idsAndProperties,
        GraphDimensions dimensions,
        AllocationTracker tracker
    ) {
        CypherRelationshipLoader relationshipLoader = new CypherRelationshipLoader(
            idsAndProperties.idMap(),
            api,
            setup,
            dimensions
        );

        Pair<GraphDimensions, ObjectLongMap<RelationshipTypeMapping>> result = relationshipLoader.load();

        GraphDimensions resultDimensions = result.getLeft();

        return relationshipLoader.allBuilders().entrySet().stream().collect(Collectors.toMap(
            entry -> entry.getKey().typeName(),
            entry -> {
                RelationshipsBuilder outgoingRelationshipsBuilder = entry.getValue();

                AdjacencyList outAdjacencyList = outgoingRelationshipsBuilder != null
                    ? outgoingRelationshipsBuilder.adjacency.build() : null;
                AdjacencyOffsets outAdjacencyOffsets = outgoingRelationshipsBuilder != null
                    ? outgoingRelationshipsBuilder.globalAdjacencyOffsets : null;

                long relationshipCount = result.getRight().getOrDefault(entry.getKey(), 0L);

                if (!resultDimensions.relProperties().hasMappings()) {
                    HugeGraph graph = HugeGraph.create(
                        tracker,
                        idsAndProperties.hugeIdMap,
                        idsAndProperties.properties,
                        outAdjacencyList,
                        outAdjacencyOffsets,
                        null,
                        null,
                        relationshipCount,
                        setup.loadAsUndirected()
                    );
                    return Collections.singletonMap("", graph);
                } else {
                    return resultDimensions.relProperties().enumerate().map(propertyEntry -> {
                        int weightIndex = propertyEntry.getKey();
                        PropertyMapping property = propertyEntry.getValue();
                        HugeGraph graph = create(
                            tracker,
                            idsAndProperties.hugeIdMap,
                            idsAndProperties.properties,
                            outgoingRelationshipsBuilder,
                            outAdjacencyList,
                            outAdjacencyOffsets,
                            weightIndex,
                            property,
                            relationshipCount,
                            setup.loadAsUndirected()
                        );
                        return Pair.of(property.propertyKey(), graph);
                    }).collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
                }
            }
        ));
    }

    private Revertable setReadOnlySecurityContext() {
        try {
            KernelTransaction kernelTransaction = api
                    .getDependencyResolver()
                    .resolveDependency(ThreadToStatementContextBridge.class)
                    .getKernelTransactionBoundToThisThread(true);
            AuthSubject subject = kernelTransaction.securityContext().subject();
            SecurityContext securityContext = new SecurityContext(subject, READ);
            return kernelTransaction.overrideWith(securityContext);
        } catch (NotInTransactionException ex) {
            // happens only in tests
            throw new IllegalStateException("Must run in a transaction.", ex);
        }
    }

    private HugeGraph create(
        AllocationTracker tracker,
        IdMap idMapping,
        Map<String, NodeProperties> nodeProperties,
        RelationshipsBuilder relationshipsBuilder,
        AdjacencyList adjacencyList,
        AdjacencyOffsets adjacencyOffsets,
        int weightIndex,
        PropertyMapping weightProperty,
        long relationshipCount,
        boolean loadAsUndirected) {

        AdjacencyList outWeightList = null;
        AdjacencyOffsets outWeightOffsets = null;
        if (relationshipsBuilder != null) {
            if (weightProperty.propertyKeyId() != StatementConstants.NO_SUCH_PROPERTY_KEY) {
                outWeightOffsets = relationshipsBuilder.globalWeightOffsets[weightIndex];
                if (outWeightOffsets != null) {
                    outWeightList = relationshipsBuilder.weights[weightIndex].build();
                }
            }
        }

        Optional<Double> maybeDefaultWeight = weightProperty == PropertyMapping.EMPTY_PROPERTY
            ? Optional.empty()
            : Optional.of(weightProperty.defaultValue());

        return HugeGraph.create(
            tracker,
            idMapping,
            nodeProperties,
            relationshipCount,
            null,
            adjacencyList,
            null,
            adjacencyOffsets,
            maybeDefaultWeight,
            Optional.empty(),
            Optional.ofNullable(outWeightList),
            Optional.empty(),
            Optional.ofNullable(outWeightOffsets),
            loadAsUndirected
        );
    }
}
