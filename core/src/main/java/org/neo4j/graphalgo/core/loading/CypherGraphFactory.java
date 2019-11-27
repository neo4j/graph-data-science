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
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.api.MultipleRelTypesSupport;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.core.DeduplicationStrategy;
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
import java.util.function.Function;
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
        // Temporarily override the security context to enforce read-only access during load
        try (Revertable revertable = setReadOnlySecurityContext()) {
            BatchLoadResult nodeCount = new CountingCypherRecordLoader(setup.nodeLabel(), api, setup).load();
            IdsAndProperties nodes = new CypherNodeLoader(nodeCount.rows(), api, setup).load();
            Relationships relationships = new CypherRelationshipLoader(nodes.idMap(), api, setup).load();

            return HugeGraph.create(
                setup.tracker(),
                    nodes.idMap(),
                    nodes.properties(),
                    relationships.relationshipCount(),
                    relationships.inAdjacency(),
                    relationships.outAdjacency(),
                    relationships.inOffsets(),
                    relationships.outOffsets(),
                    relationships.maybeDefaultRelProperty(),
                    Optional.ofNullable(relationships.inRelProperties()),
                    Optional.ofNullable(relationships.outRelProperties()),
                    Optional.ofNullable(relationships.inRelPropertyOffsets()),
                    Optional.ofNullable(relationships.outRelPropertyOffsets()),
                setup.loadAsUndirected()
            );
        }
    }

    @Override
    public GraphsByRelationshipType importAllGraphs() {
        try (Revertable revertable = setReadOnlySecurityContext()) {
            BatchLoadResult nodeCount = new CountingCypherRecordLoader(setup.startLabel, api, setup).load();
            IdsAndProperties nodes = new CypherNodeLoader(nodeCount.rows(), api, setup).load();
            Map<String, Map<String, Graph>> graphs = loadRelationships(this.dimensions, setup.tracker, nodes, setup.concurrency());
            progressLogger.logDone(setup.tracker);
            return GraphsByRelationshipType.of(graphs);
        }
    }

    private Map<String, Map<String, Graph>> loadRelationships(
        GraphDimensions dimensions,
        AllocationTracker tracker,
        IdsAndProperties idsAndProperties,
        int concurrency) {

        // TODO: create a type for that beast
        Map<RelationshipTypeMapping, Pair<RelationshipsBuilder, RelationshipsBuilder>> allBuilders = dimensions
            .relationshipTypeMappings()
            .stream()
            .collect(Collectors.toMap(
                Function.identity(),
                mapping -> createBuilderForRelationshipType(tracker)
            ));

        CypherMultiRelationshipLoader cypherMultiRelationshipLoader = new CypherMultiRelationshipLoader(
            idsAndProperties.idMap(),
            allBuilders,
            api,
            setup,
            dimensions
        );

        cypherMultiRelationshipLoader.load();

        ScanningRelationshipsImporter scanningImporter = new ScanningRelationshipsImporter(
            setup,
            api,
            dimensions,
            progress,
            tracker,
            idsAndProperties.hugeIdMap,
            allBuilders,
            threadPool,
            concurrency
        );
        ObjectLongMap<RelationshipTypeMapping> relationshipCounts = scanningImporter.call(setup.log);

        return allBuilders.entrySet().stream().collect(Collectors.toMap(
            entry -> entry.getKey().typeName(),
            entry -> {
                Pair<RelationshipsBuilder, RelationshipsBuilder> builders = entry.getValue();
                RelationshipsBuilder outgoingRelationshipsBuilder = builders.getLeft();
                RelationshipsBuilder incomingRelationshipsBuilder = builders.getRight();

                AdjacencyList outAdjacencyList = outgoingRelationshipsBuilder != null
                    ? outgoingRelationshipsBuilder.adjacency.build() : null;
                AdjacencyOffsets outAdjacencyOffsets = outgoingRelationshipsBuilder != null
                    ? outgoingRelationshipsBuilder.globalAdjacencyOffsets : null;

                AdjacencyList inAdjacencyList = incomingRelationshipsBuilder != null
                    ? incomingRelationshipsBuilder.adjacency.build() : null;
                AdjacencyOffsets inAdjacencyOffsets = incomingRelationshipsBuilder != null
                    ? incomingRelationshipsBuilder.globalAdjacencyOffsets : null;

                long relationshipCount = relationshipCounts.getOrDefault(entry.getKey(), 0L);

                if (!dimensions.relProperties().hasMappings()) {
                    HugeGraph graph = buildGraph(
                        tracker,
                        idsAndProperties.hugeIdMap,
                        idsAndProperties.properties,
                        outAdjacencyList,
                        outAdjacencyOffsets,
                        inAdjacencyList,
                        inAdjacencyOffsets,
                        relationshipCount,
                        setup.loadAsUndirected
                    );
                    return Collections.singletonMap("", graph);
                } else {
                    return dimensions.relProperties().enumerate().map(propertyEntry -> {
                        int weightIndex = propertyEntry.getKey();
                        PropertyMapping property = propertyEntry.getValue();
                        HugeGraph graph = buildGraphWithRelationshipProperty(
                            tracker,
                            idsAndProperties.hugeIdMap,
                            idsAndProperties.properties,
                            incomingRelationshipsBuilder,
                            outgoingRelationshipsBuilder,
                            outAdjacencyList,
                            outAdjacencyOffsets,
                            inAdjacencyList,
                            inAdjacencyOffsets,
                            weightIndex,
                            property,
                            relationshipCount,
                            setup.loadAsUndirected
                        );
                        return Pair.of(property.propertyKey(), graph);
                    }).collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
                }
            }));
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

    // TODO: this could probably live on abstract GraphFactory
    private Pair<RelationshipsBuilder, RelationshipsBuilder> createBuilderForRelationshipType(AllocationTracker tracker) {
        RelationshipsBuilder outgoingRelationshipsBuilder = null;
        RelationshipsBuilder incomingRelationshipsBuilder = null;

        DeduplicationStrategy[] deduplicationStrategies = dimensions
            .relProperties()
            .stream()
            .map(property -> property.deduplicationStrategy() == DeduplicationStrategy.DEFAULT
                // TODO: was skip before, changed it for Cypher
                ? DeduplicationStrategy.NONE
                : property.deduplicationStrategy()
            )
            .toArray(DeduplicationStrategy[]::new);
        // TODO: backwards compat code
        if (deduplicationStrategies.length == 0) {
            DeduplicationStrategy deduplicationStrategy =
                setup.deduplicationStrategy == DeduplicationStrategy.DEFAULT
                    // TODO: was skip before, changed it for Cypher
                    ? DeduplicationStrategy.NONE
                    : setup.deduplicationStrategy;
            deduplicationStrategies = new DeduplicationStrategy[]{deduplicationStrategy};
        }

        if (setup.loadAsUndirected) {
            outgoingRelationshipsBuilder = new RelationshipsBuilder(
                deduplicationStrategies,
                tracker,
                setup.relationshipPropertyMappings.numberOfMappings());
        } else {
            if (setup.loadOutgoing) {
                outgoingRelationshipsBuilder = new RelationshipsBuilder(
                    deduplicationStrategies,
                    tracker,
                    setup.relationshipPropertyMappings.numberOfMappings());
            }
            if (setup.loadIncoming) {
                incomingRelationshipsBuilder = new RelationshipsBuilder(
                    deduplicationStrategies,
                    tracker,
                    setup.relationshipPropertyMappings.numberOfMappings());
            }
        }

        return Pair.of(outgoingRelationshipsBuilder, incomingRelationshipsBuilder);
    }

    // TODO: move to factory methods on HugeGraph and remove here and in HugeGraphFactory
    private HugeGraph buildGraph(
        AllocationTracker tracker,
        IdMap idMapping,
        Map<String, NodeProperties> nodeProperties,
        AdjacencyList outAdjacencyList,
        AdjacencyOffsets outAdjacencyOffsets,
        AdjacencyList inAdjacencyList,
        AdjacencyOffsets inAdjacencyOffsets,
        long relationshipCount,
        boolean loadAsUndirected) {

        return HugeGraph.create(
            tracker,
            idMapping,
            nodeProperties,
            relationshipCount,
            inAdjacencyList,
            outAdjacencyList,
            inAdjacencyOffsets,
            outAdjacencyOffsets,
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            loadAsUndirected);
    }

    // TODO: move to factory methods on HugeGraph and remove here and in HugeGraphFactory
    private HugeGraph buildGraphWithRelationshipProperty(
        AllocationTracker tracker,
        IdMap idMapping,
        Map<String, NodeProperties> nodeProperties,
        RelationshipsBuilder inRelationshipsBuilder,
        RelationshipsBuilder outRelationshipsBuilder,
        AdjacencyList outAdjacencyList,
        AdjacencyOffsets outAdjacencyOffsets,
        AdjacencyList inAdjacencyList,
        AdjacencyOffsets inAdjacencyOffsets,
        int weightIndex,
        PropertyMapping weightProperty,
        long relationshipCount,
        boolean loadAsUndirected) {

        AdjacencyList outWeightList = null;
        AdjacencyOffsets outWeightOffsets = null;
        if (outRelationshipsBuilder != null) {
            if (weightProperty.propertyKeyId() != StatementConstants.NO_SUCH_PROPERTY_KEY) {
                outWeightOffsets = outRelationshipsBuilder.globalWeightOffsets[weightIndex];
                if (outWeightOffsets != null) {
                    outWeightList = outRelationshipsBuilder.weights[weightIndex].build();
                }
            }
        }

        AdjacencyList inWeightList = null;
        AdjacencyOffsets inWeightOffsets = null;
        if (inRelationshipsBuilder != null) {
            if (weightProperty.propertyKeyId() != StatementConstants.NO_SUCH_PROPERTY_KEY) {
                inWeightOffsets = inRelationshipsBuilder.globalWeightOffsets[weightIndex];
                if (inWeightOffsets != null) {
                    inWeightList = inRelationshipsBuilder.weights[weightIndex].build();
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
            inAdjacencyList,
            outAdjacencyList,
            inAdjacencyOffsets,
            outAdjacencyOffsets,
            maybeDefaultWeight,
            Optional.ofNullable(inWeightList),
            Optional.ofNullable(outWeightList),
            Optional.ofNullable(inWeightOffsets),
            Optional.ofNullable(outWeightOffsets),
            loadAsUndirected);
    }
}
