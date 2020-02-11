/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.tuple.Tuples;
import org.neo4j.graphalgo.Projection;
import org.neo4j.graphalgo.RelationshipProjectionMapping;
import org.neo4j.graphalgo.ResolvedPropertyMapping;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.ImmutableGraphDimensions;
import org.neo4j.graphalgo.core.huge.AdjacencyList;
import org.neo4j.graphalgo.core.huge.AdjacencyOffsets;
import org.neo4j.graphalgo.core.huge.HugeGraph;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

import static org.neo4j.graphalgo.core.loading.CypherRecordLoader.QueryType.NODE;
import static org.neo4j.graphalgo.core.loading.CypherRecordLoader.QueryType.RELATIONSHIP;
import static org.neo4j.internal.kernel.api.security.AccessMode.Static.READ;

public class CypherGraphFactory extends GraphFactory {

    public static final String TYPE = "cypher";

    static final String LIMIT = "limit";
    static final String SKIP = "skip";

    private final GraphDatabaseAPI api;
    private final GraphSetup setup;
    private final KernelTransaction kernelTransaction;

    public CypherGraphFactory(GraphDatabaseAPI api, GraphSetup setup, KernelTransaction kernelTransaction) {
        super(api, setup, false);
        this.api = api;
        this.setup = setup;
        this.kernelTransaction = kernelTransaction;
    }

    public final MemoryEstimation memoryEstimation() {
        BatchLoadResult nodeCount = new CountingCypherRecordLoader(nodeQuery(), NODE, api, setup).load();
        BatchLoadResult relCount = new CountingCypherRecordLoader(relationshipQuery(), RELATIONSHIP, api, setup).load();

        GraphDimensions estimateDimensions = ImmutableGraphDimensions.builder()
            .from(dimensions)
            .nodeCount(nodeCount.rows())
            .maxRelCount(relCount.rows())
            .build();

        return HugeGraphFactory.getMemoryEstimation(estimateDimensions);
    }

    @Override
    public MemoryEstimation memoryEstimation(GraphDimensions dimensions) {
        return HugeGraphFactory.getMemoryEstimation(dimensions);
    }

    @Override
    public ImportResult build() {
//         Temporarily override the security context to enforce read-only access during load
        try (KernelTransaction.Revertable ignored = setReadOnlySecurityContext()) {
            BatchLoadResult nodeCount = new CountingCypherRecordLoader(nodeQuery(), NODE, api, setup).load();

            CypherNodeLoader.LoadResult loadResult = new CypherNodeLoader(
                nodeQuery(),
                nodeCount.rows(),
                api,
                setup,
                dimensions
            ).load();

            ImportResult importResult = loadRelationships(
                relationshipQuery(),
                loadResult.idsAndProperties(),
                loadResult.dimensions()
            );
            progressLogger.logDone(setup.tracker());

            return importResult;
        }
    }

    private String nodeQuery() {
        return setup.nodeQuery().orElseThrow(() -> new IllegalArgumentException("Missing node query"));
    }

    private String relationshipQuery() {
        return setup.relationshipQuery().orElseThrow(() -> new IllegalArgumentException("Missing relationship query"));
    }

    private ImportResult loadRelationships(
        String relationshipQuery,
        IdsAndProperties idsAndProperties,
        GraphDimensions nodeLoadDimensions
    ) {
        CypherRelationshipLoader relationshipLoader = new CypherRelationshipLoader(
            relationshipQuery,
            idsAndProperties.idMap(),
            api,
            setup,
            nodeLoadDimensions
        );

        CypherRelationshipLoader.LoadResult result = relationshipLoader.load();

        GraphDimensions resultDimensions = result.dimensions();
        ObjectLongMap<RelationshipProjectionMapping> relationshipCounts = result.relationshipCounts();

        Map<String, Map<String, Graph>> graphs = relationshipLoader
            .allBuilders()
            .entrySet()
            .stream()
            .collect(Collectors.toMap(
                entry -> entry.getKey().typeName(),
                entry -> {

                    RelationshipProjectionMapping relationshipProjectionMapping = entry.getKey();
                    RelationshipsBuilder relationshipsBuilder = entry.getValue();

                    if (relationshipsBuilder == null) {
                        throw new IllegalStateException(
                            String.format(
                                "RelationshipsBuilder must not be `null` for relationship type `%s`.",
                                relationshipProjectionMapping.typeName()
                            )
                        );
                    }

                    AdjacencyList adjacencyList = relationshipsBuilder.adjacencyListBuilder.build();
                    AdjacencyOffsets adjacencyOffsets = relationshipsBuilder.globalAdjacencyOffsets;
                    boolean loadAsUndirected = relationshipProjectionMapping.projection() == Projection.UNDIRECTED;

                    long relationshipCount = relationshipCounts.getOrDefault(relationshipProjectionMapping, 0L);

                    if (!resultDimensions.relationshipProperties().hasMappings()) {
                        HugeGraph graph = HugeGraph.create(
                            setup.tracker(),
                            idsAndProperties.hugeIdMap,
                            idsAndProperties.properties,
                            adjacencyList,
                            adjacencyOffsets,
                            relationshipCount,
                            loadAsUndirected
                        );
                        return Collections.singletonMap(ANY_REL_TYPE, graph);
                    } else {
                        return resultDimensions.relationshipProperties().enumerate().map(propertyEntry -> {
                            int propertyKeyId = propertyEntry.getOne();
                            ResolvedPropertyMapping propertyMapping = propertyEntry.getTwo();
                            HugeGraph graph = HugeGraph.create(
                                setup.tracker(),
                                idsAndProperties.hugeIdMap,
                                idsAndProperties.properties,
                                relationshipsBuilder,
                                adjacencyList,
                                adjacencyOffsets,
                                propertyKeyId,
                                propertyMapping,
                                relationshipCount,
                                loadAsUndirected
                            );
                            return Tuples.pair(propertyMapping.propertyKey(), graph);
                        }).collect(Collectors.toMap(Pair::getOne, Pair::getTwo));
                    }
                }
            ));

        return ImportResult.of(resultDimensions, GraphsByRelationshipType.of(graphs));
    }

    private KernelTransaction.Revertable setReadOnlySecurityContext() {
        try {
            AuthSubject subject = kernelTransaction.securityContext().subject();
            SecurityContext securityContext = new SecurityContext(subject, READ);
            return kernelTransaction.overrideWith(securityContext);
        } catch (NotInTransactionException ex) {
            // happens only in tests
            throw new IllegalStateException("Must run in a transaction.", ex);
        }
    }
}
