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

import com.carrotsearch.hppc.LongObjectHashMap;
import com.carrotsearch.hppc.LongObjectMap;
import org.immutables.value.Value;
import org.neo4j.graphalgo.ElementIdentifier;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.ImmutableGraphDimensions;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArrayBuilder;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.neo4j.graphalgo.compat.StatementConstantsProxy.NO_SUCH_PROPERTY_KEY;

@Value.Enclosing
class CypherNodeLoader extends CypherRecordLoader<CypherNodeLoader.LoadResult> {

    static final int CYPHER_RESULT_PROPERTY_KEY = -2;

    private final long nodeCount;
    private final GraphDimensions outerDimensions;
    private final LongObjectMap<List<ElementIdentifier>> labelElementIdentifierMapping;

    private final HugeLongArrayBuilder builder;
    private final NodeImporter importer;
    private long maxNodeId;
    private CypherNodePropertyImporter nodePropertyImporter;

    CypherNodeLoader(
        String nodeQuery,
        long nodeCount,
        GraphDatabaseAPI api,
        GraphCreateConfig config,
        GraphSetup setup,
        GraphDimensions outerDimensions
    ) {
        super(nodeQuery, nodeCount, api, config, setup);
        this.nodeCount = nodeCount;
        this.outerDimensions = outerDimensions;
        this.maxNodeId = 0L;
        this.labelElementIdentifierMapping = new LongObjectHashMap<>();
        this.builder = HugeLongArrayBuilder.of(nodeCount, setup.tracker());
        this.importer = new NodeImporter(builder, new HashMap<>(), labelElementIdentifierMapping);
    }

    @Override
    BatchLoadResult loadSingleBatch(Transaction tx, int bufferSize) {
        Result queryResult = runLoadingQuery(tx);

        Collection<String> propertyColumns = getPropertyColumns(queryResult);

        nodePropertyImporter = new CypherNodePropertyImporter(
                propertyColumns,
                labelElementIdentifierMapping,
                nodeCount,
                config.readConcurrency()
        );

        boolean hasLabelInformation = queryResult.columns().contains(NodeRowVisitor.LABELS_COLUMN);

        NodesBatchBuffer buffer = new NodesBatchBufferBuilder()
            .capacity(bufferSize)
            .hasLabelInformation(hasLabelInformation)
            .readProperty(!propertyColumns.isEmpty())
            .build();

        NodeRowVisitor visitor = new NodeRowVisitor(
            buffer,
            importer,
            hasLabelInformation,
            nodePropertyImporter
        );

        queryResult.accept(visitor);
        visitor.flush();
        return new BatchLoadResult(visitor.rows(), visitor.maxId());
    }

    @Override
    void updateCounts(BatchLoadResult result) {
        if (result.maxId() > maxNodeId) {
            maxNodeId = result.maxId();
        }
    }

    @Override
    LoadResult result() {
        IdMap idMap = IdMapBuilder.build(
            builder,
            importer.elementIdentifierBitSetMapping,
            maxNodeId,
            setup.concurrency(),
            setup.tracker()
        );
        Map<ElementIdentifier, Map<PropertyMapping, NodeProperties>> nodeProperties = nodePropertyImporter.result();

        Map<String, Integer> propertyIds = nodeProperties
            .values()
            .stream()
            .flatMap(properties -> properties.keySet().stream())
            .collect(Collectors.toMap(PropertyMapping::propertyKey, (ignore) -> NO_SUCH_PROPERTY_KEY));

        GraphDimensions resultDimensions = ImmutableGraphDimensions.builder()
            .from(outerDimensions)
            .nodePropertyTokens(propertyIds)
            .build();

        return ImmutableCypherNodeLoader.LoadResult.builder()
            .dimensions(resultDimensions)
            .idsAndProperties(IdsAndProperties.of(idMap, nodeProperties))
            .build();
    }

    @Override
    Set<String> getMandatoryColumns() {
        return NodeRowVisitor.REQUIRED_COLUMNS;
    }

    @Override
    Set<String> getReservedColumns() {
        return NodeRowVisitor.RESERVED_COLUMNS;
    }

    @Override
    QueryType queryType() {
        return QueryType.NODE;
    }

    @ValueClass
    interface LoadResult {
        GraphDimensions dimensions();

        IdsAndProperties idsAndProperties();
    }
}
