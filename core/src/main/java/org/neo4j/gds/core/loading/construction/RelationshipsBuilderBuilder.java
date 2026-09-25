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
package org.neo4j.gds.core.loading.construction;

import org.neo4j.gds.Aggregation;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.PartialIdMap;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.loading.RecordsBatchBuffer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;

public final class RelationshipsBuilderBuilder {
    private PartialIdMap nodes;
    private RelationshipType relationshipType;
    private Orientation orientation = Orientation.NATURAL;
    private List<GraphFactory.PropertyConfig> propertyConfigs = new ArrayList<>();
    private Aggregation globalAggregation = Aggregation.DEFAULT;
    private boolean skipDanglingRelationships = true;
    private Concurrency concurrency = new Concurrency(1);
    private boolean indexInverse = false;
    private ExecutorService executorService = DefaultPool.INSTANCE;
    private boolean usePooledBuilderProvider = false;
    private int bufferSize = RecordsBatchBuffer.DEFAULT_BUFFER_SIZE;

    RelationshipsBuilderBuilder() {}

    public RelationshipsBuilderBuilder nodes(PartialIdMap nodes) {
        this.nodes = Objects.requireNonNull(nodes);
        return this;
    }

    public RelationshipsBuilderBuilder relationshipType(RelationshipType relationshipType) {
        this.relationshipType = Objects.requireNonNull(relationshipType);
        return this;
    }

    /**
     * @param orientation Default is {@code Orientation.NATURAL}.
     */
    public RelationshipsBuilderBuilder orientation(Orientation orientation) {
        this.orientation = Objects.requireNonNull(orientation);
        return this;
    }

    /**
     * @param propertyConfigs Default is an empty list.
     */
    public RelationshipsBuilderBuilder propertyConfigs(List<? extends GraphFactory.PropertyConfig> propertyConfigs) {
        this.propertyConfigs = Collections.unmodifiableList(propertyConfigs);
        return this;
    }

    /**
     * Global aggregation for all properties.<p>
     * To configure aggregation per property, use {@code propertyConfigs()}.
     * Global aggregation is only effective if {@code propertyConfigs} is empty.<p>
     * To set a global aggregation that conflicts with {@code propertyConfigs} is an error.
     *
     * @param aggregation Default is {@code Aggregation.NONE}.
     */
    public RelationshipsBuilderBuilder globalAggregation(Aggregation aggregation) {
        this.globalAggregation = Objects.requireNonNull(aggregation);
        return this;
    }

    /**
     * @param skipDanglingRelationships Default is {@code true}.
     */
    public RelationshipsBuilderBuilder skipDanglingRelationships(boolean skipDanglingRelationships) {
        this.skipDanglingRelationships = skipDanglingRelationships;
        return this;
    }

    /**
     * @param concurrency Default is {@code new Concurrency(1)}.
     */
    public RelationshipsBuilderBuilder concurrency(Concurrency concurrency) {
        this.concurrency = Objects.requireNonNull(concurrency);
        return this;
    }

    /**
     * @param indexInverse Default is {@code false}.
     */
    public RelationshipsBuilderBuilder indexInverse(boolean indexInverse) {
        this.indexInverse = indexInverse;
        return this;
    }

    /**
     * @param executorService Default is {@code efaultPool.INSTANCE}.
     */
    public RelationshipsBuilderBuilder executorService(ExecutorService executorService) {
        this.executorService = Objects.requireNonNull(executorService);
        return this;
    }

    /**
     * @param usePooledBuilderProvider Default is {@code false}.
     */
    public RelationshipsBuilderBuilder usePooledBuilderProvider(boolean usePooledBuilderProvider) {
        this.usePooledBuilderProvider = usePooledBuilderProvider;
        return this;
    }

    /**
     * @param bufferSize Default is {@code RecordsBatchBuffer.DEFAULT_BUFFER_SIZE}.
     */
    public RelationshipsBuilderBuilder bufferSize(int bufferSize) {
        if (bufferSize <= 0) {
            throw new IllegalArgumentException("bufferSize must be positive, got " + bufferSize);
        }
        this.bufferSize = bufferSize;
        return this;
    }

    public RelationshipsBuilder build() {
        Objects.requireNonNull(nodes, "nodes must be set");
        Objects.requireNonNull(relationshipType, "relationshipType must be set");
        if (globalAggregation != Aggregation.DEFAULT) {
            var conflictingAggregations = propertyConfigs.stream().filter(it -> it.aggregation() != globalAggregation).toList();
            if (!conflictingAggregations.isEmpty()) {
                var message = """
                    Both aggregation and propertyConfigs are set, but with conflicting aggregations:
                      PropertyConfigs: %s
                      Aggregation: %s"""
                    .formatted(conflictingAggregations, globalAggregation);
                throw new IllegalArgumentException(message);
            }
        }
        return GraphFactory.relationshipsBuilder(
            nodes,
            relationshipType,
            orientation,
            List.copyOf(propertyConfigs),
            globalAggregation,
            skipDanglingRelationships,
            concurrency,
            indexInverse,
            executorService,
            usePooledBuilderProvider,
            bufferSize
        );
    }
}
