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
package org.neo4j.graphalgo.impl.similarity;

import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.loading.AdjacencyBuilder;
import org.neo4j.graphalgo.core.loading.IdMap;
import org.neo4j.graphalgo.core.loading.IdsAndProperties;
import org.neo4j.graphalgo.core.loading.ImportSizing;
import org.neo4j.graphalgo.core.loading.RelationshipImporter;
import org.neo4j.graphalgo.core.loading.Relationships;
import org.neo4j.graphalgo.core.loading.RelationshipsBatchBuffer;
import org.neo4j.graphalgo.core.loading.RelationshipsBuilder;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;

import java.util.Optional;
import java.util.concurrent.atomic.LongAdder;

public class HugeRelationshipsBuilder {
    private final IdMap idMap;
    private final RelationshipBuilder relationshipBuilder;

    public HugeRelationshipsBuilder(final IdsAndProperties nodes) {
        this.idMap = nodes.idMap();
        this.relationshipBuilder = new RelationshipBuilder(nodes.idMap());
    }

    Relationships build() {
        return relationshipBuilder.build();
    }

    public HugeRelationshipsBuilderWithBuffer withBuffer() {
        RelationshipsBatchBuffer relBuffer = new RelationshipsBatchBuffer(idMap, -1, 10_000);
        return new HugeRelationshipsBuilderWithBuffer(idMap, relationshipBuilder, relBuffer);
    }

    public static class HugeRelationshipsBuilderWithBuffer {
        private final RelationshipBuilder builder;
        private final RelationshipsBatchBuffer buffer;
        private final IdMap idMap;

        HugeRelationshipsBuilderWithBuffer(IdMap idMap, RelationshipBuilder builder, RelationshipsBatchBuffer buffer) {
            this.idMap = idMap;
            this.builder = builder;
            this.buffer = buffer;
        }

        public void addRelationship(long source, long target) {
            add(source, target);
        }

        void addRelationshipsFrom(AnnTopKConsumer[] topKHolder) {
            for (AnnTopKConsumer consumer : topKHolder) {
                consumer.stream().forEach(result -> {
                    long source = idMap.toMappedNodeId(result.item1);
                    long target = idMap.toMappedNodeId(result.item2);
                    if(source != -1 && target != -1 && source != target) {
                        addRelationship(source, target);
                    }
                });
            }
        }

        void flushAll() {
            builder.flushAll(buffer);
        }

        private void add(long source, long target) {
            buffer.add(source, target, -1L, -1L);

            if(buffer.isFull()) {
                builder.flushAll(buffer);
                buffer.reset();
            }
        }


        public Relationships build() {
            flushAll();
            return builder.build();
        }
    }

    static class RelationshipBuilder {
        private final RelationshipImporter relationshipImporter;
        private final RelationshipImporter.Imports imports;
        private final RelationshipsBuilder outRelationshipsBuilder;
        private final RelationshipsBuilder inRelationshipsBuilder;

        RelationshipBuilder(IdMapping idMap) {
            ImportSizing importSizing = ImportSizing.of(1, idMap.nodeCount());
            int pageSize = importSizing.pageSize();
            int numberOfPages = importSizing.numberOfPages();

            outRelationshipsBuilder = createRelationshipsBuilder();
            AdjacencyBuilder outAdjacencyBuilder = createAdjacencyBuilder(
                    pageSize,
                    numberOfPages,
                    outRelationshipsBuilder
            );

            inRelationshipsBuilder = createRelationshipsBuilder();
            AdjacencyBuilder inAdjacencyBuilder = createAdjacencyBuilder(
                    pageSize,
                    numberOfPages,
                    inRelationshipsBuilder
            );

            relationshipImporter = new RelationshipImporter(AllocationTracker.EMPTY, outAdjacencyBuilder, inAdjacencyBuilder);
            imports = relationshipImporter.imports(false, true, true, false);
        }

        Relationships build() {
            ParallelUtil.run(relationshipImporter.flushTasks(), null);
            return new Relationships(
                    -1,
                    -1,
                    inRelationshipsBuilder.adjacencyList(),
                    outRelationshipsBuilder.adjacencyList(),
                    inRelationshipsBuilder.globalAdjacencyOffsets(),
                    outRelationshipsBuilder.globalAdjacencyOffsets(),
                    Optional.empty(),
                    inRelationshipsBuilder.properties(),
                    outRelationshipsBuilder.properties(),
                    inRelationshipsBuilder.globalPropertyOffsets(),
                    outRelationshipsBuilder.globalPropertyOffsets()
            );
        }
        static AdjacencyBuilder createAdjacencyBuilder(
                int pageSize,
                int numberOfPages,
                RelationshipsBuilder relationshipsBuilder) {
            return AdjacencyBuilder.compressing(
                    relationshipsBuilder,
                    numberOfPages, pageSize,
                    AllocationTracker.EMPTY, new LongAdder(), new int[] {-2}, new double[] { -1});
        }

        static RelationshipsBuilder createRelationshipsBuilder() {
            return new RelationshipsBuilder(
                    new Aggregation[]{Aggregation.NONE},
                    AllocationTracker.EMPTY,
                    0);
        }

        static RelationshipImporter.PropertyReader weightReader() {
            return RelationshipImporter.preLoadedPropertyReader();
        }

        void flushAll(RelationshipsBatchBuffer relBuffer ) {
            imports.importRels(relBuffer, weightReader());
        }

    }
}
