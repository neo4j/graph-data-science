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
package org.neo4j.graphalgo.wcc;

import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.BitUtil;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphalgo.core.utils.paged.HugeLongLongMap;
import org.neo4j.graphalgo.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.graphalgo.core.write.PropertyTranslator;
import org.neo4j.graphalgo.result.AbstractCommunityResultBuilder;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;

public abstract class WccBaseProc<CONFIG extends WccBaseConfig> extends AlgoBaseProc<Wcc, DisjointSetStruct, CONFIG> {

    static final String WCC_DESCRIPTION =
        "The WCC algorithm finds sets of connected nodes in an undirected graph, where all nodes in the same set form a connected component.";

    @Override
    protected final WccFactory<CONFIG> algorithmFactory(WccBaseConfig config) {
        return new WccFactory<>();
    }

    @Override
    protected boolean legacyMode() {
        return false;
    }

    protected Stream<WriteResult> write(
        ComputationResult<Wcc, DisjointSetStruct, CONFIG> computeResult
    ) {
        CONFIG config = computeResult.config();
        boolean write = config instanceof WccWriteConfig;
        WccWriteConfig writeConfig = ImmutableWccWriteConfig.builder()
            .writeProperty("stats does not support a write property")
            .from(config)
            .build();

        if (computeResult.isGraphEmpty()) {
            return Stream.of(WccBaseProc.WriteResult.empty(writeConfig, computeResult.createMillis()));
        } else {
            Graph graph = computeResult.graph();

            WriteResultBuilder builder = new WriteResultBuilder(
                writeConfig,
                graph.nodeCount(),
                callContext,
                computeResult.tracker()
            );
            DisjointSetStruct dss = computeResult.result();

            builder.withCreateMillis(computeResult.createMillis());
            builder.withComputeMillis(computeResult.computeMillis());
            builder.withCommunityFunction(dss::setIdOf);

            if (write && !writeConfig.writeProperty().isEmpty()) {
                writeNodeProperties(builder, computeResult);
                graph.releaseProperties();
            }

            return Stream.of(builder.build());
        }
    }

    static class ConsecutivePropertyTranslator implements PropertyTranslator.OfLong<DisjointSetStruct> {

        // Magic number to estimate the number of communities that need to be mapped into consecutive space
        private static final long MAPPING_SIZE_QUOTIENT = 10L;

        private final HugeLongArray communities;

        ConsecutivePropertyTranslator(DisjointSetStruct dss, AllocationTracker tracker) {

            long nextConsecutiveId = -1L;

            // TODO is there a better way to set the initial size, e.g. dss.setCount
            HugeLongLongMap setIdToConsecutiveId = new HugeLongLongMap(BitUtil.ceilDiv(
                dss.size(),
                MAPPING_SIZE_QUOTIENT
            ), tracker);
            this.communities = HugeLongArray.newArray(dss.size(), tracker);

            for (int nodeId = 0; nodeId < dss.size(); nodeId++) {
                long setId = dss.setIdOf(nodeId);
                long communityId = setIdToConsecutiveId.getOrDefault(setId, -1);
                if (communityId == -1) {
                    setIdToConsecutiveId.addTo(setId, ++nextConsecutiveId);
                    communityId = nextConsecutiveId;
                }
                communities.set(nodeId, communityId);
            }
        }

        @Override
        public long toLong(DisjointSetStruct data, long nodeId) {
            return communities.get(nodeId);
        }
    }

    public static final class WriteResult {

        public final String writeProperty;
        public final String seedProperty;
        public final String relationshipWeightProperty;
        public final long nodePropertiesWritten;
        public final long relationshipPropertiesWritten;
        public final long createMillis;
        public final long computeMillis;
        public final long writeMillis;
        public final long postProcessingMillis;
        public final long componentCount;
        public final double threshold;
        public final boolean consecutiveIds;
        public final Map<String, Object> componentDistribution;

        static WriteResult empty(WccWriteConfig config, long createMillis) {
            return new WriteResult(
                config,
                0,
                createMillis,
                0, 0, 0, 0,
                Collections.emptyMap()
            );
        }

        WriteResult(
            WccWriteConfig config,
            long nodePropertiesWritten,
            long createMillis,
            long computeMillis,
            long writeMillis,
            long postProcessingMillis,
            long componentCount,
            Map<String, Object> componentDistribution
        ) {
            this.writeProperty = config.writeProperty();
            this.seedProperty = config.seedProperty();
            this.relationshipWeightProperty = config.relationshipWeightProperty();
            this.threshold = config.threshold();
            this.consecutiveIds = config.consecutiveIds();

            this.nodePropertiesWritten = nodePropertiesWritten;
            this.relationshipPropertiesWritten = 0L;
            this.createMillis = createMillis;
            this.computeMillis = computeMillis;
            this.writeMillis = writeMillis;
            this.postProcessingMillis = postProcessingMillis;
            this.componentCount = componentCount;
            this.componentDistribution = componentDistribution;
        }
    }

    static class WriteResultBuilder extends AbstractCommunityResultBuilder<WccWriteConfig, WriteResult> {

        private final WccWriteConfig config;

        WriteResultBuilder(
            WccWriteConfig config,
            long nodeCount,
            ProcedureCallContext context,
            AllocationTracker tracker
        ) {
            super(
                config,
                nodeCount,
                context,
                tracker
            );
            this.config = config;
        }

        @Override
        protected WriteResult buildResult() {
            return new WriteResult(
                config,
                nodePropertiesWritten,  // should be nodePropertiesWritten
                createMillis,
                computeMillis,
                writeMillis,
                postProcessingDuration,
                maybeCommunityCount.orElse(-1L),
                communityHistogramOrNull()
            );
        }
    }
}
