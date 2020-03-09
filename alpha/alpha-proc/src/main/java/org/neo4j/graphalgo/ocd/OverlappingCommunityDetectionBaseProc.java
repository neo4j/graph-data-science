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
package org.neo4j.graphalgo.ocd;

import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.AlphaAlgorithmFactory;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.impl.ocd.CommunityAffiliations;
import org.neo4j.graphalgo.impl.ocd.ConductanceAffiliationInitializer;
import org.neo4j.graphalgo.impl.ocd.OverlappingCommunityDetection;
import org.neo4j.graphalgo.impl.ocd.OverlappingCommunityDetectionBaseConfig;
import org.neo4j.logging.Log;

abstract class OverlappingCommunityDetectionBaseProc<CONFIG extends OverlappingCommunityDetectionBaseConfig> extends AlgoBaseProc<OverlappingCommunityDetection, CommunityAffiliations, CONFIG> {
    protected static final String DESCRIPTION = "The BIGCLAM overlapping community detection algorithm can detect non-overlapping, overlapping and nested community structures at scale";

    @Override
    protected AlgorithmFactory<OverlappingCommunityDetection, CONFIG> algorithmFactory(CONFIG config) {
        return new AlphaAlgorithmFactory<OverlappingCommunityDetection, CONFIG>() {
            @Override
            public OverlappingCommunityDetection build(
                Graph graph, CONFIG configuration, AllocationTracker tracker, Log log
            ) {
                ConductanceAffiliationInitializer initializer = new ConductanceAffiliationInitializer(
                    transaction,
                    Pools.DEFAULT,
                    tracker,
                    log,
                    configuration.concurrency()
                );
                return new OverlappingCommunityDetection(
                    graph,
                    initializer,
                    Pools.DEFAULT,
                    log,
                    configuration.gradientConcurrency() == -1 ? configuration.concurrency() : configuration.gradientConcurrency()
                )
                    .withProgressLogger(ProgressLogger.wrap(log, "OverlappingCommunityDetection"))
                    .withTerminationFlag(TerminationFlag.wrap(transaction));
            }
        };
    }
}
