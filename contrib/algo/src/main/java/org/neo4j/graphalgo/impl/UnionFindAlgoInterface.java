package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;

import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;

public interface UnionFindAlgoInterface
{
    DSSResult run(
            Graph graph,
            ExecutorService executor,
            AllocationTracker tracker,
            int minBatchSize,
            int concurrency,
            double threshold,
            BiConsumer<String, Algorithm<?>> prepare);

}
