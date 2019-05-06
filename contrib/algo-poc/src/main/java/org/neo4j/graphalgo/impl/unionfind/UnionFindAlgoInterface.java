package org.neo4j.graphalgo.impl.unionfind;

import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.PagedDisjointSetStruct;

import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;

public interface UnionFindAlgoInterface
{
    PagedDisjointSetStruct run(
            Graph graph,
            ExecutorService executor,
            AllocationTracker tracker,
            int minBatchSize,
            int concurrency,
            double threshold,
            BiConsumer<String, Algorithm<?>> prepare);

}
