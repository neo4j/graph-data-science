package org.neo4j.graphalgo.impl.unionfind;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.DisjointSetStruct;
import org.neo4j.logging.NullLog;

import java.util.concurrent.ExecutorService;

public class UnionFindHelper {

    public static DisjointSetStruct run(
            UnionFindType algorithmType,
            Graph graph,
            ExecutorService executor,
            int minBatchSize,
            int concurrency,
            final UnionFind.Config config,
            AllocationTracker tracker) {

        UnionFind<?> algo = algorithmType
                .create(graph, executor, minBatchSize, concurrency, config, tracker, NullLog.getInstance());
        DisjointSetStruct communities = algo.compute();
        algo.release();
        return communities;
    }
}
