package org.neo4j.graphalgo.impl.unionfind;

import com.carrotsearch.hppc.BitSet;
import org.junit.jupiter.params.provider.Arguments;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.huge.loader.HugeGraphFactory;
import org.neo4j.graphalgo.core.neo4jview.GraphViewFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.stream.Stream;

import static org.junit.jupiter.params.provider.Arguments.arguments;

public abstract class UnionFindTestBase {

    static GraphDatabaseAPI DB;
    static final RelationshipType RELATIONSHIP_TYPE = RelationshipType.withName("TYPE");

    static Stream<Arguments> parameters() {
        return Stream.of(
                arguments("Heavy", HeavyGraphFactory.class, UnionFindType.PARALLEL),
                arguments("Heavy", HeavyGraphFactory.class, UnionFindType.FJ_MERGE),
                arguments("Heavy", HeavyGraphFactory.class, UnionFindType.FORK_JOIN),
                arguments("Huge", HugeGraphFactory.class, UnionFindType.PARALLEL),
                arguments("Huge", HugeGraphFactory.class, UnionFindType.FJ_MERGE),
                arguments("Huge", HugeGraphFactory.class, UnionFindType.FORK_JOIN),
                arguments("Kernel", GraphViewFactory.class, UnionFindType.PARALLEL),
                arguments("Kernel", GraphViewFactory.class, UnionFindType.FJ_MERGE),
                arguments("Kernel", GraphViewFactory.class, UnionFindType.FORK_JOIN)
        );
    }

    abstract int communitySize();

    protected DisjointSetStruct run(UnionFindType uf, Graph graph, UnionFind.Config config) {
        return UnionFindHelper.run(
                uf,
                graph,
                Pools.DEFAULT,
                communitySize() / Pools.DEFAULT_CONCURRENCY,
                Pools.DEFAULT_CONCURRENCY,
                config,
                AllocationTracker.EMPTY);
    }

    /**
     * Compute number of sets present.
     */
    protected static long getSetCount(DisjointSetStruct struct) {
        long capacity = struct.size();
        BitSet sets = new BitSet(capacity);
        for (long i = 0L; i < capacity; i++) {
            long setId = struct.setIdOf(i);
            sets.set(setId);
        }
        return sets.cardinality();
    }

}
