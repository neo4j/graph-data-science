package org.neo4j.graphalgo.impl.louvain;

import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphalgo.core.write.Exporter;
import org.neo4j.graphalgo.core.write.PropertyTranslator;
import org.neo4j.graphalgo.impl.Algorithm;
import org.neo4j.values.storable.Values;

import java.util.List;
import java.util.stream.Stream;

public abstract class LouvainAlgo<Self extends LouvainAlgo<Self>> extends Algorithm<Self> {

    static final PropertyTranslator<int[][]> COMMUNITIES_TRANSLATOR =
            (propertyId, allCommunities, nodeId) -> {
                // build int array
                int id = (int) nodeId;
                final int[] data = new int[allCommunities.length];
                for (int i = 0; i < data.length; i++) {
                    data[i] = allCommunities[i][id];
                }
                return Values.intArray(data);
            };

    static final PropertyTranslator<HugeLongArray[]> HUGE_COMMUNITIES_TRANSLATOR =
            (propertyId, allCommunities, nodeId) -> {
                // build int array
                final long[] data = new long[allCommunities.length];
                for (int i = 0; i < data.length; i++) {
                    data[i] = allCommunities[i].get(nodeId);
                }
                return Values.longArray(data);
            };


    /**
     * @return number of outer iterations
     */
    public abstract int getLevel();

    public abstract double[] getModularities();

    public double getFinalModularity() {
        double[] modularities = getModularities();
        return modularities[modularities.length - 1];
    }

    public abstract long communityIdOf(long node);

    public abstract void export(
            Exporter exporter,
            String propertyName,
            boolean includeIntermediateCommunities,
            String intermediateCommunitiesPropertyName);

    public abstract Stream<StreamingResult> dendrogramStream(boolean includeIntermediateCommunities);

    @SuppressWarnings("unchecked")
    @Override
    public final Self me() {
        return (Self) this;
    }

    /**
     * result object
     */
    public static final class Result {

        public final long nodeId;
        public final long community;

        public Result(long id, long community) {
            this.nodeId = id;
            this.community = community;
        }
    }

    public static final class StreamingResult {
        public final long nodeId;
        public final List<Long> communities;
        public final long community;

        public StreamingResult(long nodeId, List<Long> communities, long community) {
            this.nodeId = nodeId;
            this.communities = communities;
            this.community = community;
        }
    }
}
