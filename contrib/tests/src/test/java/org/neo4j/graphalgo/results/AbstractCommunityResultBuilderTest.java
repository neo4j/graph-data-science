package org.neo4j.graphalgo.results;

import org.HdrHistogram.Histogram;
import org.junit.Test;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;

import java.util.function.ObjLongConsumer;

import static org.junit.Assert.assertEquals;

public final class AbstractCommunityResultBuilderTest {

    @Test
    public void countCommunitySizesOverHugeCommunities() {
        AbstractCommunityResultBuilder<Void> builder = builder((histogram, communityCount) -> {
            assertEquals("should build 10 communities", 10L, communityCount);
            assertEquals("should 2 communities with 5 members", 2L, histogram.getCountAtValue(5L));
            assertEquals("should build 8 communities with 4 members", 8L, histogram.getCountAtValue(4L));
        });
        builder.build(AllocationTracker.EMPTY, 42L, n -> n % 10L);
    }

    @Test
    public void countCommunitySizesOverPresizedHugeCommunities() {
        AbstractCommunityResultBuilder<Void> builder = builder((histogram, communityCount) -> {
            assertEquals("should build 10 communities", 10L, communityCount);
            assertEquals("should 2 communities with 5 members", 2L, histogram.getCountAtValue(5L));
            assertEquals("should build 8 communities with 4 members", 8L, histogram.getCountAtValue(4L));
        });
        builder.build(10L, AllocationTracker.EMPTY, 42L, n -> n % 10L);
    }

    @Test
    public void countCommunitySizesOverIntegerCommunities() {
        AbstractCommunityResultBuilder<Void> builder = builder((histogram, communityCount) -> {
            assertEquals("should build 42 communities", 42L, communityCount);
            assertEquals("should build 10 communities with 1 member", 10L, histogram.getCountAtValue(1L));
            assertEquals("should build 10 communities with 2 members", 10L, histogram.getCountAtValue(2L));
            assertEquals("should build 10 communities with 3 members", 10L, histogram.getCountAtValue(3L));
            assertEquals("should build 10 communities with 4 members", 10L, histogram.getCountAtValue(4L));
            assertEquals("should build 2 communities with 5 members", 2L, histogram.getCountAtValue(5L));
        });
        builder.buildfromKnownSizes(42, n -> (n / 10) + 1);
    }

    @Test
    public void countCommunitySizesOverLongCommunities() {
        AbstractCommunityResultBuilder<Void> builder = builder((histogram, communityCount) -> {
            assertEquals("should build 42 communities", 42L, communityCount);
            assertEquals("should build 10 communities with 1 member", 10L, histogram.getCountAtValue(1L));
            assertEquals("should build 10 communities with 2 members", 10L, histogram.getCountAtValue(2L));
            assertEquals("should build 10 communities with 3 members", 10L, histogram.getCountAtValue(3L));
            assertEquals("should build 10 communities with 4 members", 10L, histogram.getCountAtValue(4L));
            assertEquals("should build 2 communities with 5 members", 2L, histogram.getCountAtValue(5L));
        });
        builder.buildfromKnownLongSizes(42, n -> ((int)n / 10) + 1);
    }

    private AbstractCommunityResultBuilder<Void> builder(ObjLongConsumer<Histogram> check) {
        return new AbstractCommunityResultBuilder<Void>() {
            @Override
            protected Void build(
                    long loadMillis,
                    long computeMillis,
                    long writeMillis,
                    long postProcessingMillis,
                    long nodeCount,
                    long communityCount,
                    Histogram communityHistogram,
                    boolean write) {
                check.accept(communityHistogram, communityCount);
                return null;
            }
        };
    }
}
