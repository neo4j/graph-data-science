package org.neo4j.gds.ml.pipeline.nodePipeline.train;

import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.NodeProperties;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.openjdk.jol.util.Multiset;

public class LabelsAndClassCountsExtractor {

    static LabelsAndClassCounts extractLabelsAndClassCounts(
        NodeProperties targetNodeProperty,
        long nodeCount
    ) {
        var classCounts = new Multiset<Long>();
        var labels = HugeLongArray.newArray(nodeCount);
        for (long nodeId = 0; nodeId < nodeCount; nodeId++) {
            labels.set(nodeId, targetNodeProperty.longValue(nodeId));
            classCounts.add(targetNodeProperty.longValue(nodeId));
        }
        return ImmutableLabelsAndClassCounts.of(labels, classCounts);
    }

    @ValueClass
    interface LabelsAndClassCounts {
        HugeLongArray labels();

        Multiset<Long> classCounts();
    }
}
