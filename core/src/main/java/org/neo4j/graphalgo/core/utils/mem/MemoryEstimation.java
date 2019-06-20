package org.neo4j.graphalgo.core.utils.mem;

import org.neo4j.graphalgo.core.GraphDimensions;

import java.util.Collection;
import java.util.Collections;

/**
 * A description of an object that has resources residing in memory.
 */
public interface MemoryEstimation {

    /**
     * @return a textual description for this component.
     */
    String description();

    /**
     * @return The resident memory of this component.
     */
    MemoryResident resident();

    /**
     * @return nested resources of this component.
     */
    default Collection<MemoryEstimation> components() {
        return Collections.emptyList();
    }

    MemoryTree apply(GraphDimensions dimensions, int concurrency);
}
