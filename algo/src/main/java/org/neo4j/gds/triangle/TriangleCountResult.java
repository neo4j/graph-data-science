package org.neo4j.gds.triangle;

import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.properties.nodes.LongNodePropertyValues;
import org.neo4j.gds.collections.haa.HugeAtomicLongArray;
import org.neo4j.gds.core.utils.paged.HugeArrayToNodeProperties;

@ValueClass
public interface TriangleCountResult {
    // value at index `i` is number of triangles for node with id `i`
    HugeAtomicLongArray localTriangles();

    long globalTriangles();

    static TriangleCountResult of(
        HugeAtomicLongArray triangles,
        long globalTriangles
    ) {
        return ImmutableTriangleCountResult
            .builder()
            .localTriangles(triangles)
            .globalTriangles(globalTriangles)
            .build();
    }

    default LongNodePropertyValues asNodeProperties() {

        return HugeArrayToNodeProperties.convert(localTriangles());
    }
}
