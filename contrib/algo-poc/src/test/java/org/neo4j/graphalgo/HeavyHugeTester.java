package org.neo4j.graphalgo;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.huge.loader.HugeGraphFactory;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public abstract class HeavyHugeTester {

    protected Class<? extends GraphFactory> graphImpl;

    @Parameterized.Parameters(name = "{1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[]{HeavyGraphFactory.class, "heavy"},
                new Object[]{HugeGraphFactory.class, "huge"}
        );
    }

    protected HeavyHugeTester(Class<? extends GraphFactory> graphImpl) {
        this.graphImpl = graphImpl;
    }
}
