package org.neo4j.graphalgo.impl.labelprop;

interface Step extends Runnable {
    @Override
    void run();

    Step next();

}
