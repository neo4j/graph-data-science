package org.neo4j.graphalgo.impl.labelprop;

final class StepRunner implements Runnable {

    Step current;

    StepRunner(final Step current) {
        this.current = current;
    }

    @Override
    public void run() {
        current.run();
        current = current.next();
    }
}
