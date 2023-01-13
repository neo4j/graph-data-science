package org.neo4j.gds.beta.pregel.cc;

import javax.annotation.processing.Generated;
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.beta.pregel.Pregel;
import org.neo4j.gds.beta.pregel.PregelProcedureConfig;
import org.neo4j.gds.beta.pregel.PregelResult;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

@Generated("org.neo4j.gds.beta.pregel.PregelProcessor")
public final class BidirectionalComputationAlgorithm extends Algorithm<PregelResult> {
    private final Pregel<PregelProcedureConfig> pregelJob;

    BidirectionalComputationAlgorithm(Graph graph, PregelProcedureConfig configuration,
                                      ProgressTracker progressTracker) {
        super(progressTracker);
        this.pregelJob = Pregel.create(graph, configuration, new BidirectionalComputation(), Pools.DEFAULT, progressTracker);
    }

    @Override
    public void setTerminationFlag(TerminationFlag terminationFlag) {
        super.setTerminationFlag(terminationFlag);
        pregelJob.setTerminationFlag(terminationFlag);
    }

    @Override
    public PregelResult compute() {
        return pregelJob.run();
    }

    @Override
    public void release() {
        pregelJob.release();
    }
}
