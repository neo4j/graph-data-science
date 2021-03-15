package org.neo4j.graphalgo.influenceΜaximization;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.AlphaAlgorithmFactory;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.impl.influenceMaximization.CELF;
import org.neo4j.graphalgo.result.AbstractResultBuilder;
import org.neo4j.graphalgo.results.InfluenceMaximizationResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import static org.neo4j.procedure.Mode.READ;

public class CELFProc extends AlgoBaseProc<CELF,CELF,InfluenceMaximizationConfig>
{
    private static final String DESCRIPTION =
            "CELF algorithm for Influence Maximization aims to find `k` nodes that maximize the expected spread of influence in the network";

    @Procedure( name = "gds.alpha.celf.stream", mode = READ )
    @Description( DESCRIPTION )
    public Stream<InfluenceMaximizationResult> stream(
            @Name( value = "graphName" ) Object graphNameOrConfig,
            @Name( value = "configuration", defaultValue = "{}" ) Map<String,Object> configuration )
    {

        ComputationResult<CELF,CELF,InfluenceMaximizationConfig> computationResult = compute( graphNameOrConfig, configuration );

        if ( computationResult.graph().isEmpty() )
        {
            computationResult.graph().release();
            return Stream.empty();
        }

        computationResult.graph().release();
        return computationResult.algorithm().resultStream();
    }

    @Procedure( name = "gds.alpha.celf.stats", mode = READ )
    @Description( DESCRIPTION )
    public Stream<InfluenceMaximizationResult.Stats> stats(
            @Name( value = "graphName" ) Object graphNameOrConfig,
            @Name( value = "configuration", defaultValue = "{}" ) Map<String,Object> configuration
    )
    {
        ComputationResult<CELF,CELF,InfluenceMaximizationConfig> computationResult = compute( graphNameOrConfig, configuration );

        InfluenceMaximizationConfig config = computationResult.config();
        Graph graph = computationResult.graph();

        AbstractResultBuilder<InfluenceMaximizationResult.Stats> builder = new InfluenceMaximizationResult.Stats.Builder()
                .withNodeCount( graph.nodeCount() )
                .withConfig( config )
                .withComputeMillis( computationResult.computeMillis() );

        return Stream.of( builder.build() );
    }

    @Override
    protected InfluenceMaximizationConfig newConfig(
            String username,
            Optional<String> graphName,
            Optional<GraphCreateConfig> maybeImplicitCreate,
            CypherMapWrapper config
    )
    {
        return new InfluenceMaximizationConfigImpl( graphName, maybeImplicitCreate, username, config );
    }

    @Override
    protected AlgorithmFactory<CELF,InfluenceMaximizationConfig> algorithmFactory()
    {
        return (AlphaAlgorithmFactory<CELF,InfluenceMaximizationConfig>) ( graph, configuration, tracker, log, eventTracker ) -> new CELF(
                graph,
                configuration.k(),
                configuration.p(),
                configuration.mc(),
                Pools.DEFAULT,
                configuration.concurrency(),
                tracker
        );
    }
}
