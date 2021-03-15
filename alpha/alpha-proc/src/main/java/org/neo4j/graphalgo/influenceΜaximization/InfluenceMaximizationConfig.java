package org.neo4j.graphalgo.influenceΜaximization;

import org.immutables.value.Value;

import org.neo4j.graphalgo.annotation.Configuration;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.config.AlgoBaseConfig;
import org.neo4j.graphalgo.config.WritePropertyConfig;

@Configuration
@ValueClass
@SuppressWarnings( "immutables:subtype" )
public interface InfluenceMaximizationConfig extends AlgoBaseConfig, WritePropertyConfig
{ //BaseConfig
    String DEFAULT_TARGET_PROPERTY = "spreadGain";

    int k();

    @Value.Default
    default double p()
    {
        return 0.1;
    }

    @Value.Default
    default int mc()
    {
        return 1000;
    }

    @Override
    default String writeProperty()
    {
        return DEFAULT_TARGET_PROPERTY;
    }
}
