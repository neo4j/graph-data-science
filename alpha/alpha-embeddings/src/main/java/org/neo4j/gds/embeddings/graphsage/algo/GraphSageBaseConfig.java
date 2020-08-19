/*
 * Copyright (c) 2017-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.gds.embeddings.graphsage.algo;

import org.neo4j.gds.embeddings.graphsage.Layer;
import org.neo4j.graphalgo.annotation.Configuration;
import org.neo4j.graphalgo.config.AlgoBaseConfig;
import org.neo4j.graphalgo.config.BatchSizeConfig;
import org.neo4j.graphalgo.config.NodePropertiesConfig;
import org.neo4j.graphalgo.config.TrainConfig;
import org.neo4j.graphalgo.core.model.ModelCatalog;

import java.util.List;

public interface GraphSageBaseConfig extends AlgoBaseConfig, BatchSizeConfig, TrainConfig, NodePropertiesConfig {

    @Override
    @Configuration.Ignore
    default List<String> nodePropertyNames() {
        var model = ModelCatalog.get(modelName(), Layer[].class, GraphSageTrainConfig.class);
        return model.trainConfig().nodePropertyNames();
    }
}
