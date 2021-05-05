/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.graphalgo.doc;

import org.neo4j.gds.ml.nodemodels.NodeClassificationPredictMutateProc;
import org.neo4j.gds.ml.nodemodels.NodeClassificationPredictStreamProc;
import org.neo4j.gds.ml.nodemodels.NodeClassificationPredictWriteProc;
import org.neo4j.gds.ml.nodemodels.NodeClassificationTrainProc;
import org.neo4j.graphalgo.catalog.GraphCreateProc;
import org.neo4j.graphalgo.catalog.GraphStreamNodePropertiesProc;

import java.util.List;

class NodeClassificationDocTest extends DocTestBase {

    @Override
    List<Class<?>> procedures() {
        return List.of(
            NodeClassificationTrainProc.class,
            NodeClassificationPredictStreamProc.class,
            NodeClassificationPredictMutateProc.class,
            NodeClassificationPredictWriteProc.class,
            GraphCreateProc.class,
            GraphStreamNodePropertiesProc.class
        );
    }

    @Override
    String adocFile() {
        return "algorithms/alpha/nodeclassification/nodeclassification.adoc";
    }
}
