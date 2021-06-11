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
package org.neo4j.gds.model.catalog;

import org.neo4j.graphalgo.BaseProc;
import org.neo4j.graphalgo.core.GdsEdition;
import org.neo4j.graphalgo.core.model.ModelCatalog;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.io.IOException;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;
import static org.neo4j.procedure.Mode.READ;

public class ModelLoadProc extends BaseProc {

    private static final String DESCRIPTION = "Load a stored model into main memory.";

    @Procedure(name = "gds.alpha.model.load", mode = READ)
    @Description(DESCRIPTION)
    public Stream<ModelLoadResult> load(@Name(value = "modelName") String modelName) throws IOException {
        if (!GdsEdition.instance().isOnEnterpriseEdition()) {
            throw new RuntimeException("Loading a model is only available with the Graph Data Science library Enterprise Edition.");
        }

        var model = ModelCatalog.getUntyped(username(), modelName);

        if (model.loaded()) {
            return Stream.of(new ModelLoadResult(modelName, 0));
        }

        if (!model.stored()) {
            throw new IllegalArgumentException(formatWithLocale("The model %s is not stored.", modelName));
        }

        var timer = ProgressTimer.start();
        model.load();
        timer.stop();

        return Stream.of(new ModelLoadResult(modelName, timer.getDuration()));
    }

    public static class ModelLoadResult {
        public final String modelName;
        public final long loadMillis;

        ModelLoadResult(String modelName, long loadMillis) {
            this.modelName = modelName;
            this.loadMillis = loadMillis;
        }
    }
}
