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

import org.apache.commons.io.FileUtils;
import org.neo4j.gds.model.StoredModel;
import org.neo4j.graphalgo.BaseProc;
import org.neo4j.graphalgo.core.model.ImmutableModel;
import org.neo4j.graphalgo.core.model.ModelCatalog;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.io.IOException;
import java.util.stream.Stream;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;
import static org.neo4j.procedure.Mode.READ;

public class ModelDeleteProc extends BaseProc {

    private static final String DESCRIPTION = "Deletes a stored model from disk.";

    @Procedure(name = "gds.alpha.model.delete", mode = READ)
    @Description(DESCRIPTION)
    public Stream<ModelDeleteResult> delete(@Name(value = "modelName") String modelName) throws IOException {
        var model = ModelCatalog.getUntyped(username(), modelName);

        if (!(model instanceof StoredModel)) {
            throw new IllegalArgumentException(formatWithLocale("The model %s is not stored.", modelName));
        }

        var storedModel = (StoredModel) model;

        var timer = ProgressTimer.start();

        var modelDir = storedModel.fileLocation();
        FileUtils.deleteDirectory(modelDir.toFile());

        if (storedModel.loaded()) {
            var unstoredModel = ImmutableModel.builder().from(storedModel).build();
            ModelCatalog.setUnsafe(unstoredModel);
        } else {
            ModelCatalog.drop(username(), modelName);
        }

        timer.stop();

        return Stream.of(new ModelDeleteResult(modelName, timer.getDuration()));
    }

    public static class ModelDeleteResult {
        public final String modelName;
        public final long deleteMillis;

        ModelDeleteResult(String modelName, long deleteMillis) {
            this.modelName = modelName;
            this.deleteMillis = deleteMillis;
        }
    }
}
