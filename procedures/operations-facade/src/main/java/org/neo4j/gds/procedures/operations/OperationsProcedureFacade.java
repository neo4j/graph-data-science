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
package org.neo4j.gds.procedures.operations;

import org.neo4j.gds.applications.ApplicationsFacade;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.core.utils.warnings.UserLogEntry;

import java.util.stream.Stream;

public class OperationsProcedureFacade {
    private final ApplicationsFacade applicationsFacade;

    public OperationsProcedureFacade(ApplicationsFacade applicationsFacade) {
        this.applicationsFacade = applicationsFacade;
    }

    public Stream<ProgressResult> listProgress(String jobIdAsString) {
        if (jobIdAsString.isBlank()) return summaryView();

        var jobId = new JobId(jobIdAsString);

        return detailView(jobId);
    }

    public Stream<UserLogEntry> queryUserLog(String jobId) {
        return applicationsFacade.operations().queryUserLog(jobId);
    }

    public Stream<FeatureState> resetUseMixedAdjacencyList() {
        var isEnabled = applicationsFacade.operations().resetUseMixedAdjacencyList();

        return Stream.of(new FeatureState(isEnabled));
    }

    public Stream<FeatureState> resetUsePackedAdjacencyList() {
        var isEnabled = applicationsFacade.operations().resetUsePackedAdjacencyList();

        return Stream.of(new FeatureState(isEnabled));
    }

    public Stream<FeatureState> resetUseUncompressedAdjacencyList() {
        var isEnabled = applicationsFacade.operations().resetUseUncompressedAdjacencyList();

        return Stream.of(new FeatureState(isEnabled));
    }

    public void setPagesPerThread(long value) {
        applicationsFacade.operations().setPagesPerThread(value);
    }

    public void setUseMixedAdjacencyList(boolean value) {
        applicationsFacade.operations().setUseMixedAdjacencyList(value);
    }

    public void setUsePackedAdjacencyList(boolean value) {
        applicationsFacade.operations().setUsePackedAdjacencyList(value);
    }

    public void setUseUncompressedAdjacencyList(boolean value) {
        applicationsFacade.operations().setUseUncompressedAdjacencyList(value);
    }

    private Stream<ProgressResult> detailView(JobId jobId) {
        var resultRenderer = new DefaultResultRenderer(jobId);

        return applicationsFacade.operations().listProgress(jobId, resultRenderer);
    }

    private Stream<ProgressResult> summaryView() {
        var results = applicationsFacade.operations().listProgress();

        return results.map(ProgressResult::fromTaskStoreEntry);
    }
}
