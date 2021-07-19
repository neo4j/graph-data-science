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
package org.neo4j.gds.internal;

import com.github.marschall.memoryfilesystem.MemoryFileSystemBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.TestLog;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphalgo.core.model.ModelCatalog;
import org.neo4j.graphalgo.utils.CheckedSupplier;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

class AuraBackupBaseNegativeProcTest extends AuraBackupBaseProcTest {

    // instantiate an in-memory file system with
    // default permissions set to read-only
    private final FileSystem fs = CheckedSupplier.supplier(
        () -> MemoryFileSystemBuilder.newEmpty()
            .addFileAttributeView(PosixFileAttributeView.class)
            .setUmask(Set.of(
                PosixFilePermission.OWNER_WRITE,
                PosixFilePermission.GROUP_WRITE,
                PosixFilePermission.OTHERS_WRITE
            ))
            .build())
        .get();


    Path getBackupLocation() {
        return fs.getPath("/temp");
    }

    @AfterEach
    void shutdownFileSystem() throws IOException {
        fs.close();
    }

    @Test
    void shouldCollectErrorsWhenPersistingGraphStores() {
        var shutdownQuery = "CALL gds.internal.backup()";

        var backupName = new AtomicReference<String>();

        runQueryWithRowConsumer(shutdownQuery, row -> {
            assertThat(row.getBoolean("done")).isFalse();
            backupName.set(row.getString("backupName"));
        });

        var backupPath = getBackupLocation().resolve(backupName.get()).resolve("graphs").toAbsolutePath().toString();

        assertThat(testLog.getMessages(TestLog.WARN))
            .contains(formatWithLocale(
                "Graph backup failed for graph 'first' for user 'userA' - Directory '%s' not writable: ",
                backupPath
            ));
    }
}
