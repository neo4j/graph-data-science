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
package org.neo4j.gds.compat._54;

import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.store.format.FormatFamily;
import org.neo4j.storageengine.api.StoreVersionCheck;
import org.neo4j.storageengine.api.StoreVersionIdentifier;

import static org.neo4j.gds.compat._54.InMemoryStoreVersion.STORE_VERSION;

public class InMemoryVersionCheck implements StoreVersionCheck {

    private static final StoreVersionIdentifier STORE_IDENTIFIER = new StoreVersionIdentifier(
        STORE_VERSION,
        FormatFamily.STANDARD.name(),
        0,
        0
    );

    @Override
    public boolean isCurrentStoreVersionFullySupported(CursorContext cursorContext) {
        return true;
    }

    @Override
    public MigrationCheckResult getAndCheckMigrationTargetVersion(String formatFamily, CursorContext cursorContext) {
        return new StoreVersionCheck.MigrationCheckResult(MigrationOutcome.NO_OP, STORE_IDENTIFIER, null, null);
    }

    @Override
    public UpgradeCheckResult getAndCheckUpgradeTargetVersion(CursorContext cursorContext) {
        return new StoreVersionCheck.UpgradeCheckResult(UpgradeOutcome.NO_OP, STORE_IDENTIFIER, null, null);
    }

    @Override
    public String getIntroductionVersionFromVersion(StoreVersionIdentifier storeVersionIdentifier) {
        return STORE_VERSION;
    }

    public StoreVersionIdentifier findLatestVersion(String s) {
        return STORE_IDENTIFIER;
    }
}
