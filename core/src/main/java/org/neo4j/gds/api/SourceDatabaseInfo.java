package org.neo4j.gds.api;

import org.neo4j.gds.annotation.ValueClass;

@ValueClass
public interface SourceDatabaseInfo {

    enum DatabaseLocation {
        LOCAL,
        REMOTE
    }

    DatabaseId databaseId();

    DatabaseLocation databaseLocation();
}
