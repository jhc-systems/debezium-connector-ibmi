/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import io.debezium.config.Configuration;
import io.debezium.relational.RelationalTableFilters;
import io.debezium.relational.Selectors.TableIdToStringMapper;
import io.debezium.relational.Tables.TableFilter;

// used to filter what schemas we fetch
public class As400NormalRelationalTableFilters extends RelationalTableFilters {

    public As400NormalRelationalTableFilters(Configuration config, TableFilter systemTablesFilter, TableIdToStringMapper tableIdMapper) {
        super(config, systemTablesFilter, tableIdMapper, false);
    }

    // make eligible the same as the data collection
    @Override
    public TableFilter eligibleDataCollectionFilter() {
        return super.dataCollectionFilter(); // only fetch schema for
    }

}
