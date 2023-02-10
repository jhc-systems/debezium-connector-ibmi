/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import io.debezium.config.Configuration;
import io.debezium.relational.RelationalTableFilters;
import io.debezium.relational.Selectors;
import io.debezium.relational.Selectors.TableIdToStringMapper;
import io.debezium.relational.Tables.TableFilter;

// used to filter additional tables in snapshotting
public class As400AdditionalRelationalTableFilters extends RelationalTableFilters {

    private TableFilter addTableFilter;

    public As400AdditionalRelationalTableFilters(Configuration config, TableFilter systemTablesFilter, TableIdToStringMapper tableIdMapper, String additionalTables) {
        super(config, systemTablesFilter, tableIdMapper, false);

        this.addTableFilter = Selectors.tableSelector().includeTables(additionalTables, x -> x.schema() + "." + x.table()).build()::test;
    }

    @Override
    public TableFilter dataCollectionFilter() {
        return addTableFilter;
    }

    @Override
    public TableFilter eligibleDataCollectionFilter() {
        return super.dataCollectionFilter(); // only fetch schema for
    }

}
