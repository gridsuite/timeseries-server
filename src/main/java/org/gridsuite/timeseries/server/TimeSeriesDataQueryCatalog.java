/**
 * Copyright (c) 2022, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.gridsuite.timeseries.server;

import java.util.List;
import java.util.stream.Collectors;

public final class TimeSeriesDataQueryCatalog {

    public static final String INSERT = "insert into timeseries_group_data ( group_id, time, json_obj ) values (?,?,?);";
    public static final String COUNT = "select count(*) from timeseries_group_data where group_id=?;";
    public static final String DELETE = "delete from timeseries_group_data where group_id=?";

    private static final String SELECTALL = "select time, json_obj from timeseries_group_data where group_id=? and time>=? and time <? order by time;";

    public static String makeSelect(List<String> timeSeriesNames) {
        //TODO validate timeSeriesName sql injection
        //filter on time series names ( select time, json_build_object ('a', json_obj->'a', 'd', json_obj->'d', 'e', json_obj->'e'), ...  instead of the whole json_obj)
        if (timeSeriesNames == null || timeSeriesNames.isEmpty()) {
            return SELECTALL;
        } else {
            // TODO here if we try with 51 time series names we get an exception
            // Caused by: org.postgresql.util.PSQLException: ERROR: cannot pass more than 100 arguments to a function
            return timeSeriesNames.stream().map(c -> "'" + c + "', json_obj->'" + c + "'").collect(Collectors.joining(", ",
                    "select time, json_build_object (",
                    ") json_obj from timeseries_group_data where group_id=? and time>=? and time <? order by time;"));
        }
    }

    private TimeSeriesDataQueryCatalog() {
    }
}
