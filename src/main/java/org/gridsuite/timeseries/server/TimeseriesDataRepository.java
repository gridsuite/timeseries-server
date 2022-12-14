/**
 * Copyright (c) 2022, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.gridsuite.timeseries.server;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.function.BiFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.powsybl.timeseries.DoubleDataChunk;
import com.powsybl.timeseries.DoubleTimeSeries;
import com.powsybl.timeseries.StoredDoubleTimeSeries;
import com.powsybl.timeseries.StringDataChunk;
import com.powsybl.timeseries.StringTimeSeries;
import com.powsybl.timeseries.TimeSeries;
import com.powsybl.timeseries.TimeSeriesIndex;
import com.powsybl.timeseries.UncompressedDoubleDataChunk;
import com.powsybl.timeseries.UncompressedStringDataChunk;
import com.zaxxer.hikari.HikariDataSource;

/**
 * @author Jon Schuhmacher <jon.harper at rte-france.com>
 */

@Repository
public class TimeseriesDataRepository {

    private static final Logger LOGGER = LoggerFactory.getLogger(TimeseriesDataRepository.class);

    private static final String INSERT = "insert into timeseries_group_data ( group_id, time, json_obj ) values (?,?,?);";
    private static final String COUNT = "select count(*) from timeseries_group_data where group_id=?;";
    private static final String SELECTALL = "select time, json_obj from timeseries_group_data where group_id=? and time>=? and time <?;";
    private static final String DELETE = "delete from timeseries_group_data where group_id=?";

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private HikariDataSource datasource;

    @Autowired
    private TimeSeriesMetadataService timeseriesMetadataService;

    // TODO tune these parameters for performance
    // TODO make these parameters in application.yaml
    private static final int WRITE_BATCHSIZE = 30000;
    private static final int WRITE_THREADSIZE = 3; // 3 batches => e.g. 300 rows of 300cols
    private static final int READ_THREADSIZE = 300; // 300 db rows, TODO take the number of cols into account

    public void save(UUID uuid, List<TimeSeries> listTimeseries) {
        try {
            dosave(uuid, listTimeseries);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // TODO untangle multithreaded scatter/gather from actual work
    private void dosave(UUID uuid, List<TimeSeries> listTimeseries) throws Exception {

        int colcount = listTimeseries.size();
        int rowcount = listTimeseries.get(0).getMetadata().getIndex().getPointCount();

        int batchrow = (WRITE_BATCHSIZE + colcount - 1) / colcount;
        int batchcount = (rowcount + batchrow - 1) / batchrow;

        int threadcount = (batchcount + WRITE_THREADSIZE - 1) / WRITE_THREADSIZE;
        int threadbatches = (batchcount + threadcount - 1) / threadcount;

        Callable[] callables = new Callable[threadcount];

        LOGGER.debug(
                "insert {} in batch of {} rows ({} doubles for each batch), numbatch={}, numthreads={}, threadbatches={}",
                uuid, batchrow, batchrow * colcount, batchcount, threadcount, threadbatches);
        long a = System.nanoTime();

        // TODO here we transpose, which means it's impossible to stream
        // data from the client to the database, the server has to buffer in memory.
        // try to change the API to allow streaming.
        // TODO avoid copying the data (timeseries toArray())?
        BiFunction<Integer, Integer, Object> stringOrDoubledataGetter;
        if (listTimeseries.get(0) instanceof DoubleTimeSeries) {
            List<double[]> datadouble = new ArrayList<>();
            for (int i = 0; i < listTimeseries.size(); i++) {
                // TODO timeseries raw type
                datadouble.add(((DoubleTimeSeries) listTimeseries.get(i)).toArray());
            }
            stringOrDoubledataGetter = (row, col) -> datadouble.get(row)[col];
        } else if (listTimeseries.get(0) instanceof StringTimeSeries) {
            List<String[]> datastring = new ArrayList<>();
            for (int i = 0; i < listTimeseries.size(); i++) {

                datastring.add(((StringTimeSeries) listTimeseries.get(i)).toArray());
            }
            stringOrDoubledataGetter = (row, col) -> datastring.get(row)[col];
        } else {
            throw new RuntimeException("Unsupported save of timeseries type" + listTimeseries.get(0).getClass());
        }

        for (int i = 0; i < threadcount; i++) {
            int iCopy = i;
            callables[i] = () -> {
                try (var conn = datasource.getConnection();
                     var ps = conn.prepareStatement(INSERT);
                ) {
                    conn.setAutoCommit(false);

                    int threadrowstart = iCopy * threadbatches * batchrow;
                    int remainingrows = rowcount % (threadbatches * batchrow);
                    int threadrowcount = iCopy == threadcount - 1 && remainingrows > 0 ? remainingrows
                            : threadbatches * batchrow;
                    for (int l = 0; l < threadrowcount; l++) {
                        int row = threadrowstart + l;
                        Map<String, Object> tsdata = new HashMap<>();
                        for (int m = 0; m < colcount; m++) {
                            int col = m;
                            String tsName = listTimeseries.get(col).getMetadata().getName();
                            Object tsData = stringOrDoubledataGetter.apply(col, row);
                            tsdata.put(tsName, tsData);
                        }
                        ps.setObject(1, uuid);
                        // TODO instants/durations ?
//                        ps.setObject(1,
//                                Timestamp.from(listTimeseries.get(0).getMetadata().getIndex().getInstantAt(row)));
                        ps.setInt(2, l);
                        ps.setObject(3, objectMapper.writeValueAsString(tsdata), java.sql.Types.OTHER);
                        ps.addBatch();

                        if (l == threadrowcount - 1 || (l % batchrow) == batchrow - 1) {
                            ps.executeBatch();
                        }
                    }

                    //TODO rollback
                    conn.commit();
                    conn.setAutoCommit(true);
                }
                return null;
            };
        }
        //TODO improve multithread impl ? use better APIs than forkjoinpool ? don't create the pool for each request ?
        if (threadcount > 1) {
            ForkJoinTask[] tasks = new ForkJoinTask[threadcount];
            int size = datasource.getMaximumPoolSize();
            LOGGER.debug("Starting inserts in forkjoinpool size={}", size);
            ForkJoinPool pool = new ForkJoinPool(size);
            for (int i = 0; i < threadcount; i++) {
                tasks[i] = pool.submit(callables[i]);
            }
            for (int i = 0; i < threadcount; i++) {
                tasks[i].get();
            }
        } else {
            LOGGER.debug("Starting inserts in http thread");
            callables[0].call();
        }
        long b = System.nanoTime();
        LOGGER.debug("inserted {} took: {}ms", uuid, (b - a) / 1000000);
    }

    public List<TimeSeries> findById(TimeSeriesIndex index, Map<String, Object> individualMetadatas, UUID uuid, boolean tryToCompress, String time, String col) {
        try {
            return dofindById(index, individualMetadatas, uuid, tryToCompress, time, col);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // TODO untangle multithreaded scatter/gather from actual work
    private List<TimeSeries> dofindById(TimeSeriesIndex index, Map<String, Object> individualMetadatas, UUID uuid, boolean tryToCompress, String time, String col) throws Exception {
        long a = System.nanoTime();
        int cnt = -1;
        //TODO maintain this as a separate metadata instead of select count(*) when requesting all rows ?
        //TODO maintain an estimated col count as metadata instead of just guessing when requesting all cols ?
        try (var connection = datasource.getConnection();
             var ps = connection.prepareStatement(COUNT);) {
            ps.setObject(1, uuid);
            try (var resultSet = ps.executeQuery();) {
                if (resultSet.next()) {
                    cnt = resultSet.getInt(1);
                }
            }
        }
        long b = System.nanoTime();
        int threadcount = (cnt + READ_THREADSIZE - 1) / READ_THREADSIZE;
        Callable[] callables = new Callable[threadcount];
        for (int i = 0; i < threadcount; i++) {
            int iCopy = i;
            int threadrowstart = iCopy * READ_THREADSIZE;
            int threadrowend = (iCopy + 1) * READ_THREADSIZE;
            callables[i] = () -> {
                Map<Object, Object> threadres = new LinkedHashMap<>();
                try (var connection = datasource.getConnection();
                //TODO, add filter on rows (start < time < end)
                //TODO, add filter on cols ( select json_obj->>XXX, json_obj->>YYY, ...  instead of the whole json_obj)
                //     var ps = connection.prepareStatement("select  sim_time,  from simulations_10 where group_id=? and and sim_time >= ? and sim_time < ?;");
                     var ps = connection.prepareStatement(SELECTALL);
                ) {
                    ps.setObject(1, uuid);
                    // TODO instants/durations ?
                    ps.setInt(2, threadrowstart);
                    ps.setInt(3, threadrowend);
                    try (var resultSet = ps.executeQuery();) {
                        while (resultSet.next()) {
                            // TODO avoid copying the data by writing directly from each thread to the final
                            // structure ?
                            // TODO instants/durations ?
                            threadres.put(resultSet.getInt(1), objectMapper.readValue(resultSet.getString(2), Map.class));
                        }
                    }
                }
                return threadres;
            };
        }

        Map<Object, Object> res;
        if (threadcount > 1) {
            ForkJoinTask[] tasks = new ForkJoinTask[threadcount];
            int size = datasource.getMaximumPoolSize();
            LOGGER.debug("Starting inserts in forkjoinpool size={}", size);
            ForkJoinPool pool = new ForkJoinPool(size);
            for (int i = 0; i < threadcount; i++) {
                tasks[i] = pool.submit(callables[i]);
            }
            res = new LinkedHashMap<>();
            for (int i = 0; i < threadcount; i++) {
                // TODO avoid copying the data by writing directly from each thread to the final
                // structure ?
                res.putAll((Map) tasks[i].get());
            }
        } else {
            LOGGER.debug("Starting inserts in http thread");
            res = (Map) callables[0].call();
        }
        long c = System.nanoTime();
        LOGGER.debug("read {} took {}ms (count {}ms, {} queries in {} threads {}ms)", uuid, (c - a) / 1000000,
                (b - a) / 1000000, cnt, threadcount, (c - b) / 1000000);

        // TODO same as save, avoid the transpose to allow stream from database to
        // clients ?
        // TODO avoid this extra copy to an intermediate transposed map
        Map<String, List<Object>> data = new HashMap<>();
        for (Map.Entry<Object, Object> entry : res.entrySet()) {
            Map<Object, Object> dict = (Map) entry.getValue();
            for (Map.Entry<Object, Object> entryPoint : dict.entrySet()) {
                String tsname = (String) entryPoint.getKey();
                // TODO more types
                Object val = entryPoint.getValue();
                data.computeIfAbsent(tsname, _ignored -> new ArrayList<>()).add(val);
            }
        }
        List<TimeSeries> ret = new ArrayList<>();
        for (Map.Entry<String, List<Object>> entry : data.entrySet()) {
            // TODO remove duplication
            if (entry.getValue().get(0) instanceof Double) {
                double[] doubles = entry.getValue().stream().map(Double.class::cast).mapToDouble(Double::doubleValue)
                        .toArray();

                // TODO should be in the timeseries API ?
                DoubleDataChunk ddc = new UncompressedDoubleDataChunk(0, doubles);
                // TODO get compress mode from the metadata sent by the client
                if (tryToCompress) {
                    ddc = ddc.tryToCompress();
                }
                // TODO more types
                // TODO index from client
                TimeSeries timeseries = new StoredDoubleTimeSeries(timeseriesMetadataService.getMetadata(index, individualMetadatas, entry.getKey()), List.of(ddc));
                ret.add(timeseries);
            } else if (entry.getValue().get(0) instanceof String) {
                String[] strings = entry.getValue().toArray(new String[0]);

                // TODO should be in the timeseries API ?
                StringDataChunk ddc = new UncompressedStringDataChunk(0, strings);
                // TODO get compress mode from the metadata sent by the client
                if (tryToCompress) {
                    ddc = ddc.tryToCompress();
                }
                // TODO more types
                // TODO index from client
                TimeSeries timeseries = new StringTimeSeries(timeseriesMetadataService.getMetadata(index, individualMetadatas, entry.getKey()), List.of(ddc));
                ret.add(timeseries);
            } else {
                throw new RuntimeException("Unsupported read of timeseries type" + entry.getValue().get(0).getClass());
            }
        }
        return ret;
    }

    private void doDelete(UUID uuid) throws Exception {
        try (var conn = datasource.getConnection();
                var ps = conn.prepareStatement(DELETE);
           ) {
            ps.setObject(1, uuid);
            ps.executeUpdate();
        }
    }

    public void delete(UUID uuid) {
        try {
            doDelete(uuid);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
