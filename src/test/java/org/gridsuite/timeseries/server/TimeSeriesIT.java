/**
 * Copyright (c) 2022, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.gridsuite.timeseries.server;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.powsybl.commons.json.JsonUtil;
import com.powsybl.timeseries.IrregularTimeSeriesIndex;
import com.powsybl.timeseries.RegularTimeSeriesIndex;
import com.powsybl.timeseries.StoredDoubleTimeSeries;
import com.powsybl.timeseries.StringTimeSeries;
import com.powsybl.timeseries.TimeSeries;
import com.powsybl.timeseries.TimeSeriesDataType;
import com.powsybl.timeseries.TimeSeriesIndex;
import com.powsybl.timeseries.TimeSeriesMetadata;
import com.powsybl.timeseries.UncompressedDoubleDataChunk;

/**
 * @author Jon Schuhmacher <jon.harper at rte-france.com>
 *
 */
@RunWith(SpringRunner.class)
@AutoConfigureMockMvc
@SpringBootTest
public class TimeSeriesIT {

    private static final int LARGE_ROWS = 600;
    private static final int LARGE_COLS = 400;

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper mapper;

    // TODO check more infos in tsgroups getAll
    private String getAllRef(Map<String, List<TimeSeries>> groupsById) throws JsonProcessingException {
        return mapper.writeValueAsString(
            groupsById.entrySet().stream()
                .map(entry -> Map.of("id", entry.getKey()))
                .collect(Collectors.toList())
        );
    }

    // TODO compare more than just the data
    // TODO more types
    private void assertTimeSeriesEquals(List<TimeSeries> tsRefs, String actual) {
        List<TimeSeries> tsGets = TimeSeries.parseJson(actual);

        assertEquals(tsRefs.size(), tsGets.size());
        if (TimeSeriesDataType.DOUBLE.equals(tsRefs.get(0).getMetadata().getDataType())) {
            for (int i = 0; i < tsRefs.size(); i++) {
                StoredDoubleTimeSeries tsRef = (StoredDoubleTimeSeries) tsRefs.get(i);
                StoredDoubleTimeSeries tsGet = (StoredDoubleTimeSeries) tsGets.get(i);
                assertArrayEquals(tsRef.toArray(), tsGet.toArray(), 0);
                assertEquals(tsRef.getMetadata(), tsGet.getMetadata());
            }
        } else if (TimeSeriesDataType.STRING.equals(tsRefs.get(0).getMetadata().getDataType())) {
            for (int i = 0; i < tsRefs.size(); i++) {
                StringTimeSeries tsRef = (StringTimeSeries) tsRefs.get(i);
                StringTimeSeries tsGet = (StringTimeSeries) tsGets.get(i);
                assertArrayEquals(tsRef.toArray(), tsGet.toArray());
                assertEquals(tsRef.getMetadata(), tsGet.getMetadata());
            }
        }
    }

    // TODO try to simplify ?
    private void assertTimeSeriesMetadataEquals(List<TimeSeries> tsRef, String getMetadataJson) throws Exception {
        Map<String, Object> getMetadatasParsed = mapper.readValue(getMetadataJson, Map.class);
        TimeSeriesIndex refIndex = tsRef.get(0).getMetadata().getIndex();
        assertEquals(refIndex.getType(), getMetadatasParsed.get("indexType"));
        assertEquals(mapper.readValue(refIndex.toJson(), Object.class), getMetadatasParsed.get(refIndex.getType()));
        List<TimeSeriesMetadata> refMetadatas = tsRef.stream()
                .map(TimeSeries::getMetadata)
                .collect(Collectors.toList());
        List<Map<String, Object>> getIndividualMetadatas = (List<Map<String, Object>>) getMetadatasParsed.get("metadatas");
        assertEquals(refMetadatas.size(), getIndividualMetadatas.size());
        for (int i = 0; i < getIndividualMetadatas.size(); i++) {
            TimeSeriesMetadata refMetadata = refMetadatas.get(i);
            Map<String, Object> getMetadataParse = getIndividualMetadatas.get(i);
            String refMetadataJson = JsonUtil.toJson(generator -> refMetadata.writeJson(generator));
            Map<String, Object> refMetadataParsed = (Map<String, Object>) mapper.readValue(refMetadataJson, Object.class);
            // expected is the metadata in TimeSeriesMetadata without the index (only individual metadatas)
            refMetadataParsed.remove(refMetadata.getIndex().getType());
            assertEquals(refMetadataParsed, getMetadataParse);
        }
    }

    private Pair<List<TimeSeries>, String> someCols(List<TimeSeries> tsRef, int n) {
        String col = tsRef.stream().map(TimeSeries::getMetadata).map(TimeSeriesMetadata::getName).limit(n)
                .collect(Collectors.joining(","));
        return Pair.of(tsRef.stream().limit(n).collect(Collectors.toList()), col);
    }

    private String testCreateGetTs(List<TimeSeries> tsRef)
            throws Exception, JsonProcessingException, JsonMappingException, UnsupportedEncodingException {
        MvcResult resCreate =
                mockMvc.perform(
                    post("/v1/timeseries-group")
                        .content(TimeSeries.toJson(tsRef))
                ).andExpect(status().isOk())
                .andReturn();
        String createdUuid = (String) mapper.readValue(resCreate.getResponse().getContentAsString(), Map.class).get("id");

        MvcResult resGet = mockMvc.perform(get("/v1/timeseries-group/{uuid}", createdUuid)).andExpect(status().isOk())
                .andReturn();
        String getJson = resGet.getResponse().getContentAsString();
        assertTimeSeriesEquals(tsRef, getJson);

        MvcResult resGetMetadata = mockMvc.perform(get("/v1/timeseries-group/{uuid}/metadata", createdUuid))
                .andExpect(status().isOk()).andReturn();
        String getMetadataJson = resGetMetadata.getResponse().getContentAsString();
        assertTimeSeriesMetadataEquals(tsRef, getMetadataJson);

        // TODO here if we try with 51 instead of 50 we get
        // Caused by: org.postgresql.util.PSQLException: ERROR: cannot pass more than 100 arguments to a function
        // this is for the json_build_object ('a', json_obj->'a', 'd', json_obj->'d , ...) select
        for (int n : List.of(1, 2, tsRef.size() - 1, tsRef.size())
                .stream().map(x -> Math.max(1, Math.min(50, x))).distinct().collect(Collectors.toList())) {
            Pair<List<TimeSeries>, String> pairCols = someCols(tsRef, n);
            String somecols = pairCols.getRight();
            List<TimeSeries> someTimeSeries = pairCols.getLeft();
            MvcResult resGetcol = mockMvc.perform(get("/v1/timeseries-group/{uuid}?col={col}", createdUuid, somecols))
                    .andExpect(status().isOk()).andReturn();
            String getColJson = resGetcol.getResponse().getContentAsString();
            assertTimeSeriesEquals(someTimeSeries, getColJson);
        }

        return createdUuid;
    }

    @Test
    // TODO only one test for now to avoid cleaning up the db
    public void test() throws Exception {
        mockMvc.perform(get("/v1/timeseries-group")).andExpectAll(status().isOk(), content().json("[]"));

        RegularTimeSeriesIndex regularIndex = new RegularTimeSeriesIndex(0, 2, 1);
        List<TimeSeries> tsRef1 = List.of(
            TimeSeries.createDouble("first", regularIndex, 2d, 3d, 4d),
            // this one has tags, little more verbose
            new StoredDoubleTimeSeries(new TimeSeriesMetadata("second", TimeSeriesDataType.DOUBLE, Map.of("unit", "kV"), regularIndex), List.of(new UncompressedDoubleDataChunk(0, new double[] {5d, 6d, 7d})))
        );

        String createdUuid1 = testCreateGetTs(tsRef1);

        MvcResult res = mockMvc.perform(get("/v1/timeseries-group")).andExpectAll(
                status().isOk(),
                content().json(getAllRef(Map.of(createdUuid1, tsRef1)))
        ).andReturn();
        System.out.println(res.getResponse().getContentAsString());

        IrregularTimeSeriesIndex irregularIndex = new IrregularTimeSeriesIndex(new long[] {0, 1, 2 });
        List<TimeSeries> tsRef2 = List.of(
            TimeSeries.createDouble("first", regularIndex, 2d, 3d, 4d),
            TimeSeries.createDouble("second", regularIndex, 5d, 6d, 7d)
        );

        String createdUuid2 = testCreateGetTs(tsRef2);

        mockMvc.perform(get("/v1/timeseries-group")).andExpectAll(
            status().isOk(),
            content().json(
                getAllRef(Map.of(
                    createdUuid1, tsRef1,
                    createdUuid2, tsRef2
                ))
            )
        );

        mockMvc.perform(delete("/v1/timeseries-group/{uuid}", createdUuid1)).andExpect(status().isOk());

        mockMvc.perform(get("/v1/timeseries-group")).andExpectAll(
                status().isOk(),
                content().json(getAllRef(Map.of(createdUuid2, tsRef2)))
        );

        mockMvc.perform(delete("/v1/timeseries-group/{uuid}", createdUuid2)).andExpect(status().isOk());

        mockMvc.perform(get("/v1/timeseries-group")).andExpectAll(
                status().isOk(),
                content().json("[]")
        );

        List<TimeSeries> tsRef3 = List.of(
            TimeSeries.createString("first", regularIndex, "two", "three", "four"),
            TimeSeries.createString("second", regularIndex, "five", "six", "seven")
        );

        String createdUuid3 = testCreateGetTs(tsRef3);

        mockMvc.perform(get("/v1/timeseries-group")).andExpectAll(
                status().isOk(),
                content().json(getAllRef(Map.of(createdUuid3, tsRef3)))
        );

        List<TimeSeries> tsRef4 = List.of(
            TimeSeries.createDouble("first", regularIndex, 2d, 3d, 4d),
            TimeSeries.createDouble("second", irregularIndex, 5d, 6d, 7d)
        );
        mockMvc.perform(
            post("/v1/timeseries-group")
                .content(TimeSeries.toJson(tsRef4))
        ).andExpect(status().isBadRequest());

        RegularTimeSeriesIndex largeRegularIndex = new RegularTimeSeriesIndex(0, LARGE_ROWS - 1, 1);
        List<TimeSeries> tsRefLargeDouble = new ArrayList<>(LARGE_COLS);
        for (int i = 0; i < LARGE_COLS; i++) {
            double[] values = new double[LARGE_ROWS];
            for (int j = 0; j < LARGE_ROWS; j++) {
                values[j] = i * LARGE_ROWS + j;
            }
            tsRefLargeDouble.add(TimeSeries.createDouble("large" + i, largeRegularIndex, values));
        }
        String createdUuidLargeDouble = testCreateGetTs(tsRefLargeDouble);
        List<TimeSeries> tsRefLargeString = new ArrayList<>(LARGE_ROWS);
        for (int i = 0; i < LARGE_COLS; i++) {
            String[] values = new String[LARGE_ROWS];
            for (int j = 0; j < LARGE_ROWS; j++) {
                values[j] = Integer.toString(i * LARGE_ROWS + j);
            }
            tsRefLargeString.add(TimeSeries.createString("large" + i, largeRegularIndex, values));
        }
        String createdUuidLargeString = testCreateGetTs(tsRefLargeString);
        mockMvc.perform(delete("/v1/timeseries-group/{uuid}", createdUuidLargeDouble)).andExpect(status().isOk());
        mockMvc.perform(delete("/v1/timeseries-group/{uuid}", createdUuidLargeString)).andExpect(status().isOk());
    }

}
