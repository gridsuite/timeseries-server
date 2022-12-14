/**
 * Copyright (c) 2021, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.gridsuite.timeseries.server;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.powsybl.timeseries.StoredDoubleTimeSeries;
import com.powsybl.timeseries.TimeSeries;

import static org.junit.Assert.*;

/**
 * @author Jon Schuhmacher <jon.harper at rte-france.com>
 * 
 *         adapted from
 *         https://www.baeldung.com/spring-boot-testcontainers-integration-test
 */

@RunWith(SpringRunner.class)
@AutoConfigureMockMvc
@SpringBootTest
public class TimeSeriesIT {

    private static final String refJson = ""
            + "[ {\n"
            + "    \"metadata\" : {\n"
            + "      \"name\" : \"first\",\n"
            + "      \"dataType\" : \"DOUBLE\",\n"
            + "      \"tags\" : [ ],\n"
            + "      \"irregularIndex\" : [ 0, 1, 2 ]\n"
            + "    },\n"
            + "    \"chunks\" : [ {\n"
            + "      \"offset\" : 0,\n"
            + "      \"uncompressedLength\" : 3,\n"
            + "      \"stepValues\" : [ 1.0, 2.0 ],\n"
            + "      \"stepLengths\" : [ 2, 1 ]\n"
            + "    } ]\n"
            + "  }, {\n"
            + "    \"metadata\" : {\n"
            + "      \"name\" : \"second\",\n"
            + "      \"dataType\" : \"DOUBLE\",\n"
            + "      \"tags\" : [ ],\n"
            + "      \"irregularIndex\" : [ 0, 1, 2 ]\n"
            + "    },\n"
            + "    \"chunks\" : [ {\n"
            + "      \"offset\" : 0,\n"
            + "      \"uncompressedLength\" : 3,\n"
            + "      \"stepValues\" : [ 3.0, 4.0 ],\n"
            + "      \"stepLengths\" : [ 1, 2 ]\n"
            + "    } ]\n"
            + "  }\n"
            + "]\n"
    ;

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper mapper;

    @Test
    public void test() throws Exception {
        mockMvc.perform(get("/v1/timeseries-group")).andExpect(status().isOk());
        MvcResult resCreate = mockMvc.perform(post("/v1/timeseries-group").content(refJson)).andExpect(status().isOk())
                .andReturn();
        String createdUuid = mapper.readValue(resCreate.getResponse().getContentAsString(), String.class);
        MvcResult resGet = mockMvc.perform(get("/v1/timeseries-group/{uuid}", createdUuid)).andExpect(status().isOk())
                .andReturn();
        String getJson = resGet.getResponse().getContentAsString();
        List<TimeSeries> tsRef = TimeSeries.parseJson(refJson);
        List<TimeSeries> tsGet = TimeSeries.parseJson(getJson);

        assertEquals(tsRef.size(), tsGet.size());
        for (int i = 0; i < tsRef.size(); i++) {
            assertArrayEquals(((StoredDoubleTimeSeries) tsRef.get(i)).toArray(),
                    ((StoredDoubleTimeSeries) tsGet.get(i)).toArray(), 0);
        }
    }
}
