/**
 * Copyright (c) 2022, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.gridsuite.timeseries.server;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.powsybl.commons.json.JsonUtil;
import com.powsybl.timeseries.InfiniteTimeSeriesIndex;
import com.powsybl.timeseries.IrregularTimeSeriesIndex;
import com.powsybl.timeseries.RegularTimeSeriesIndex;
import com.powsybl.timeseries.TimeSeries;
import com.powsybl.timeseries.TimeSeriesDataType;
import com.powsybl.timeseries.TimeSeriesIndex;
import com.powsybl.timeseries.TimeSeriesMetadata;

/**
 * @author Jon Schuhmacher <jon.harper at rte-france.com>
 */
//TODO temporary, will go away we we model metadata directly in the database
@SuppressWarnings("all")
@Service
public class TimeSeriesMetadataService {

    private final ObjectMapper objectMapper;

    public TimeSeriesMetadataService(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public TimeSeriesIndex readIndex(String indexType, String indexJson) {
        return JsonUtil.parseJson(indexJson, parser -> {
            // TODO lifted from TimeSeriesMetadata.parseFieldName
            switch (indexType) {
                case RegularTimeSeriesIndex.TYPE:
                    return RegularTimeSeriesIndex.parseJson(parser);
                case IrregularTimeSeriesIndex.TYPE:
                    return IrregularTimeSeriesIndex.parseJson(parser);
                case InfiniteTimeSeriesIndex.TYPE:
                    return InfiniteTimeSeriesIndex.parseJson(parser);
                default:
                    throw new RuntimeException("unknown index type");
            }
        });
    }

    // TODO proper modeling of metadatas
    // TODO this is a trimmed down version of TimeSeriesMetadata::writeJson
    // with only the fields that we keep for each timeseries in the group
    private void writeIndividualMetadatas(TimeSeriesMetadata metadata, JsonGenerator generator) throws IOException {
        generator.writeStartObject();

        generator.writeStringField("name", metadata.getName());
        generator.writeStringField("dataType", metadata.getDataType().name());

        generator.writeFieldName("tags");
        generator.writeStartArray();
        for (Map.Entry<String, String> e : metadata.getTags().entrySet()) {
            generator.writeStartObject();
            generator.writeStringField(e.getKey(), e.getValue());
            generator.writeEndObject();
        }
        generator.writeEndArray();
        generator.writeEndObject();
    }

    public String gatherIndividualMetadatas(List<TimeSeries> timeseries) {
        return JsonUtil.toJson(generator -> {
            try {
                generator.writeStartArray();
                for (TimeSeries ts : timeseries) {
                    writeIndividualMetadatas(ts.getMetadata(), generator);
                }
                generator.writeEndArray();
            } catch (IOException e) {
                throw new RuntimeException("Error serializing metadatas", e);
            }
        });
    }

    public Map<String, Object> scatterIndividualMetadatas(String metadatas) {
        try {
            List<Map<String, Object>> list = objectMapper.readValue(metadatas, List.class);
            return list.stream().collect(Collectors.toMap(map -> (String) map.get("name"), Function.identity(), (e1,e2) -> e1, LinkedHashMap::new));
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Error restoring individual metadatas", e);
        }
    }

    public TimeSeriesMetadata getMetadata(TimeSeriesIndex index, Map<String, Object> individualMetadatas, String name) {
        Map<String, Object> individualMetadata = (Map) individualMetadatas.get(name);
        Map<String, String> tags = (Map) ((List) individualMetadata.get("tags")).stream()
                .collect(Collectors.toMap(map -> ((Map) map).keySet().iterator().next(),
                    map -> ((Map) map).values().iterator().next())); // TODO why using a list of single valued objects here...
        return new TimeSeriesMetadata(name,
                TimeSeriesDataType.valueOf((String) individualMetadata.get("dataType")),
                tags, index);
    }
}
