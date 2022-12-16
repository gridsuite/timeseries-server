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
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
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

    public void writeIndex(TimeSeriesIndex index, JsonGenerator generator) {
        index.writeJson(generator);
    }

    public String indexToJson(TimeSeriesIndex index) {
        return JsonUtil.toJson(generator -> {
            writeIndex(index, generator);
        });
    }

    private TimeSeriesIndex readIndex(String indexType, JsonParser parser) {
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
    }

    public TimeSeriesIndex indexFromJson(String indexType, String indexJson) {
        return JsonUtil.parseJson(indexJson, parser -> {
            return readIndex(indexType, parser);
        });
    }

    // TODO proper modeling of metadatas
    // TODO this is a trimmed down version of TimeSeriesMetadata::writeJson
    // with only the fields that we keep for each timeseries in the group
    private void writeOneIndividualMetadatas(TimeSeriesMetadata metadata, JsonGenerator generator) throws IOException {
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

    private void writeIndividualMetadatas(List<TimeSeriesMetadata> metadatas, JsonGenerator generator) throws IOException {
        generator.writeStartArray();
        for (TimeSeriesMetadata metadata : metadatas) {
            writeOneIndividualMetadatas(metadata, generator);
        }
        generator.writeEndArray();
    }

    private void writeIndividualTimeSeriesMetadatas(List<TimeSeries> timeseries, JsonGenerator generator) throws IOException {
        writeIndividualMetadatas(timeseries.stream().map(TimeSeries::getMetadata).collect(Collectors.toList()), generator);
    }

    public String individualTimeseriesMetadatasToJson(List<TimeSeries> timeseries) {
        return JsonUtil.toJson(generator -> {
            try {
                writeIndividualTimeSeriesMetadatas(timeseries, generator);
            } catch (IOException e) {
                throw new RuntimeException("Error serializing metadatas", e);
            }
        });
    }

    // we use objectMapper directly but should we avoid it ?
    public List<Map<String, Object>> individualMetadatasListFromJson(String metadatas) {
        try {
            return objectMapper.readValue(metadatas, List.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Error restoring individual metadatas", e);
        }
    }

    // we use objectMapper directly but should we avoid it ?
    public Map<String, Object> individualMetadatasMapFromJson(String metadatas) {
        List<Map<String, Object>> list = individualMetadatasListFromJson(metadatas);
        return list.stream().collect(Collectors.toMap(map -> (String) map.get("name"), Function.identity(),
                (e1, e2) -> e1, LinkedHashMap::new));
    }

    private TimeSeriesMetadata timeSeriesMetadataFromParsed(TimeSeriesIndex index,
            Map<String, Object> individualMetadata) {
        Map<String, String> tags = (Map) ((List) individualMetadata.get("tags")).stream()
                .collect(Collectors.toMap(map -> ((Map) map).keySet().iterator().next(),
                    map -> ((Map) map).values().iterator().next())); // TODO why using a list of single valued objects here...
        return new TimeSeriesMetadata((String) individualMetadata.get("name"),
                TimeSeriesDataType.valueOf((String) individualMetadata.get("dataType")),
                tags, index);
    }

    // we use objectMapper directly but should we avoid it ?
    public TimeSeriesMetadata getMetadata(TimeSeriesIndex index, Map<String, Object> individualMetadatas, String name) {
        Map<String, Object> individualMetadata = (Map) individualMetadatas.get(name);
        return timeSeriesMetadataFromParsed(index, individualMetadata);
    }

    public List<TimeSeriesMetadata> timeseriesMetadataListFromJson(
            TimeSeriesIndex index,
            String individualMetadatasJson) {
        return individualMetadatasListFromJson(individualMetadatasJson).stream()
                .map(m -> timeSeriesMetadataFromParsed(index, m))
                .collect(Collectors.toList());
    }

    public String allMetadatasToJson(UUID uuid, TimeSeriesIndex index, List<TimeSeriesMetadata> individualMetadatasList) {
        return JsonUtil.toJson(generator -> {
            try {
                generator.writeStartObject();
                generator.writeStringField("id", uuid.toString());
                generator.writeStringField("indexType", index.getType());
                generator.writeFieldName(index.getType());
                writeIndex(index, generator);
                generator.writeFieldName("metadatas");
                writeIndividualMetadatas(individualMetadatasList, generator);
                generator.writeEndObject();
            } catch (IOException e) {
                throw new RuntimeException("Error serializing metadatas", e);
            }
        });
    }

}
