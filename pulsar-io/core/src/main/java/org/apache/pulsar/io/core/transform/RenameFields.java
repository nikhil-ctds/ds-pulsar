package org.apache.pulsar.io.core.transform;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.avro.generic.GenericData;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class RenameFields implements Transformation<Object> {
    private static final Logger LOG = LoggerFactory.getLogger(RenameFields.class);

    List<String> sources = new ArrayList<>();
    List<String> targets = new ArrayList<>();

    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    class SchemaAndVersion {
        Schema schema;
        byte[] schemaVersion;
        org.apache.avro.Schema keyOrValueAvroSchema;
    }

    // record wrapper with the replaced schema and value
    private class MyRecord implements Record
    {
        private final Record record;
        private final Schema schema;
        private final Optional<String> key;
        private final Object value;

        public MyRecord(Record record, Schema schema, Optional<String> key, Object value) {
            this.record = record;
            this.schema = schema;
            this.key = key;
            this.value = value;
        }

        @Override
        public Optional<String> getTopicName() {
            return record.getTopicName();
        }

        @Override
        public Optional<String> getKey() {
            return key;
        }

        public Schema getSchema() {
            return schema;
        }

        @Override
        public Object getValue() {
            return value;
        }

        /**
         * Acknowledge that this record is fully processed.
         */
        public void ack() {
            record.ack();
        }

        /**
         * To indicate that this record has failed to be processed.
         */
        public void fail() {
            record.fail();
        }

        /**
         * To support message routing on a per message basis.
         *
         * @return The topic this message should be written to
         */
        public Optional<String> getDestinationTopic() {
            return record.getDestinationTopic();
        }

        public Optional<Message> getMessage() {
            return record.getMessage();
        }
    }

    /**
     * Last processed output pulsar Schema
     */
    SchemaAndVersion lastSchemaAndVersion;

    @Override
    public void init(Map<String, Object> config) throws Exception
    {
        if (config.containsKey("renames")) {
            for(String rename : ((String)config.get("renames")).split(",")) {
                String[] parts = rename.split(":");
                if (parts.length == 2 && parts[0].length() > 0 && parts[1].length() > 0) {
                    sources.add(parts[0]);
                    targets.add(parts[1]);
                }
            }
        }
        LOG.debug("rename sources={} targets={}", sources, targets);
    }

    /**
     * Build the output schema
     * @param inputSchema
     * @return
     */
    org.apache.avro.Schema maybeUpdateAvroSchema(org.apache.avro.Schema inputSchema, byte[] schemaVersion) {
        TreeMap<String, org.apache.avro.Schema> props = new TreeMap<>();
        flattenSchema(props, "", inputSchema);
        for(int i = 0; i < sources.size(); i++)
        {
            org.apache.avro.Schema fieldSchema = props.remove(sources.get(i));
            if (fieldSchema != null)
                props.put(targets.get(i), fieldSchema);
        }
        props.remove("");
        return rebuildSchema(inputSchema, new LinkedList<>(props.entrySet()), "", new HashMap<>());
    }

    TreeMap<String, org.apache.avro.Schema> flattenSchema(TreeMap<String, org.apache.avro.Schema> map, String path, org.apache.avro.Schema schema) {
        if (schema.getType().equals(org.apache.avro.Schema.Type.RECORD)) {
            map.put(path, schema);
            for (org.apache.avro.Schema.Field field : schema.getFields()) {
                org.apache.avro.Schema fieldSchema = field.schema();
                String key = path.length() > 0 ? path + "." + field.name() : field.name();
                flattenSchema(map, key, fieldSchema);
            }
        } else {
            map.put(path, schema);
        }
        return map;
    }

    org.apache.avro.Schema rebuildSchema(org.apache.avro.Schema rootSchema, List<Map.Entry<String, org.apache.avro.Schema>> entries, String path, Map<String, org.apache.avro.Schema> fields) {
        while(!entries.isEmpty()) {
            Map.Entry<String, org.apache.avro.Schema> entry = entries.get(0);
            if (!entry.getKey().startsWith(path))
                // end node
                break;

            entries.remove(0);
            String subpath = entry.getKey().substring(path.isEmpty() ? 0 : path.length() + 1);
            if (subpath.contains(".")) {
                // start subnode
                String subfield = subpath.substring(0, subpath.indexOf("."));
                fields.put(subfield, rebuildSchema(rootSchema, entries, subfield, new HashMap<>()));
            } else {
                // add field
                fields.put(subpath, entry.getValue().getType().equals(org.apache.avro.Schema.Type.RECORD)
                        ? rebuildSchema(rootSchema, entries, subpath, new HashMap<>())
                        : entry.getValue());
            }
        }
        return org.apache.avro.Schema.createRecord(path.isEmpty() ? "root" : path, "", rootSchema.getNamespace(), rootSchema.isError(),
                fields.entrySet().stream()
                        .map(e -> new org.apache.avro.Schema.Field(e.getKey(), e.getValue()))
                        .collect(Collectors.toList()));
    }

    TreeMap<String, Object> falltenRecord(TreeMap<String, Object> map, String path, org.apache.avro.generic.GenericRecord genericRecord) {
        for(org.apache.avro.Schema.Field field : genericRecord.getSchema().getFields()) {
            Object value = genericRecord.get(field.name());
            String key = path.length() > 0 ? path + "." + field.name() : field.name();
            if (value instanceof org.apache.avro.generic.GenericRecord) {
                falltenRecord(map, key, (org.apache.avro.generic.GenericRecord) value);
            } else {
                map.put(key, value);
            }
        }
        return map;
    }

    org.apache.avro.generic.GenericRecord rebuidRecord(org.apache.avro.Schema schema, List<Map.Entry<String, Object>> entries, String path) {
        org.apache.avro.generic.GenericRecordBuilder genericRecordBuilder = new org.apache.avro.generic.GenericRecordBuilder(schema);
        while(!entries.isEmpty()) {
            Map.Entry<String, Object> entry = entries.get(0);
            if (!entry.getKey().startsWith(path))
                break;

            String subpath = entry.getKey().substring(path.isEmpty() ? 0 : path.length() + 1);
            if (subpath.contains(".")) {
                // add subnode
                String subfield = subpath.substring(0, subpath.indexOf("."));
                genericRecordBuilder.set(subfield, rebuidRecord(schema.getField(subfield).schema(), entries, subfield));
            } else {
                // add field
                entries.remove(0);
                genericRecordBuilder.set(subpath, entry.getValue());
            }
        }
        return genericRecordBuilder.build();
    }

    @Override
    public Record apply(Record<Object> record) {
        //  update the local schema if obsolete
        Object object = record.getValue();
        if (object  instanceof GenericData.Record) {
            GenericData.Record input = (GenericData.Record) object;
            org.apache.avro.Schema avroSchema = maybeUpdateAvroSchema(input.getSchema(), record.getMessage().isPresent() ? record.getMessage().get().getSchemaVersion() : null);
            TreeMap props = new TreeMap<>();
            falltenRecord(props, "", input);
            for(int i = 0; i < sources.size(); i++) {
                Object value = props.remove(sources.get(i));
                if (value != null)
                    props.put(targets.get(i), value);
            }
            org.apache.avro.generic.GenericRecord outGenericRecord = rebuidRecord(avroSchema, new LinkedList<>(props.entrySet()), "");
            return new MyRecord(record, new AvroSchemaWrapper(avroSchema), record.getKey(), outGenericRecord);
        }
        return record;
    }

    @Override
    public void close() {

    }

    @Override
    public boolean test(Record<Object> objectRecord) {
        return true;
    }
}
