package org.apache.pulsar.io.core.transform;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.avro.generic.GenericData;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.KVRecord;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class RenameFields implements Transformation<GenericObject> {
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
    private class MyKVGenericRecord implements KVRecord
    {
        private final Record record;
        private final Schema keySchema;
        private final Schema valueSchema;
        private final GenericObject genericKeyValue;

        public MyKVGenericRecord(Record record,
                                 Schema keySchema,
                                 Schema valueSchema,
                                 GenericObject keyValue) {
            this.record = record;
            this.keySchema = keySchema;
            this.valueSchema = valueSchema;
            this.genericKeyValue = keyValue;
        }

        @Override
        public Optional<String> getTopicName() {
            return record.getTopicName();
        }

        @Override
        public Optional<String> getKey() {
            KeyValue<GenericObject, GenericObject> keyValue = (KeyValue<GenericObject, GenericObject>) genericKeyValue.getNativeObject();
            return keyValue.getKey() == null
            ? Optional.empty()
            : Optional.of(Base64.getEncoder().encodeToString(keySchema.encode(keyValue.getKey())));
        }

        @Override
        public Schema getSchema() {
            return Schema.KeyValue(keySchema, valueSchema, KeyValueEncodingType.SEPARATED);
        }

        @Override
        public Object getValue() {
            return genericKeyValue;
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

        @Override
        public Schema getKeySchema()
        {
            return keySchema;
        }

        @Override
        public Schema getValueSchema()
        {
            return valueSchema;
        }

        @Override
        public KeyValueEncodingType getKeyValueEncodingType()
        {
            return KeyValueEncodingType.SEPARATED;
        }

        @Override
        public String toString() {
            KeyValue<GenericObject, GenericObject> keyValue = (KeyValue<GenericObject, GenericObject>) genericKeyValue.getNativeObject();
            return keyValue.toString();
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
        LOG.warn("rename sources={} targets={}", sources, targets);
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
    public Record<GenericObject> apply(Record<GenericObject> record) {
        //  update the local schema if obsolete
        Object object = record.getValue();
        Schema schema = record.getSchema();

        LOG.warn("transforming class={} record={} schemaType={} schemaClass={}",
                object == null ? null : object.getClass().getName(), object, schema.getSchemaInfo().getType(), schema.getClass().getName());
        if (schema.getNativeSchema().isPresent()) {
            schema = (Schema) schema.getNativeSchema().get();
        }
        if (object != null && object instanceof org.apache.pulsar.client.api.schema.GenericRecord) {
            object = ((org.apache.pulsar.client.api.schema.GenericRecord)object).getNativeObject();
        }
        LOG.warn("transforming2 class={} record={} schemaType={} schemaClass={}",
                object == null ? null : object.getClass().getName(), object, schema.getSchemaInfo().getType(), schema.getClass().getName());

        if (record.getSchema().getSchemaInfo().getType().equals(SchemaType.KEY_VALUE)) {
            Object value = object == null ? null : ((KeyValue)object).getValue();
            Object key = object == null ? record.getKey().get() : ((KeyValue)object).getKey();

            if (key instanceof org.apache.pulsar.client.api.schema.GenericRecord) {
                key = ((org.apache.pulsar.client.api.schema.GenericRecord)key).getNativeObject();
            }
            if (value != null && value instanceof org.apache.pulsar.client.api.schema.GenericRecord) {
                value = ((org.apache.pulsar.client.api.schema.GenericRecord)value).getNativeObject();
            }
            LOG.warn("transforming3 valueClass={} value={} keyClass={} key={}",
                    value == null ? null : value.getClass().getName(), value,
                    key == null ? null : key.getClass().getName(), key);
            if (value  instanceof GenericData.Record) {
                GenericData.Record inKey = (GenericData.Record) key;
                GenericData.Record inValue = (GenericData.Record) value;
                org.apache.avro.Schema avroSchema = maybeUpdateAvroSchema(inValue.getSchema(), record.getMessage().isPresent() ? record.getMessage().get().getSchemaVersion() : null);
                TreeMap props = new TreeMap<>();
                falltenRecord(props, "", inValue);
                for(int i = 0; i < sources.size(); i++) {
                    Object v = props.remove(sources.get(i));
                    if (v != null)
                        props.put(targets.get(i), v);
                }
                final org.apache.avro.generic.GenericRecord outGenericRecord = rebuidRecord(avroSchema, new LinkedList<>(props.entrySet()), "");
                final Object outKey = key;
                MyKVGenericRecord transformedRecord = new MyKVGenericRecord(record,
                        new AvroSchemaWrapper(inKey.getSchema()),
                        new AvroSchemaWrapper(avroSchema),
                        new GenericObject() {
                            @Override
                            public SchemaType getSchemaType() {
                                return SchemaType.KEY_VALUE;
                            }

                            @Override
                            public Object getNativeObject() {
                                return new KeyValue(new GenericObject()
                                {
                                    @Override
                                    public SchemaType getSchemaType()
                                    {
                                        return SchemaType.AVRO;
                                    }

                                    @Override
                                    public Object getNativeObject()
                                    {
                                        return outKey;
                                    }
                                }, new GenericObject()
                                {
                                    @Override
                                    public SchemaType getSchemaType()
                                    {
                                        return SchemaType.AVRO;
                                    }

                                    @Override
                                    public Object getNativeObject()
                                    {
                                        return outGenericRecord;
                                    }
                                });
                            }
                        });
                LOG.warn("transformedRecord={} class={}", transformedRecord, transformedRecord.getClass().getName());
                return transformedRecord;
            }
        }
        LOG.warn("Unchanged record={}  class={}", record, record.getClass().getName());
        return record;
    }

    @Override
    public boolean test(Record<GenericObject> objectRecord) {
        return true;
    }
}
