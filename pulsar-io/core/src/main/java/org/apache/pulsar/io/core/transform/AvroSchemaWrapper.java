package org.apache.pulsar.io.core.transform;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.client.api.schema.SchemaInfoProvider;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Optional;

public class AvroSchemaWrapper implements org.apache.pulsar.client.api.Schema<GenericObject> {

    private final SchemaInfo schemaInfo;
    private final org.apache.avro.Schema nativeAvroSchema;

    public AvroSchemaWrapper(org.apache.avro.Schema nativeAvroSchema) {
        this.nativeAvroSchema = nativeAvroSchema;
        this.schemaInfo = SchemaInfo.builder()
                .schema(nativeAvroSchema.toString(false).getBytes(StandardCharsets.UTF_8))
                .properties(new HashMap<>())
                .type(SchemaType.AVRO)
                .name(nativeAvroSchema.getName())
                .build();
    }

    @Override
    public byte[] encode(GenericObject genericObject) {
        return serialize((GenericRecord) genericObject.getNativeObject(), this.nativeAvroSchema);
    }

    public static byte[] serialize(GenericRecord record, org.apache.avro.Schema schema)
    {
        try
        {
            SpecificDatumWriter<GenericRecord> datumWriter = new SpecificDatumWriter<>(schema);
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            BinaryEncoder binaryEncoder = new EncoderFactory().binaryEncoder(byteArrayOutputStream, null);
            datumWriter.write(record, binaryEncoder);
            binaryEncoder.flush();
            return byteArrayOutputStream.toByteArray();
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static GenericRecord  deserialize(byte[] recordBytes, org.apache.avro.Schema schema) throws IOException {
        DatumReader<GenericRecord> datumReader = new SpecificDatumReader(schema);
        ByteArrayInputStream stream = new ByteArrayInputStream(recordBytes);
        BinaryDecoder binaryDecoder = new DecoderFactory().binaryDecoder(stream, null);
        return datumReader.read(null, binaryDecoder);
    }

    @Override
    public SchemaInfo getSchemaInfo() {
        return schemaInfo;
    }

    @Override
    public AvroSchemaWrapper clone() {
        return new AvroSchemaWrapper(nativeAvroSchema);
    }

    @Override
    public void validate(byte[] message) {
        // nothing to do
    }

    @Override
    public boolean supportSchemaVersioning() {
        return true;
    }

    @Override
    public void setSchemaInfoProvider(SchemaInfoProvider schemaInfoProvider) {

    }

    @Override
    public GenericObject decode(byte[] bytes) {
        throw new UnsupportedOperationException();
    }

    @Override
    public GenericObject decode(byte[] bytes, byte[] schemaVersion) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean requireFetchingSchemaInfo() {
        return true;
    }

    @Override
    public void configureSchemaInfo(String topic, String componentName, SchemaInfo schemaInfo) {

    }

    @Override
    public Optional<Object> getNativeSchema() {
        return Optional.of(nativeAvroSchema);
    }
}
