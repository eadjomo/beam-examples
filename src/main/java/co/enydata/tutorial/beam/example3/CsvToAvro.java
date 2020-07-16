package co.enydata.tutorial.beam.example3;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.csv.CSVRecord;

import java.sql.Date;
import java.util.List;

/**
 * @author #training <eadjomo@enydata.co> on 07/12/2019
 */
public class CsvToAvro {
    public static void main(String... args) {
        PipelineOptionsFactory.register(SampleOptions.class);
        SampleOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(SampleOptions.class);
        Pipeline pipeline = Pipeline.create(options);

        pipeline
                .apply(FileIO.match().filepattern(options.getInputFile()))
                .apply(FileIO.readMatches())
                .apply(ParDo.of(new CsvParser()))
                .apply(ParDo.of(new DoFn<CSVRecord,GenericRecord>() {
                    @DoFn.ProcessElement
                    public void processElement(@Element CSVRecord element, DoFn.OutputReceiver<GenericRecord> receiver) {
                        GenericRecord genericRecord = new GenericData.Record(AvroUtils.toAvroSchema(Util.schema));
                        List<Schema.Field> fields = (AvroUtils.toAvroSchema(Util.schema)).getFields();

                        for (Schema.Field field : fields) {
                            String fieldType = field.schema().getType().getName().toLowerCase();
                            switch (fieldType) {
                                case "string":
                                    genericRecord.put(field.name(), element.get(field.name().toUpperCase()));
                                    break;
                                case "int":
                                    genericRecord.put(field.name(), Integer.valueOf(element.get(field.name().toUpperCase())));
                                    break;
                                case "long":
                                    genericRecord.put(field.name(), Long.valueOf(element.get(field.name().toUpperCase())));
                                    break;
                                case "datetime":
                                    genericRecord.put(field.name(), Date.valueOf(element.get(field.name().toUpperCase())));
                                    break;
                                default:
                                    throw new IllegalArgumentException("Field type " + fieldType + " is not supported.");
                            }
                        }
                        receiver.output(genericRecord);
                    }

                }))
                .setCoder(AvroCoder.of(GenericRecord.class, AvroUtils.toAvroSchema(Util.schema)))
                .apply("Write Avro formatted data", AvroIO.writeGenericRecords(AvroUtils.toAvroSchema(Util.schema))
                        .to(options.getOutput()).withCodec(CodecFactory.snappyCodec()).withSuffix(".avro"));



        ;

        // run the pipeline and wait until the whole pipeline is finished
        pipeline.run().waitUntilFinish();
    }
}
