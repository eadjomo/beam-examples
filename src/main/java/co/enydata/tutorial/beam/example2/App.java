package co.enydata.tutorial.beam.example2;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;

/**
 * @author #training <eadjomo@enydata.co> on 07/12/2019
 */
public class App {
    public static void main(String... args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        pipeline
                .apply(TextIO.read().from(args[0]))
                .apply(new CountWords())
                .apply(MapElements.via(new SimpleFunction<KV<String, Long>, String>() {
                    @Override
                    public String apply(KV<String, Long> input) {
                        return String.format("%s: %d", input.getKey(), input.getValue());
                    }
                }))
                .apply(TextIO.write().to("output"))
        ;

        // run the pipeline and wait until the whole pipeline is finished
        pipeline.run().waitUntilFinish();
    }
}
