package co.enydata.tutorial.beam.example1;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/**
 * @author #training <eadjomo@enydata.co> on 07/12/2019
 */
public class SalesPerCarsBrand{
    public static final void main(String args[]) throws Exception {
        Pipeline pipeline = Pipeline.create();

        PCollection<String> input;
        input = pipeline.apply(TextIO.read().from(args[0]));

        PCollection<KV<String, Integer>> parseAndConvertToKV =
                input.apply(
                        "ParseAndConvertToKV",
                        MapElements.via(
                                new SimpleFunction<String, KV<String, Integer>>() {

                                    @Override
                                    public KV<String, Integer> apply(String input) {
                                        String[] split = input.split(",");
                                        if (split.length < 4) {
                                            return null;
                                        }
                                        String key = split[1];
                                        Integer value = Integer.valueOf(split[3]);
                                        return KV.of(key, value);
                                    }
                                }));

        PCollection<KV<String, Iterable<Integer>>> kvpCollection =
                parseAndConvertToKV.apply(GroupByKey.<String, Integer>create());

        PCollection<String> sumUpValuesByKey =
                kvpCollection.apply(
                        "SumUpValuesByKey",
                        ParDo.of(
                                new DoFn<KV<String, Iterable<Integer>>, String>() {

                                    @DoFn.ProcessElement
                                    public void processElement(ProcessContext context) {
                                        Integer totalSells = 0;
                                        String brand = context.element().getKey();
                                        Iterable<Integer> sells = context.element().getValue();
                                        for (Integer amount : sells) {
                                            totalSells += amount;
                                        }
                                        context.output(brand + ": " + totalSells);
                                    }
                                }));

        sumUpValuesByKey.apply(TextIO.write().to("/beam/cars_sales_report").withoutSharding());

        pipeline.run();
    }

}
