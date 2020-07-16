package co.enydata.tutorial.beam.example2;

/**
 * @author #training <eadjomo@enydata.co> on 07/12/2019
 */

import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.Arrays;

public class CountWords extends PTransform<PCollection<String>, PCollection<KV<String, Long>>> {
    @Override
    public PCollection<KV<String, Long>> expand(PCollection<String> input) {
        return input
                .apply(MapElements.into(TypeDescriptors.lists(TypeDescriptors.strings())).via(s -> Arrays.asList(s.split(" "))))
                .apply(Flatten.iterables())
                .apply(Count.perElement())
                ;
    }

}
