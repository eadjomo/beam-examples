package co.enydata.tutorial.beam.example3;

import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.channels.Channels;

/**
 * @author #training <eadjomo@enydata.co> on 07/12/2019
 */
public class CsvParser extends DoFn<FileIO.ReadableFile,CSVRecord> {

    @DoFn.ProcessElement
    public void processElement(@Element FileIO.ReadableFile element, OutputReceiver<CSVRecord> receiver) throws IOException {
        InputStream is = Channels.newInputStream(element.open());
        Reader reader = new InputStreamReader(is);
        Iterable<CSVRecord> records = CSVFormat.DEFAULT.withHeader(Util.headerNames).withDelimiter(Util.delimiter).withFirstRecordAsHeader().parse(reader);
        for (CSVRecord record : records) { receiver.output(record); }
    }
}
