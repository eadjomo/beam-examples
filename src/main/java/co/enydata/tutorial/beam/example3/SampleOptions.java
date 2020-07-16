package co.enydata.tutorial.beam.example3;

/**
 * @author #training <eadjomo@enydata.co> on 07/12/2019
 */

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;
public interface SampleOptions extends DataflowPipelineOptions {
    /**
     * Set inuptFile required parameter to a file path or glob. A local path and Google Cloud Storage
     * path are both supported.
     */
    @Description(
            "Set inuptFile required parameter to a file path or glob. A local path and Google Cloud "
                    + "Storage path are both supported.")
    @Validation.Required
    String getInputFile();

    void setInputFile(String value);

    /**
     * Set output required parameter to define output path. A local path and Google Cloud Storage path
     * are both supported.
     */
    @Description(
            "Set output required parameter to define output path. A local path and Google Cloud Storage"
                    + " path are both supported.")
    @Validation.Required
    String getOutput();

    void setOutput(String value);


}
