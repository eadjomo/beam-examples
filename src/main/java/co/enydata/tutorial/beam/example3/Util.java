package co.enydata.tutorial.beam.example3;


import org.apache.beam.sdk.schemas.Schema;

/**
 * @author #training <eadjomo@enydata.co> on 07/12/2019
 */
public abstract class Util {
   public final static Schema schema = Schema.builder()
            .addInt16Field("ID")
            .addStringField("CODE")
            .addStringField("VALUE")
            .addStringField("CREATEDATE")
            .build();

    public  final static String [] headerNames={"ID","CODE","VALUE","CREATEDATE"};
    public final static  char delimiter=',';
}
