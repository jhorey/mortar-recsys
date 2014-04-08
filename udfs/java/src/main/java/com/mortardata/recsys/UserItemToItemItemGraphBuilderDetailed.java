package com.mortardata.recsys;

import com.google.common.collect.ImmutableList;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.tools.pigstats.PigStatusReporter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class UserItemToItemItemGraphBuilderDetailed extends EvalFunc<DataBag> {

    private static final TupleFactory tf = TupleFactory.getInstance();
    private static final BagFactory bf = BagFactory.getInstance();

    /**
     * For a single user, takes a bag of weighted user-item links and creates
     * a bag of weighted item-item links.
     *
     * Input Schema:  { (user: chararray, item: chararray, weight: float, signal_types: map) }
     * Output Schema: { (item_A: chararray, item_B: chararray, weight: float, link_data: map) }
     */
    public UserItemToItemItemGraphBuilderDetailed() {}

    public Schema outputSchema(Schema input) {
        try {
            ArrayList<Schema.FieldSchema> tupleFields = new ArrayList<Schema.FieldSchema>(3);
            tupleFields.add(new Schema.FieldSchema("item_A", DataType.CHARARRAY));
            tupleFields.add(new Schema.FieldSchema("item_B", DataType.CHARARRAY));
            tupleFields.add(new Schema.FieldSchema("weight", DataType.FLOAT));
            tupleFields.add(new Schema.FieldSchema("link_data", DataType.MAP));

            return new Schema(
                    new Schema.FieldSchema("ii_terms",
                            new Schema(
                                    new Schema.FieldSchema(null,
                                            new Schema(tupleFields),
                                            DataType.TUPLE)),
                            DataType.BAG)
            );
        } catch (FrontendException e) {
            throw new RuntimeException(e);
        }
    }

    public DataBag exec(Tuple input) throws IOException {
        DataBag inputBag = (DataBag) input.get(0);
        DataBag outputBag = bf.newDefaultBag();
        PigStatusReporter reporter = PigStatusReporter.getInstance();

        //Join the set of input items with itself and create a link
        //for each pairing with the minimum user-item weight for
        //the corresponding items.
        int i = 0;
        for (Tuple u : inputBag) {
            int j = 0;
            for (Tuple v : inputBag) {
                Map<String, Integer> u_signals = (Map) u.get(3);
                Map<String, Integer> v_signals = (Map) v.get(3);
                Map<String, Integer> combinedMap = combineMaps(u_signals, v_signals);

                if (j > i) {
                    Float weight
                            = Math.min(
                            (Float) u.get(2),
                            (Float) v.get(2));


                    outputBag.add(
                            tf.newTupleNoCopy(
                                    ImmutableList.of(
                                            u.get(1),
                                            v.get(1),
                                            weight,
                                            combinedMap
                                    )
                            )
                    );

                    outputBag.add(
                            tf.newTupleNoCopy(
                                    ImmutableList.of(
                                            v.get(1),
                                            u.get(1),
                                            weight,
                                            combinedMap
                                    )
                            )
                    );
                }
                j++;
            }
            i++;

            if (reporter != null) {
                reporter.progress();
            }
        }

        return outputBag;
    }

    private static Map<String, Integer> combineMaps(Map<String, Integer> u_signals, Map<String, Integer> v_signals) {
        Map<String, Integer> combinedSignals = new HashMap<String, Integer>();
        combinedSignals.putAll(v_signals);
        for (String s : u_signals.keySet()) {
            if (combinedSignals.containsKey(s)) {
                int num = combinedSignals.get(s);
                combinedSignals.put(s, num + u_signals.get(s));
            }
            else combinedSignals.put(s, u_signals.get(s));
        }
        combinedSignals.put("NUM_USERS", 1);
        return combinedSignals;
    }
}

