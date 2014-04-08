package com.mortardata.recsys;

import com.google.common.collect.ImmutableList;
import gnu.trove.iterator.TObjectFloatIterator;
import gnu.trove.map.hash.TObjectFloatHashMap;
import org.apache.pig.Accumulator;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class FilterItemItemLinksDetailed extends EvalFunc<DataBag> implements Accumulator<DataBag> {
    private static final TupleFactory tf = TupleFactory.getInstance();
    private static final BagFactory bf = BagFactory.getInstance();

    private float minLinkWeight;
    private TObjectFloatHashMap<String> inputItems;
    private Map<String, Map<String, Integer>> inputSignals;
    private DataBag outputItems;

    /**
     * For a single item_A, this UDF takes a bag of weighted item-item links and returns a bag of
     * (item_B, weight, link_data) tuples that are above a minimum weight.
     *
     * The input bag should have a common item_A (results of a group on item_A) so
     * item_A isn't returned because the caller can easily add it back.
     *
     * Input Schema:  { (item_A: chararray, item_B: chararray, weight: float, link_data: map) }
     * Output Schema: { (item_B: chararray, weight: float, link_data: map) }
     *
     * @param minLinkWeight: Any item-item link with a weight less than this will be removed.
     */
    public FilterItemItemLinksDetailed(String minLinkWeight) {
        this.minLinkWeight = Float.parseFloat(minLinkWeight);
        cleanup();
    }

    public Schema outputSchema(Schema input) {
        try {
            ArrayList<Schema.FieldSchema> tupleFields = new ArrayList<Schema.FieldSchema>(2);
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

    public DataBag exec(Tuple input) {
        accumulate(input);
        DataBag output = getValue();
        cleanup();
        return output;
    }



    public void cleanup() {
        inputItems = new TObjectFloatHashMap<String>();
        inputSignals = new HashMap<String, Map<String, Integer>>();
        outputItems = bf.newDefaultBag();
    }

    public DataBag getValue() {
        TObjectFloatIterator<String> it = inputItems.iterator();
        while (it.hasNext()) {
            it.advance();
            if (it.value() >= minLinkWeight) {
                outputItems.add(tf.newTupleNoCopy(
                        ImmutableList.of(it.key(), it.value(), inputSignals.get(it.key()))
                ));

            }
        }

        return outputItems;
    }

    /**
     * @param input:  Bag of (item_A: chararray, item_B: chararray, weight: float, link_data: map) tuples
     *                  with common item_A
     */
    public void accumulate(Tuple input) {
        try {
            DataBag inputBag = (DataBag) input.get(0);
            for (Tuple t : inputBag) {
                String item = (String) t.get(1);
                float weight = (Float) t.get(2);
                Map<String, Integer> link_data = (Map) t.get(3);
                inputItems.adjustOrPutValue(item, weight, weight);
                addToMap(item, link_data);
            }
        } catch (ExecException e) {
            throw new RuntimeException(e);
        }
    }

    private void addToMap(String item, Map<String, Integer> signals) {
        if (inputSignals.containsKey(item)) {
            Map<String, Integer> item_signals = inputSignals.get(item);
            for (String s : signals.keySet()) {
                if (item_signals.containsKey(s)) {
                    item_signals.put(s, item_signals.get(s) + signals.get(s));
                }  else{
                    item_signals.put(s, signals.get(s));
                }
            }
            inputSignals.put(item, item_signals);
        }
        else {
            inputSignals.put(item, signals);
        }

    }
}
