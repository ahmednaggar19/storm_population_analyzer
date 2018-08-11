/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package populationanalyzer.bolts;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import populationanalyzer.TimeTracker;

public class AgeIncomeProcessingBolt implements IRichBolt{
	private OutputCollector collector;
    /** Map that maps a combined range of age-income to count of people in this category.
     *  Key : Range name as string.
     *  Value : Count of people in this category.
     */
	Map<String, Integer> ageIncomeMap;

	/** Map to track tuples from different sources (Bolts).
     *  Key : Tuple Id produced by the Spout.
     *  Value : Age or Income of that tuple, whichever arrived first to the bolt.
     */
    Map<Integer, Integer> tuplesTrackerMap;
        
        @Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
                initializeAgeIncomeMap();
                initializeTuplesTrackerMap();
	}

	@Override
	public void execute(Tuple input) {
//        updateTuplesTrackerMap(input);
        collector.ack(input);
	}
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

	@Override
	public void cleanup() {
            System.out.println("Execution Time : " + TimeTracker.getCurrentRelativeMillis(System.currentTimeMillis()));
//            BufferedWriter out;
//            try {
//                out = new BufferedWriter(new FileWriter("output.txt", true));
//                out.newLine();
//                out.write("Adult 10k-50k : " + ageIncomeMap.get("adult-10k-50k"));
//                out.newLine();
//                out.write("Execution Time : " + TimeTracker.getCurrentRelativeMillis(System.currentTimeMillis()));
//                out.newLine();
//                out.close();
//            } catch (IOException ex) {
//                Logger.getLogger(AgeIncomeProcessingBolt.class.getName()).log(Level.SEVERE, null, ex);
//            }
	}
	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	/**Initializes Tuples Tracker Map.*/
    private void initializeTuplesTrackerMap() {
        tuplesTrackerMap = new HashMap<>();
    }

    /**Initializes Age Income Map an insert supported ranges (keys) .*/
    private void initializeAgeIncomeMap () {
        ageIncomeMap = new HashMap<>();
        ageIncomeMap.put("adult-10k-50k", 0);

    }

    /** Updates the count for the range in which the tuple falls.
     * @param  age  age of the person
     * @param  income income of the person
     * */
    private void updateAgeIncomeMap(Integer age, Integer income){
        String ageIncomeRange = null;
        Integer count;
        if (age < 50 && age > 24 && income > 10000 && income < 50000) {
            ageIncomeRange = "adult-10k-50k";
            count = ageIncomeMap.get(ageIncomeRange);
            ageIncomeMap.put(ageIncomeRange, count + 1);
        }
    }

    /** Informs the tuple tracker that a tuple has arrived so that it checks
     * whether the age and income are now available and we can update ageIncomeMap.
     * @param  tuple tuple containing id and one of age or income.
     * */
    private void updateTuplesTrackerMap(Tuple tuple) {
        // check if an age or income already exists in the map for that tuple.
        if (tuplesTrackerMap.containsKey(tuple.getInteger(1))) {
            // check for source of tuple (input is age or income).
            if (tuple.getSourceComponent() == "age-processing-bolt") { // source = age-processing-bolt
                updateAgeIncomeMap(tuple.getInteger(0), tuplesTrackerMap.get(tuple.getInteger(1)));
            } else {  // source = income-processing-bolt
                updateAgeIncomeMap(tuplesTrackerMap.get(tuple.getInteger(1)), tuple.getInteger(0));
            }
            tuplesTrackerMap.remove(tuple.getInteger(1)); // remove entry from map as it has been processed.
        } else {
            tuplesTrackerMap.put(tuple.getInteger(1), tuple.getInteger(0));
        }
    }


}
