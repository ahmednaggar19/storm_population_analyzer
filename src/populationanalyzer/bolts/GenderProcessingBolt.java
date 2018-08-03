/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package populationanalyzer.bolts;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class GenderProcessingBolt implements IRichBolt{
	private OutputCollector collector;
    /** Predefined Map Keys. */
    private final String WOMEN = "Female";
    private final String MEN = "Male";

    /** Map that maps a range of age to count of people in this category.
     *  Key : Range name as string.
     *  Value : Count of people in this category.
     */
    Map<String, Integer> genderMap;

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
        initializeGenderMap();
	}
        
	@Override
	public void execute(Tuple input) {
        String gender = input.getString(4);
        updateGenderMap(gender);
        collector.emit(new Values(gender, input.getInteger(6)));
		collector.ack(input);
	}
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("Gender", "Id"));
	}

	@Override
	public void cleanup() {
            BufferedWriter out;
            try {
                out = new BufferedWriter(new FileWriter("output.txt", true));
                for (String key : genderMap.keySet()) {
                    out.write("Count of " + key + " : " + genderMap.get(key));
                    out.newLine();
                }
                out.close();
            } catch (IOException ex) {
                       Logger.getLogger(GenderProcessingBolt.class.getName()).log(Level.SEVERE, null, ex);
            }
	}
	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

    /**Initializes Gender Map an insert supported ranges (keys) .*/
    private void initializeGenderMap() {
        genderMap = new HashMap<>();
        genderMap.put(WOMEN, 0);
        genderMap.put(MEN, 0);
    }

    /** Updates the count for the range in which the tuple falls.
     * @param  gender gender of the person
     * */
    private void updateGenderMap(String gender){
        Integer count = genderMap.get(gender);
        genderMap.put(gender, count + 1);
    }
}
