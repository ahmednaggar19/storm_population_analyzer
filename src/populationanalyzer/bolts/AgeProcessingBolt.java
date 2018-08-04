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
import org.apache.storm.tuple.Values;

public class AgeProcessingBolt implements IRichBolt{
	private OutputCollector collector;
    /** Predefined Map Keys. */
    private final String CHILD = "child";
    private final String TEEN = "teen";
    private final String ADULT = "adult";
    private final String ELDER = "elder";

    /** Map that maps a range of age to count of people in this category.
     *  Key : Range name as string.
     *  Value : Count of people in this category.
     */
    Map<String, Integer> ageMap;
    /** Integer denoting average age **/
    private Integer averageAge = 0;

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
        initializeAgeMap();
	}
        
	@Override
	public void execute(Tuple input) {
		Integer age = input.getInteger(1);
        averageAge = (averageAge + age) / 2;
        updateAgeMap(age);
        collector.emit(new Values(age, input.getInteger(6)));
		collector.ack(input);
	}
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("Age", "Id"));
	}

	@Override
	public void cleanup() {
//            BufferedWriter out;
//            try {
//                out = new BufferedWriter(new FileWriter("output.txt", true));
//                out.write("Age Average : " + averageAge.toString());
//                out.newLine();
//                for (String key : ageMap.keySet()) {
//                    out.write("Count of " + key + " : " + ageMap.get(key));
//                    out.newLine();
//                }
//                out.close();
//            } catch (IOException ex) {
//                       Logger.getLogger(AgeProcessingBolt.class.getName()).log(Level.SEVERE, null, ex);
//            }
	}
	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

    /**Initializes Age Map an insert supported ranges (keys) .*/
    private void initializeAgeMap () {
        ageMap = new HashMap<>();
        ageMap.put(CHILD, 0);
        ageMap.put(TEEN, 0);
        ageMap.put(ADULT, 0);
        ageMap.put(ELDER, 0);

    }

    /** Updates the count for the range in which the tuple falls.
     * @param  age  age of the person
     * */
    private void updateAgeMap(Integer age){
        String ageRange;
        Integer count;
        if (age < 15) {
            ageRange = CHILD;
        } else if (age < 24) {
            ageRange = TEEN;
        } else if (age < 50) {
            ageRange = ADULT;
        } else {
            ageRange = ELDER;
        }
        count = ageMap.get(ageRange);
        ageMap.put(ageRange, count + 1);
    }
}
