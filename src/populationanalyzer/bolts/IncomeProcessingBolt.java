/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package populationanalyzer.bolts;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
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
import populationanalyzer.bolts.utils.Range;

public class IncomeProcessingBolt implements IRichBolt{
	private OutputCollector collector;
	/** Predefined Map Keys. */
	private final String K1_10K =  "1K_10K";
    private final String K10_50K =  "10K_50K";
    private final String K50_100K =  "50K_100K";

    /** Map that maps a range of income to count of people in this category.
     *  Key : Range name as string.
     *  Value : Count of people in this category.
     */
    Map<String, Integer> incomeMap;
    /** Integer denoting average age **/
    private Integer averageIncome = 0;

    private Range range;

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
        initializeIncomeMap();
        ArrayList<Integer> rangePoints = new ArrayList<>();
        rangePoints.add(1000);
        rangePoints.add(10000);
        rangePoints.add(50000);
        rangePoints.add(100000);
        range = new Range(rangePoints);
	}

	@Override
	public void execute(Tuple input) {
		Integer income = input.getInteger(2);
        averageIncome = (averageIncome + income) / 2;
        updateIncomeMap(income);
        range.updateRangeCount(income);
        collector.emit(new Values(income, input.getInteger(6)));
		collector.ack(input);
	}
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("Income", "Id"));
	}

	@Override
	public void cleanup() {
//            BufferedWriter out;
//            try {
//                out = new BufferedWriter(new FileWriter("output.txt", true));
//                out.newLine();
//                out.write("Income Average : " + averageIncome.toString());
//                out.newLine();
//                Map<String, Integer> rangeMap= range.getMap();
//                for (String key : rangeMap.keySet()) {
//                    out.write("Count of " + key + " : " + rangeMap.get(key));
//                    out.newLine();
//                }
//                    out.close();
//            } catch (IOException ex) {
//                       Logger.getLogger(IncomeProcessingBolt.class.getName()).log(Level.SEVERE, null, ex);
//            }
	}
	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

    private void initializeIncomeMap () {
        incomeMap = new HashMap<>();
        incomeMap.put(K1_10K, 0);
        incomeMap.put(K10_50K, 0);
        incomeMap.put(K50_100K, 0);
    }

    private void updateIncomeMap(Integer income){
        String incomeRange;
        Integer count;
        if (income < 10000) {
            incomeRange = K1_10K;
        } else if (income < 50000){
            incomeRange = K10_50K;
        } else {
            incomeRange = K50_100K;
        }
//        count = incomeMap.get(incomeRange);
//        incomeMap.put(incomeRange, count + 1);
    }
}
