/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package populationanalyzer;

import org.apache.storm.tuple.Fields;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import populationanalyzer.bolts.AgeIncomeProcessingBolt;
import populationanalyzer.bolts.AgeProcessingBolt;
import populationanalyzer.bolts.GenderProcessingBolt;
import populationanalyzer.bolts.IncomeProcessingBolt;
import populationanalyzer.spout.LineReaderSpout;

import java.sql.Time;

public class PopulationAnalyzer {

	public static void main(String[] args) throws Exception{
		Config config = new Config();
		config.put("inputFile", "inputFile.txt");

		config.setDebug(true);
		config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 200);
//		config.registerMetricsConsumer(org.apache.storm.metric.LoggingMetricsConsumer.class, 1);

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("line-reader-spout", new LineReaderSpout());
		builder.setBolt("age-processing-bolt", new AgeProcessingBolt()).shuffleGrouping("line-reader-spout");
		builder.setBolt("gender-processing-bolt", new GenderProcessingBolt()).shuffleGrouping("line-reader-spout");
		builder.setBolt("income-processing-bolt", new IncomeProcessingBolt()).shuffleGrouping("line-reader-spout");
		builder.setBolt("age-income-processing-bolt", new AgeIncomeProcessingBolt()).shuffleGrouping("income-processing-bolt").shuffleGrouping("age-processing-bolt");
		long before = System.currentTimeMillis();
		TimeTracker.setStartTime(before);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("PopulationAnalyzer", config, builder.createTopology());
//		Thread.sleep(30000);
//		long after = System.currentTimeMillis();
//		System.out.println("\n\n\n\n\nTime : " + (after - before) + " ms");
//		cluster.getTopologyInfo().get_executors().get(0).get_stats().
//		cluster.shutdown();
	}

}