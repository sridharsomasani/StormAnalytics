package udacity.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

/**
 * The main class for the topology. It contains information about topology structure and 
 * how all of the bolts and spouts are tied together.
 *
 */
class TopNTweetTopology
{
  public static void main(String[] args) throws Exception
  {
    //Variable TOP_N number of words
    int TOP_N = 20;
    // create the topology
    TopologyBuilder builder = new TopologyBuilder();

    /*
     * In order to create the spout, you need to get twitter credentials
     * If you need to use Twitter firehose/Tweet stream for your idea,
     * create a set of credentials by following the instructions at
     *
     * https://dev.twitter.com/discussions/631
     *
     */
    // now create the tweet spout with the credentials
    // credential
    TweetSpout tweetSpout = new TweetSpout(
    	      "<<consumer_key>>",
    	      "<<consumer_secret>>",
    	      "<<access_key>>",
    	      "<<access_secret>>"
    	    );
    


    // attach the tweet spout to the topology - parallelism of 1
    builder.setSpout("tweet-spout", tweetSpout, 1);
    
 // attach the parse tweet bolt using shuffle grouping
    builder.setBolt("country-bolt", new ParseCountryBolt(), 10).shuffleGrouping("tweet-spout");

    
    builder.setBolt("parse-tweet-bolt", new ParseTweetBolt(), 10).fieldsGrouping("country-bolt", new Fields("country-id"));
    
    builder.setBolt("country-hashtag-counter-bolt", new CountryHashtagCounter(), 15).fieldsGrouping("parse-tweet-bolt", new Fields("country-id"));
//    builder.setBolt("intermediate-ranker", new PartitionRankingsBolt(TOP_N), 10).fieldsGrouping("country-hashtag-counter-bolt", new Fields("country-id"));
//    builder.setBolt("total-ranker", new PartitionRankingsBolt(TOP_N)).globalGrouping("intermediate-ranker");
    
    builder.setBolt("ranker", new PartitionRankingsBolt(TOP_N), 10).fieldsGrouping("country-hashtag-counter-bolt", new Fields("country-id"));
    
    builder.setBolt("filter-top-tweet", new TopTweetBolt())
    												.globalGrouping("ranker")
    												.shuffleGrouping("country-bolt")
    												.shuffleGrouping("country-bolt","no-geo-info-country-bolt");
    
    builder.setBolt("report-bolt", new ReportBolt(), 1).globalGrouping("filter-top-tweet");
   //builder.setBolt("report-bolt", new ReportBolt(), 1).globalGrouping("total-ranker");

    // create the default config object
    Config conf = new Config();

    // set the config in debugging mode
    conf.setDebug(true);

    if (args != null && args.length > 0) {

      // run it in a live cluster

      // set the number of workers for running all spout and bolt tasks
      conf.setNumWorkers(3);

      // create the topology and submit with config
      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());

    } else {

      // run it in a simulated local cluster

      // set the number of threads to run - similar to setting number of workers in live cluster
      conf.setMaxTaskParallelism(4);

      // create the local cluster instance
      LocalCluster cluster = new LocalCluster();

      // submit the topology to the local cluster
      cluster.submitTopology("tweet-word-count", conf, builder.createTopology());

      // let the topology run for 300 seconds. note topologies never terminate!
      Utils.sleep(300000000);

      // now kill the topology
      cluster.killTopology("tweet-word-count");

      // we are done, so shutdown the local cluster
      cluster.shutdown();
    }
  }
}
