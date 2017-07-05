package udacity.storm;

import java.util.Map;

import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import udacity.storm.utils.Constants;

/**
 * A bolt that prints the word and count to redis
 */
public class ReportBolt extends BaseRichBolt
{
  /**
	 * 
	 */
	private static final long serialVersionUID = 3512746162413977574L;
// place holder to keep the connection to redis
  transient RedisConnection<String,String> redis;
  
  @Override
  public void prepare(
      Map                     map,
      TopologyContext         topologyContext,
      OutputCollector         outputCollector)
  {
    // instantiate a redis connection
    RedisClient client = new RedisClient("localhost",6379);
    // initiate the actual connection
    redis = client.connect();
  }

  @Override
  public void execute(Tuple tuple)
  {
	
//	  uncomment to test without top tweet bolt
//	  String country_id = tuple.getStringByField("country-id");
//	  HashtagWrapper tagsCount = (HashtagWrapper) tuple.getValueByField("rankings");
//	  Map<String, Long> rankings = tagsCount.getRankings();
//	  for(String tag: rankings.keySet()){
//		  redis.publish("WordCountTopology", country_id + Constants.DELIMETER + tag + Constants.DELIMETER + rankings.get(tag) + "|" + "30");
//	  }
	  
	  String country_id = tuple.getStringByField("country-id");
	  String rank = tuple.getStringByField("rank");
	  String tweet = tuple.getStringByField("tweet");
	  
	  redis.publish("WordCountTopology", country_id + Constants.DELIMETER + tweet + Constants.DELIMETER + rank );
    
  }

  public void declareOutputFields(OutputFieldsDeclarer declarer)
  {
    // nothing to add - since it is the final bolt
  }
}
