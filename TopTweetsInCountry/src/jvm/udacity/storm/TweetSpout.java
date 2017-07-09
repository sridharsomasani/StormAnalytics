package udacity.storm;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;
import udacity.storm.utils.Constants;

/**
 * A spout that uses Twitter streaming API for continuously
 * getting tweets
 */
public class TweetSpout extends BaseRichSpout
{
  // Twitter API authentication credentials
  String custkey, custsecret;
  String accesstoken, accesssecret;

  // To output tuples from spout to the next stage bolt
  SpoutOutputCollector collector;

  // Twitter4j - twitter stream to get tweets
  TwitterStream twitterStream;

  // Shared queue for getting buffering tweets received
  LinkedBlockingQueue<String> queue = null;
  
  // Class for listening on the tweet stream - for twitter4j
  private class TweetListener implements StatusListener {

    // Implement the callback function when a tweet arrives
    @Override
    public void onStatus(Status status)
    {
      // add the tweet into the queue buffer
    	String geoInfo = "37.7833,122.4167";
    	if(status.getGeoLocation() != null)
    	{
    		//System.out.println(String.format("tweet ====> %s ===== geo =====> %s", status.getText(), status.getGeoLocation()));
    		geoInfo = String.valueOf(status.getGeoLocation().getLatitude()) + "," + String.valueOf(status.getGeoLocation().getLongitude());
       	    queue.offer(status.getText() + Constants.DELIMETER + geoInfo);
    	}else{
    		queue.offer(status.getText() + Constants.DELIMETER + "None,None");
    	}
    }

    @Override
    public void onDeletionNotice(StatusDeletionNotice sdn)
    {
    }

    @Override
    public void onTrackLimitationNotice(int i)
    {
    }

    @Override
    public void onScrubGeo(long l, long l1)
    {
    }

    @Override
    public void onStallWarning(StallWarning warning)
    {
    }

    @Override
    public void onException(Exception e)
    {
      e.printStackTrace();
    }
  };

  /**
   * Constructor for tweet spout that accepts the credentials
   */
  public TweetSpout(
      String                key,
      String                secret,
      String                token,
      String                tokensecret)
  {
    custkey = key;
    custsecret = secret;
    accesstoken = token;
    accesssecret = tokensecret;
  }

  @Override
  public void open(
      Map                     map,
      TopologyContext         topologyContext,
      SpoutOutputCollector    spoutOutputCollector)
  {
    // create the buffer to block tweets
    queue = new LinkedBlockingQueue<String>(1000);
    // save the output collector for emitting tuples
    collector = spoutOutputCollector;


    // build the config with credentials for twitter 4j
    ConfigurationBuilder config =
        new ConfigurationBuilder()
               .setOAuthConsumerKey(custkey)
               .setOAuthConsumerSecret(custsecret)
               .setOAuthAccessToken(accesstoken)
               .setOAuthAccessTokenSecret(accesssecret);

    // create the twitter stream factory with the config
    TwitterStreamFactory fact =
        new TwitterStreamFactory(config.build());

    // get an instance of twitter stream
    twitterStream = fact.getInstance();    
//    
//    FilterQuery tweetFilterQuery = new FilterQuery(); // See 
//    tweetFilterQuery.locations(new double[][]{new double[]{-124.848974,24.396308},
//                    new double[]{-66.885444,49.384358
//                    }}); 
//    tweetFilterQuery.language(new String[]{"en"});

    
 
    
    // provide the handler for twitter stream
    twitterStream.addListener(new TweetListener());

    //twitterStream.filter(tweetFilterQuery);

    // start the sampling of tweets
    twitterStream.sample();
  
  }

  @Override
  public void nextTuple()
  {
    // try to pick a tweet from the buffer
    String ret = queue.poll();
    String geoInfo;
    String originalTweet;
    // if no tweet is available, wait for 50 ms and return
    if (ret==null)
    {
      Utils.sleep(50);
      return;
    }
    else
    {
        geoInfo = ret.split(Constants.DELIMETER)[1];
        originalTweet = ret.split(Constants.DELIMETER)[0];
        collector.emit(new Values(originalTweet, geoInfo));
    }
    
  }

  @Override
  public void close()
  {
    // shutdown the stream - when we are going to exit
    twitterStream.shutdown();
  }

  /**
   * Component specific configuration
   */
  @Override
  public Map<String, Object> getComponentConfiguration()
  {
    // create the component config
    Config ret = new Config();

    // set the parallelism for this spout to be 1
    ret.setMaxTaskParallelism(1);

    return ret;
  }

  @Override
  public void declareOutputFields(
      OutputFieldsDeclarer outputFieldsDeclarer)
  {
    // tell storm the schema of the output tuple for this spout
    // tuple consists of a single column called 'tweet'
    outputFieldsDeclarer.declare(new Fields("tweet", "location"));
  }
}
