# Twitter Real-Time Analytics with Apache Storm
## Country wise top tweets
	Final project for Udacity Twitter Real-Time Analytics with Apache Storm


This project helps to view top tweets in each country using real time Twitter data. Twitter4j is used to read stream data
from Twitter and it is the input to Apache Storm topology.

Storm parses and maps location information from each tweet to its country and calculates top hashtags from each country.
It publishes top tags and tweets to redis pub-sub channel. 

Python flask server is used to bridge data analyzed from Storm to visualize it on d3js Choropleth world map. 
The d3js world Choropleth map shows top tweets in each country when hovered on particular country.

---
Setup
---
- Clone the repository
- Install vagrant and virtualbox 
- Open command prompt in the above cloned repository

Enter following command:
```
vagrant up
	It downloads all image and sets up the system for the first time
vagrant ssh 		// logs into the vm
cd /vagrant/		// you should see all your host system folders
```

- Sign up for twitter developer account and register to access twitter api
- Fill in your OAuth details in [TopNTweetTopology.java](./TopTweetsInCountry/src/jvm/udacity/storm/TopNTweetTopology.java) file

Enter following commands to compile
```
cd TopTweetsInCountry 		// assuming you are in root folder of the current repository
mvn package			// compiles and builds jar file in target folder in the current directory
```


Commands to submit topology to Storm:
```
storm jar target/TopTweetsInCountry-0.0.1-SNAPSHOT-jar-with-dependencies.jar udacity.storm.TopNTweetTopology
```

To view d3 World Choropleth visualization:
- Change directory to `TopTweetsinCountry/viz` folder
```
cd TopTweetsinCountry/viz 			// assuming current directory is repository root
```
- Start flask server using below command
```
python app.py
```
- Open browser in host machine and enter below url
```
http://localhost:5000/map
```
Note: Refer to vagrant file for port mapping

Note: The above vagrant file is taken from: https://github.com/udacity/ud381


---
### Screenshots of d3js World Choropleth map

![screenshot](screens/d3.JPG?raw=true "d3js World Choropleth Map")
--
![screenshot](screens/us.jpg?raw=true "d3js USA Choropleth Map")
--
![screenshot](screens/russia.jpg?raw=true "d3js Russia Choropleth Map")




