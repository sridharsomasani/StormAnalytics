package udacity.storm.tools;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.URL;
import java.util.HashMap;

import geocode.ReverseGeoCode;


public class CountryLookup implements Serializable {

	private static final long serialVersionUID = -9102878650001058090L;	
	private HashMap<String, String> data;
	String csvFile = "country_geoinfo.csv";
	ReverseGeoCode rgc; 
	
	public int getGeoID(String country){
		if(data.containsKey(country.toUpperCase()))
		{
			return Integer.parseInt(data.get(country.toUpperCase()));
		}
		return 0;
	}
	
	public String getCountryCodeByGeo(double latitude, double longitude)
	{
		return rgc.nearestPlace(latitude, longitude).country_iso_code3;
	}
	
	
	public CountryLookup()
	{
		URL absPath = this.getClass().getResource("/" + csvFile);
		//System.out.println(absPath + "---------------------------------------------> " + csvFile);
		InputStream in = null;
		try {
			in = absPath.openStream();
		} catch (Exception e) {
			e.printStackTrace();
		}
		try {
			rgc = new ReverseGeoCode(in, false);
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
	
	
}
