package ipToLocation;

import java.io.File;
import java.io.IOException;

import com.maxmind.geoip2.exception.GeoIp2Exception;

public class SampleLocationProgram {
	/**
		Created by Niyat Patel
	*/
	public static void main(String[] args) throws IOException, GeoIp2Exception {
		File Database = new File("GeoLite2-City.mmdb");
		String  ip = "128.101.101.101";
		IPtoLocation ip2loc = new IPtoLocation();
		String City = ip2loc.getLocation(Database, ip);
		System.out.println(City);
	}
}
