package de.uni.leipzig.bis.mongodb;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class MongoDB_Config {
	/**
	 * Database specific settings
	 */
	public static String HOST = "localhost";
	public static Integer PORT = 27020;
	public static String DB = "BIS_mongo_eval";
	public static String COLLECTION_MEASURINGS = "measurings";
	public static String COLLECTION_STATIONS = "stations";
	public static Integer BUFFER_SIZE = 20000;

	/**
	 * DataTypes used in the time series data
	 * 
	 * @author s1ck
	 * 
	 */
	public static enum DataType {
		UDC {
			public String toString() {
				return "udc";
			}
		},
		PDC {
			public String toString() {
				return "pdc";
			}
		},
		PAC {
			public String toString() {
				return "pac";
			}
		},
		TEMP {
			public String toString() {
				return "temp";
			}
		},
		GAIN {
			public String toString() {
				return "gain";
			}
		}
	}

	/**
	 * Value identifiers (should be as short as possible)
	 */
	public static String TIMESTAMP = "timestamp";
	public static String VALUE = "value";
	public static String STATION_ID = "stationID";
	public static String PART_ID = "partID";
	public static String SERIAL_NO = "serialNo";
	public static String DATATYPE = "datatype";
	public static String OPT_STRING = "opt_string";
	public static String SERIAL_NO_2 = "serialNo2";

	/**
	 * Data paths
	 */
	public static String PATH;

	/**
	 * Eval specific
	 */

	/**
	 * Defines the number of global test runs including import and benchmark
	 */
	public static int GLOBAL_RUNS;
	/**
	 * Defines the number of randomized benchmark runs
	 */
	public static int LOCAL_RUNS;
	/**
	 * Range size for the queries
	 */
	public static int DAYS;

	public static void initConfig(String configFile) {
		Properties properties = new Properties();
		try {
			BufferedInputStream stream;
			stream = new BufferedInputStream(new FileInputStream(configFile));
			properties = new Properties();
			properties.load(stream);
			stream.close();
		} catch (FileNotFoundException e) {
			properties = null;
			System.out.println(String.format(
					"Configuration not found at \"%s\".", configFile));
			e.printStackTrace();
		} catch (IOException e) {
			properties = null;
			System.out.println("Configuration read error.");
			e.printStackTrace();
		}

		if (properties != null) {
			// mongodb connection
			HOST = properties.getProperty("mongodb.host");
			PORT = Integer.parseInt(properties.getProperty("mongodb.port"));
			DB = properties.getProperty("mongodb.db");
			COLLECTION_MEASURINGS = properties
					.getProperty("mongodb.collection.measurings");
			COLLECTION_STATIONS = properties
					.getProperty("mongodb.collection.stations");
			BUFFER_SIZE = Integer.parseInt(properties
					.getProperty("mongodb.buffersize"));

			// dataset properties
			PATH = properties.getProperty("data.path");

			TIMESTAMP = properties.getProperty("data.timestamp");
			VALUE = properties.getProperty("data.value");
			STATION_ID = properties.getProperty("data.stationid");
			PART_ID = properties.getProperty("data.wrid");
			SERIAL_NO = properties.getProperty("data.wrid");
			DATATYPE = properties.getProperty("data.datatype");
			OPT_STRING = properties.getProperty("data.optstring");
			SERIAL_NO_2 = properties.getProperty("data.stringid");

			// evaluation properties

			GLOBAL_RUNS = Integer.parseInt(properties
					.getProperty("eval.globalruns"));
			LOCAL_RUNS = Integer.parseInt(properties
					.getProperty("eval.localruns"));
			DAYS = Integer.parseInt(properties.getProperty("eval.days"));
		}
	}

}
