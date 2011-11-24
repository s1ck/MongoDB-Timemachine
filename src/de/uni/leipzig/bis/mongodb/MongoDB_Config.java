package de.uni.leipzig.bis.mongodb;

public class MongoDB_Config {
	/**
	 * Database specific settings
	 */
	public static final String HOST = "localhost";
	public static final Integer PORT = 27017;
	public static final String DB = "BIS_mongo_eval";
	public static final String COLLECTION_MEASURINGS = "measurings";
	public static final String COLLECTION_STATIONS = "stations";
	public static final Integer BUFFER_SIZE = 20000;

	/**
	 * Value identifiers (should be as short as possible)
	 */
	public static final String TIMESTAMP = "timestamp";
	public static final String VALUE = "value";
	public static final String STATION_ID = "stationID";
	public static final String PART_ID = "partID";
	public static final String SERIAL_NO = "serialNo";
	public static final String DATATYPE = "datatype";
	public static final String OPT_STRING = "opt_string";
	public static final String SERIAL_NO_2 = "serialNo2";
	
//	public static final String TIMESTAMP = "a";
//	public static final String VALUE = "b";
//	public static final String STATION_ID = "c";
//	public static final String PART_ID = "d";
//	public static final String SERIAL_NO = "e";
//	public static final String DATATYPE = "f";
//	public static final String OPT_STRING = "g";
//	public static final String SERIAL_NO_2 = "h";
			
	/**
	 * Datatypes
	 */
	public static final String PDC = "pdc";
	public static final String UDC = "udc";
	public static final String GAIN = "gain";
		
	/**
	 * Data paths
	 */
	public static final String PATH_380K = "data/380K.csv";
	public static final String PATH_6M = "data/6mio.csv";
	public static final String PATH_30M = "data/30nio.csv";

	/**
	 * Eval specific
	 */
	
	/*
	 * Defines the number of warmup runs
	 */
	public static final int SKIPS = 10;
	/*
	 * Defines the number of test runs
	 */
	public static final int RUNS = 100;
	
	/**
	 * Represents the datatypes of the stored values
	 */
	public static enum DataType {
		UDC, PDC, PAC, TEMP, GAIN
	}
}
