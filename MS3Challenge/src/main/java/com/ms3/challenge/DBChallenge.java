package com.ms3.challenge;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

public class DBChallenge {
	// set table name
	static String tableName = "persons";
	
	static HashMap<Integer, String> map;
	static CSVPrinter csvPrinter;
	static DBChallenge app = new DBChallenge();
	
	static int total_counter;
	static int failed_counter;
	static int successful_counter;

	// before running, delete 'persons' table from testing.db in SQLite to avoid writing records twice
	static String url = "jdbc:sqlite:src/main/resources/testing.db";

	//method to get a connection
	private Connection connect() {
		Connection conn = null;
		try {
			conn = DriverManager.getConnection(url);
		} catch (SQLException e) {
			System.out.println(e.getMessage());
			System.out.println("Issue connecting with db");
		}
		return conn;
	}

	public static void createTable() {
		//creates table with 10 columns A-J
		String sqlCreate = "CREATE TABLE IF NOT EXISTS " + tableName + " (\n" + "	A text,\n" + "	B text,\n"
				+ "	C text,\n" + "	D text,\n" + "	E text,\n" + "	F text,\n" + "	G text,\n" + "	H text,\n"
				+ "	I text,\n" + "	J text\n" + ");";

		//execute statement to create table
		try (Connection conn = DriverManager.getConnection(url); Statement stmt = conn.createStatement()) {
			stmt.execute(sqlCreate);
			System.out.println("Columns created...");
			
			//calls method to parse .csv file
			csvParser();
		} catch (SQLException e) {
			System.out.println(e.getMessage());
			System.out.println("Issue creating table");
		}

	}

	public static void csvParser() {

		//path to .csv file
		String csv = "src/main/resources/ms3Interview.csv";
		BufferedReader reader = null;
		
		String line = "";

		try {

			System.out.println("Reading .csv entries...");
			reader = new BufferedReader(new FileReader(csv));
			
			//this string eats the headerLine so it isn't recorded into DB
			String headerLine = reader.readLine();
			while ((line = reader.readLine()) != null) {

				// regex way to find commas, but ignore commas within double quotes
				String[] array = line.split("(?!\\B\"[^\"]*),(?![^\"]*\"\\B)");
				
				//array to ArrayList for easier manipulation
				List<String> data = new ArrayList<String>(Arrays.asList(array));

				//key is db index
				//value is its corresponding entry
				map = new HashMap<Integer, String>();
 
				//j is the key in the HashMap, it represents the db index
				int j = 1;
				
				//loops 10x (for each 10 columns) 
				for (int i = 0; i <= 9; i++) {

					//if the size of the list isn't equal to the number of columns
					if (data.size() <= i) {
						//keep adding entries that say "bad-data" to the list until size== 10
						data.add(i, "bad-data");
						
						//just realized this counter increments every time "bad-entry" is recorded in db
						//meaning that it doesn't record # of unmatched records
						//it records each time there is an unmatched entry!!! oh no failed counter has failed
						failed_counter++;
					}

					//maps db index to its entry
					map.put(j, data.get(i));
					j++;
					
					total_counter++;
				}

				//insert record to table
				app.insert();
				System.out.println(data);

			}

			System.out.println("\nTesting.db contains all the records now!");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (reader != null) {
				try {
					reader.close();
					//creating a .csv file to record bad entries
					createBadDataCSV();

				} catch (IOException e) {
					e.printStackTrace();
				}
			}

		}

	}

	public void insert() {
		String sqlInsert = "INSERT INTO " + tableName + "(A,B,C,D,E,F,G,H,I,J) VALUES(?,?,?,?,?,?,?,?,?,?)";

		try (Connection conn = this.connect(); PreparedStatement ps = conn.prepareStatement(sqlInsert)) {

			//first arg for setString specifies the question mark placeholder's
			//second arg, the HashMap keys are used to get entries
			for (int i = 1; i <= 10; i++) {
				ps.setString(i, map.get(i));
			}

			ps.executeUpdate();
		} catch (SQLException e) {
			System.out.println(e.getMessage());
			System.out.println("Issue inserting data");
		}
	}

	//simply creates a .csv file, nothing besides header is recorded yet
	public static void createBadDataCSV() throws IOException {

		String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss"));
		String badDataCSV = "src/main/resources/bad-data-" + timestamp + ".csv";
		BufferedWriter writer = Files.newBufferedWriter(Paths.get(badDataCSV));

		csvPrinter = new CSVPrinter(writer,
				CSVFormat.EXCEL.withHeader("A", "B", "C", "D", "E", "F", "G", "H", "I", "J"));
		System.out.println("\nThe bad-data-<timestamp>.csv file has been created.");
		
		//find records that do not match
		app.getBadData();
	}

	public void getBadData() throws IOException {
		//find any entries in db that were marked 'bad-data'
		String sqlSelect = "SELECT * FROM " + tableName + " WHERE " + "(A = 'bad-data' " + "OR  B = 'bad-data' "
				+ "OR C = 'bad-data' " + "OR D = 'bad-data' " + "OR F = 'bad-data' " + "OR G = 'bad-data' "
				+ "OR H = 'bad-data' " + "OR I = 'bad-data' " + "OR J = 'bad-data');";
		try (Connection conn = this.connect(); PreparedStatement ps = conn.prepareStatement(sqlSelect)) {
			ResultSet rs = ps.executeQuery();

			while (rs.next()) {

				//resultset is read into the ArrayList
				ArrayList<String> badDataRecord = new ArrayList<String>();
				for (int i = 1; i <= 10; i++) {
					badDataRecord.add(rs.getString(i));
				}
				
				//send ArrayList to this method
				writeBadData(badDataRecord);
			}
			
			System.out.println("\nAll bad entries have been written to bad-data-<timestamp>.csv.");
			createLogFile();

		} catch (SQLException e) {
			System.out.println(e.getMessage());
			System.out.println("Issue getting data from testing.db");
		}
	}

	public static void writeBadData(ArrayList<String> badDataRecord) {
		
		try {
			//ArrayList is read into our bad-data-<timestamp>.csv
			csvPrinter.printRecord(badDataRecord);
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	//total_counter is correct
	//however, successful and failed counters are not
	//failed_counter incremented for each entry (not for each row) that had 'bad-data'
	//failed_counter will be larger than it should be
	public static void createLogFile() throws IOException {
		total_counter /= 10;
		successful_counter = total_counter - failed_counter;
		BufferedWriter output = null;

		try {
			File file = new File("src/main/resources/db_stats.txt");
			output = new BufferedWriter(new FileWriter(file));
			output.write("Log File!" + "\n--------------------------" + "\n# of records received: " + total_counter
					+ "\n# of records successful: " + successful_counter + "\n# of records failed: " + failed_counter);

			System.out.println("\nThe db_stats.txt file has been created.");
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("Issue creating or logging to db_stats.txt");
		} finally {
			if (output != null) {
				output.close();
			}
		}
	}

	public static void main(String[] args) {
		//stand-alone application starts here
		createTable();

	}

}