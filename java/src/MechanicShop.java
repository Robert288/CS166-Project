/*
 * Template JAVA User Interface
 * =============================
 *
 * Database Management Systems
 * Department of Computer Science &amp; Engineering
 * University of California - Riverside
 *
 * Target DBMS: 'Postgres'
 *
 */


import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.ArrayList;

/**
 * This class defines a simple embedded SQL utility class that is designed to
 * work with PostgreSQL JDBC drivers.
 *
 */

public class MechanicShop{
	//reference to physical database connection
	private Connection _connection = null;
	static BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
	
	public MechanicShop(String dbname, String dbport, String user, String passwd) throws SQLException {
		System.out.print("Connecting to database...");
		try{
			// constructs the connection URL
			String url = "jdbc:postgresql://localhost:" + dbport + "/" + dbname;
			System.out.println ("Connection URL: " + url + "\n");
			
			// obtain a physical connection
	        this._connection = DriverManager.getConnection(url, user, passwd);
	        System.out.println("Done");
		}catch(Exception e){
			System.err.println("Error - Unable to Connect to Database: " + e.getMessage());
	        System.out.println("Make sure you started postgres on this machine");
	        System.exit(-1);
		}
	}
	
	/**
	 * Method to execute an update SQL statement.  Update SQL instructions
	 * includes CREATE, INSERT, UPDATE, DELETE, and DROP.
	 * 
	 * @param sql the input SQL string
	 * @throws java.sql.SQLException when update failed
	 * */
	public void executeUpdate (String sql) throws SQLException { 
		// creates a statement object
		Statement stmt = this._connection.createStatement ();

		// issues the update instruction
		stmt.executeUpdate (sql);

		// close the instruction
	    stmt.close ();
	}//end executeUpdate

	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and outputs the results to
	 * standard out.
	 * 
	 * @param query the input query string
	 * @return the number of rows returned
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public int executeQueryAndPrintResult (String query) throws SQLException {
		//creates a statement object
		Statement stmt = this._connection.createStatement ();

		//issues the query instruction
		ResultSet rs = stmt.executeQuery (query);

		/*
		 *  obtains the metadata object for the returned result set.  The metadata
		 *  contains row and column info.
		 */
		ResultSetMetaData rsmd = rs.getMetaData ();
		int numCol = rsmd.getColumnCount ();
		int rowCount = 0;
		
		//iterates through the result set and output them to standard out.
		boolean outputHeader = true;
		while (rs.next()){
			if(outputHeader){
				for(int i = 1; i <= numCol; i++){
					System.out.print(rsmd.getColumnName(i) + "\t");
			    }
			    System.out.println();
			    outputHeader = false;
			}
			for (int i=1; i<=numCol; ++i)
				System.out.print (rs.getString (i) + "\t");
			System.out.println ();
			++rowCount;
		}//end while
		stmt.close ();
		return rowCount;
	}
	
	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and returns the results as
	 * a list of records. Each record in turn is a list of attribute values
	 * 
	 * @param query the input query string
	 * @return the query result as a list of records
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public List<List<String>> executeQueryAndReturnResult (String query) throws SQLException { 
		//creates a statement object 
		Statement stmt = this._connection.createStatement (); 
		
		//issues the query instruction 
		ResultSet rs = stmt.executeQuery (query); 
	 
		/*
		 * obtains the metadata object for the returned result set.  The metadata 
		 * contains row and column info. 
		*/ 
		ResultSetMetaData rsmd = rs.getMetaData (); 
		int numCol = rsmd.getColumnCount (); 
		int rowCount = 0; 
	 
		//iterates through the result set and saves the data returned by the query. 
		boolean outputHeader = false;
		List<List<String>> result  = new ArrayList<List<String>>(); 
		while (rs.next()){
			List<String> record = new ArrayList<String>(); 
			for (int i=1; i<=numCol; ++i) 
				record.add(rs.getString (i)); 
			result.add(record); 
		}//end while 
		stmt.close (); 
		return result; 
	}//end executeQueryAndReturnResult
	
	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and returns the number of results
	 * 
	 * @param query the input query string
	 * @return the number of rows returned
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public int executeQuery (String query) throws SQLException {
		//creates a statement object
		Statement stmt = this._connection.createStatement ();

		//issues the query instruction
		ResultSet rs = stmt.executeQuery (query);

		int rowCount = 0;

		//iterates through the result set and count nuber of results.
		if(rs.next()){
			rowCount++;
		}//end while
		stmt.close ();
		return rowCount;
	}
	
	/**
	 * Method to fetch the last value from sequence. This
	 * method issues the query to the DBMS and returns the current 
	 * value of sequence used for autogenerated keys
	 * 
	 * @param sequence name of the DB sequence
	 * @return current value of a sequence
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	
	public int getCurrSeqVal(String sequence) throws SQLException {
		Statement stmt = this._connection.createStatement ();
		
		ResultSet rs = stmt.executeQuery (String.format("Select currval('%s')", sequence));
		if (rs.next()) return rs.getInt(1);
		return -1;
	}

	/**
	 * Method to close the physical connection if it is open.
	 */
	public void cleanup(){
		try{
			if (this._connection != null){
				this._connection.close ();
			}//end if
		}catch (SQLException e){
	         // ignored.
		}//end try
	}//end cleanup

	/**
	 * The main execution method
	 * 
	 * @param args the command line arguments this inclues the <mysql|pgsql> <login file>
	 */
	public static void main (String[] args) {
		if (args.length != 3) {
			System.err.println (
				"Usage: " + "java [-classpath <classpath>] " + MechanicShop.class.getName () +
		            " <dbname> <port> <user>");
			return;
		}//end if
		
		MechanicShop esql = null;
		
		try{
			System.out.println("(1)");
			
			try {
				Class.forName("org.postgresql.Driver");
			}catch(Exception e){

				System.out.println("Where is your PostgreSQL JDBC Driver? " + "Include in your library path!");
				e.printStackTrace();
				return;
			}
			
			System.out.println("(2)");
			String dbname = args[0];
			String dbport = args[1];
			String user = args[2];
			
			esql = new MechanicShop (dbname, dbport, user, "");
			
			boolean keepon = true;
			while(keepon){
				System.out.println("MAIN MENU");
				System.out.println("---------");
				System.out.println("1. AddCustomer");
				System.out.println("2. AddMechanic");
				System.out.println("3. AddCar");
				System.out.println("4. InsertServiceRequest");
				System.out.println("5. CloseServiceRequest");
				System.out.println("6. ListCustomersWithBillLessThan100");
				System.out.println("7. ListCustomersWithMoreThan20Cars");
				System.out.println("8. ListCarsBefore1995With50000Milles");
				System.out.println("9. ListKCarsWithTheMostServices");
				System.out.println("10. ListCustomersInDescendingOrderOfTheirTotalBill");
				System.out.println("11. < EXIT");
				
				/*
				 * FOLLOW THE SPECIFICATION IN THE PROJECT DESCRIPTION
				 */
				switch (readChoice()){
					case 1: AddCustomer(esql); break;
					case 2: AddMechanic(esql); break;
					case 3: AddCar(esql); break;
					case 4: InsertServiceRequest(esql); break;
					case 5: CloseServiceRequest(esql); break;
					case 6: ListCustomersWithBillLessThan100(esql); break;
					case 7: ListCustomersWithMoreThan20Cars(esql); break;
					case 8: ListCarsBefore1995With50000Milles(esql); break;
					case 9: ListKCarsWithTheMostServices(esql); break;
					case 10: ListCustomersInDescendingOrderOfTheirTotalBill(esql); break;
					case 11: keepon = false; break;
				}
			}
		}catch(Exception e){
			System.err.println (e.getMessage ());
		}finally{
			try{
				if(esql != null) {
					System.out.print("Disconnecting from database...");
					esql.cleanup ();
					System.out.println("Done\n\nBye !");
				}//end if				
			}catch(Exception e){
				// ignored.
			}
		}
	}

	public static int readChoice() {
		int input;
		// returns only if a correct value is given.
		do {
			System.out.print("Please make your choice: ");
			try { // read the integer, parse it and break.
				input = Integer.parseInt(in.readLine());
				break;
			}catch (Exception e) {
				System.out.println("Your input is invalid!");
				continue;
			}//end try
		}while (true);
		return input;
	}//end readChoice
	
	public static void AddCustomer(MechanicShop esql){//1
		String firstName;
		do {
			System.out.print("\tEnter customer's first name: ");
			try {
				firstName = in.readLine();
				if (firstName.length() <= 0 || firstName.length() > 32) {
					throw new RuntimeException("Customer's first name can not be null (empty) or exceed 32 characters.");
				}
				break;
			} catch (Exception e) {
				System.err.println(e.getMessage());
				continue;
			}
		} while (true);

		String lastName;
		do {
			System.out.print("\tEnter customer's last name: ");
			try {
				lastName = in.readLine();
				if (lastName.length() <= 0 || lastName.length() > 32) {
					throw new RuntimeException("Customer's last name can not be null (empty) or exceed 32 characters.");
				}
				break;
			} catch (Exception e) {
				System.err.println(e.getMessage());
				continue;
			}
		} while (true);

		String phoneNumber;
		do {
			System.out.print("\tEnter customer's phone number: ");
			try {
				phoneNumber = in.readLine();
				if (phoneNumber.length() <= 0 || phoneNumber.length() > 13) {
					throw new RuntimeException("Customer's phone number can not be null (empty) or exceed 13 characters.");
				}
				break;
			} catch (Exception e) {
				System.err.println(e.getMessage());
				continue;
			}
		} while (true);

		String address;
		do {
			System.out.print("\tEnter customer's address: ");
			try {
				address = in.readLine();
				if (address.length() <= 0 || address.length() > 256) {
					throw new RuntimeException("Customer's address can not be null (empty) or exceed 256 characters.");
				}
				break;
			} catch (Exception e) {
				System.err.println(e.getMessage());
				continue;
			}
		} while (true);

    		try {
	        	int rowCount = esql.executeQueryAndPrintResult("SELECT * FROM Customer");
        		System.out.println("total row(s): " + rowCount);

			String query = "INSERT INTO Customer VALUES (" + rowCount + ", \'" + firstName + "\', \'" + lastName + "\', \'" + phoneNumber + "\', \'" + address + "\')";
			esql.executeUpdate(query);
	        	rowCount = esql.executeQueryAndPrintResult("SELECT * FROM Customer");
        		System.out.println("total row(s): " + rowCount);
			System.out.println("\tSuccess!");
    		} catch (Exception e) {
            		System.err.println(e.getMessage());
        	}		
	}
	
	public static void AddMechanic(MechanicShop esql){//2
		String firstName;
		do {
			System.out.print("\tEnter mechanic's first name: ");
			try {
				firstName = in.readLine();
				if (firstName.length() <= 0 || firstName.length() > 32) {
					throw new RuntimeException("Mechanic's first name can not be null (empty) or exceed 32 characters.");
				}
				break;
			} catch (Exception e) {
				System.err.println(e.getMessage());
				continue;
			}
		} while (true);

		String lastName;
		do {
			System.out.print("\tEnter mechanic's last name: ");
			try {
				lastName = in.readLine();
				if (lastName.length() <= 0 || lastName.length() > 32) {
					throw new RuntimeException("Mechanic's last name can not be null (empty) or exceed 32 characters.");
				}
				break;
			} catch (Exception e) {
				System.err.println(e.getMessage());
				continue;
			}
		} while (true);

		int yearsExperience;
		do {
			System.out.print("\tEnter mechanic's years of experience: ");
			try {
				yearsExperience = Integer.parseInt(in.readLine());
				break;
			} catch (Exception e) {
				System.err.println(e.getMessage());
				continue;
			}
		} while (true);

    		try {
	        	int rowCount = esql.executeQueryAndPrintResult("SELECT * FROM Mechanic");
        		System.out.println("total row(s): " + rowCount);

			String query = "INSERT INTO Mechanic VALUES (" + rowCount + ", \'" + firstName + "\', \'" + lastName + "\', " + yearsExperience + ")";
			esql.executeUpdate(query);
	        	rowCount = esql.executeQueryAndPrintResult("SELECT * FROM Mechanic");
        		System.out.println("total row(s): " + rowCount);
			System.out.println("\tSuccess!");
    		} catch (Exception e) {
            		System.err.println(e.getMessage());
        	}	
	}
	
	public static void AddCar(MechanicShop esql){//3
		String vin;
		do {
			System.out.print("\tEnter car's vin: ");
			try {
				vin = in.readLine();
				if (vin.length() <= 0 || vin.length() > 16) {
					throw new RuntimeException("Car's vin can not be null (empty) or exceed 32 characters.");
				}
				break;
			} catch (Exception e) {
				System.err.println(e.getMessage());
				continue;
			}
		} while (true);

		String make;
		do {
			System.out.print("\tEnter car's make: ");
			try {
				make = in.readLine();
				if (make.length() <= 0 || make.length() > 32) {
					throw new RuntimeException("Car's make can not be null (empty) or exceed 32 characters.");
				}
				break;
			} catch (Exception e) {
				System.err.println(e.getMessage());
				continue;
			}
		} while (true);

		String model;
		do {
			System.out.print("\tEnter car's model: ");
			try {
				model = in.readLine();
				if (model.length() <= 0 || model.length() > 32) {
					throw new RuntimeException("Car's model can not be null (empty) or exceed 32 characters.");
				}
				break;
			} catch (Exception e) {
				System.err.println(e.getMessage());
				continue;
			}
		} while (true);

		int year;
		do {
			System.out.print("\tEnter car's year: ");
			try {
				year = Integer.parseInt(in.readLine());
				break;
			} catch (Exception e) {
				System.err.println(e.getMessage());
				continue;
			}
		} while (true);

    		try {
			String query = "INSERT INTO Car VALUES (\'" + vin + "\', \'" + make + "\', \'" + model + "\', " + year + ")";
			esql.executeUpdate(query);

	       		int rowCount = esql.executeQueryAndPrintResult("SELECT * FROM Car");
        		System.out.println("total row(s): " + rowCount);
			System.out.println("\tSuccess!");
    		} catch (Exception e) {
            		System.err.println(e.getMessage());
        	}		
	}
	
	public static void InsertServiceRequest(MechanicShop esql){//4
        String lastName;
        do {
            System.out.print("\tEnter customer's last name: ");
            try {
                 lastName = in.readLine();
                if (lastName.length() <= 0 || lastName.length() > 32) {
                    throw new RuntimeException("Customer's last name can not be null (empty) or exceed 32 characters.");
                }
                break;
            } catch (Exception e) {
                System.err.println(e.getMessage());
                continue;
            }
        } while (true);

		int cID = -1;
        try {
        	String query = "SELECT C.id, C.fname, C.lname FROM Customer C WHERE C.lname = \'" + lastName + "\'";
            int rowCount = esql.executeQueryAndPrintResult(query);
            System.out.println("Total row(s): " + rowCount);
            if (rowCount > 1) {
                System.out.println("\tWhich customer wants to insert a service request? Enter customer's id: ");
                cID = Integer.parseInt(in.readLine());
                query = "SELECT C.id, C.fname, C.lname FROM Customer C WHERE C.id = " + cID;
                rowCount = esql.executeQueryAndPrintResult(query);
                System.out.println("Total row(s): " + rowCount);
            } else if (rowCount == 0) {
                throw new RuntimeException("Customer does not exist!");
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            System.out.println("Before inserting a service request, please use the \"AddCustomer\" function. Otherwise, try a different last name.");
        }

        String VIN;
        try {
			String query;
            if (cID == -1) {
                query = "SELECT * FROM Car WHERE vin IN (SELECT car_vin FROM Owns WHERE customer_id IN (SELECT id FROM Customer WHERE lname = \'" + lastName + "\'))";
            } else {
                query = "SELECT * FROM Car WHERE vin IN (SELECT car_vin FROM Owns WHERE customer_id = " + cID + ")";
            }
            int rowCount = esql.executeQueryAndPrintResult(query);
			System.out.println("Total row(s): " + rowCount);
            if (rowCount > 1) {
	            System.out.print("Enter the vin for the car that needs serivce: ");
                VIN = in.readLine();
			} else if (rowCount == 0) {
                throw new RuntimeException("No car(s) found under this customer.");
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            System.out.println("Before adding a service request for a customer, please use the \"AddCar\" function. Otherwise, try a different name.");
        }	
	}
	
	public static void CloseServiceRequest(MechanicShop esql) throws Exception{//5
        int serviceRequestNumber;
            do {
                System.out.print("\tEnter service request number: ");
                try {
                    serviceRequestNumber = Integer.parseInt(in.readLine());
                    break;
                } catch (Exception e) {
                    System.err.println(e.getMessage());
                    continue;
                }
            } while (true);

        	int employeeID;
            do {
                System.out.print("\tEnter employee ID: ");
                try {
                    employeeID = Integer.parseInt(in.readLine());
                    break;
                } catch (Exception e) {
                    System.err.println(e.getMessage());
                    continue;
				}
            } while (true);

            String comment;
            do {
                System.out.print("\tEnter comment: ");
                try {
                    comment = in.readLine();
                    break;
                } catch (Exception e) {
                    System.err.println(e.getMessage());
                    continue;
                }
            } while (true);

            int bill;
            do {
                System.out.print("\tEnter bill: ");
                try {
                    bill = Integer.parseInt(in.readLine());
                    break;
                } catch (Exception e) {
                    System.err.println(e.getMessage());
                    continue;
                }
            } while (true);

            try {
                String query = "INSERT INTO Closed_Request VALUES (" + serviceRequestNumber + ", " + serviceRequestNumber + ", " + employeeID + ", getdate(), \'" + comment + "\', " + bill + ") RETURNING *";
                int rowCount = esql.executeQueryAndPrintResult(query);
                System.out.println("Total row(s): " + rowCount);
            } catch (Exception e) {
                System.err.println(e.getMessage());
            }
	}
	
	public static void ListCustomersWithBillLessThan100(MechanicShop esql){//6
		try {
			String query = "SELECT C.fname, C.lname, CR.date, CR.comment, CR.bill FROM Customer C, Closed_Request CR WHERE C.id IN (SELECT SR.customer_ID FROM Service_Request SR WHERE CR.bill < 100 AND CR.wid = SR.rid)";

	        int rowCount = esql.executeQueryAndPrintResult(query);
			System.out.println("Total row(s): " + rowCount);
		} catch (Exception e) {
			System.err.println(e.getMessage());
		}
	}
	
	public static void ListCustomersWithMoreThan20Cars(MechanicShop esql){//7
		try {
			String query = "SELECT C.fname, C.lname FROM Customer C WHERE C.id IN (SELECT O.customer_id FROM Owns O GROUP BY O.customer_id HAVING COUNT(O.car_vin) > 20)";

	        int rowCount = esql.executeQueryAndPrintResult(query);
			System.out.println("Total row(s): " + rowCount);
		} catch (Exception e) {
			System.err.println(e.getMessage());
		}
	}
	
	public static void ListCarsBefore1995With50000Milles(MechanicShop esql){//8
		try {
			String query = "SELECT C.make, C.model, C.year FROM Car C WHERE C.vin IN (SELECT SR.car_vin FROM Service_Request SR WHERE SR.odometer < 50000) AND C.year < 1995";

	        int rowCount = esql.executeQueryAndPrintResult(query);
			System.out.println("Total row(s): " + rowCount);
		} catch (Exception e) {
			System.err.println(e.getMessage());
		}
	}
	
	public static void ListKCarsWithTheMostServices(MechanicShop esql){//9
		int limit;
		do {
			System.out.print("\tEnter a number to see which car(s) have the highest number of service orders: ");
			try {
				limit = Integer.parseInt(in.readLine());
				if (limit <= 0) {
					throw new RuntimeException("Number must be greater than zero.");
				}
				break;
			} catch (Exception e) {
				System.err.println(e.getMessage());
				continue;
			}
		} while (true);

		try {
			String query = "SELECT C.make, C.model, C.year, COUNT(C.vin) AS numServiceOrders FROM Car C, Service_Request SR WHERE C.vin = SR.car_vin GROUP BY C.vin ORDER BY numServiceOrders DESC LIMIT " + limit;
		
			int rowCount = esql.executeQueryAndPrintResult(query);
			System.out.println("Total row(s): " + rowCount);
		} catch (Exception e) {
			System.err.println(e.getMessage());
		}	
	}
	
	public static void ListCustomersInDescendingOrderOfTheirTotalBill(MechanicShop esql){//9
		try {
			String query = "SELECT C.fname, C.lname, SUM(CR.bill) AS totalBill FROM Customer C, Closed_Request CR WHERE EXISTS (SELECT * FROM Service_Request SR WHERE SR.rid = CR.wid AND C.id = SR.customer_id) GROUP BY C.id ORDER BY TotalBill DESC";
		
			int rowCount = esql.executeQueryAndPrintResult(query);
			System.out.println("Total row(s): " + rowCount);
		} catch (Exception e) {
			System.err.println(e.getMessage());
		}	
	}
}