package flightapp;

import java.io.IOException;
import java.security.KeyStore.ProtectionParameter;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Runs queries against a back-end database
 */
public class Query extends QueryAbstract {
  //
  // Canned queries
  //
  private static final String FLIGHT_CAPACITY_SQL = "SELECT capacity FROM Flights WHERE fid = ?";
  private PreparedStatement flightCapacityStmt;

  //
  // Instance variables
  //
  private boolean loggedIn;
  private String currentUser;
  private List<Itinerary> lastSearchedItineraries;

  // Prepared Statements
  private PreparedStatement clearUsersTable;
  private PreparedStatement clearReservationsTable;
  private PreparedStatement createUser;
  private PreparedStatement getUserIfExists;
  private PreparedStatement getDirectFlights;
  private PreparedStatement getIndirectFlights;
  private PreparedStatement getReservationsForDay;
  private PreparedStatement addReserveration;
  private PreparedStatement getSeatsTakenFlight1;
  private PreparedStatement getSeatsTakenFlight2;
  private PreparedStatement getReservationFromResID;
  private PreparedStatement updateReservationPayment;
  private PreparedStatement getFlightFromID;
  private PreparedStatement getUserBalance;
  private PreparedStatement updateUserBalance;
  private PreparedStatement getNextResID;
  private PreparedStatement getReservationList;

  protected Query() throws SQLException, IOException {
    this.loggedIn = false;
    prepareStatements();
  }

  /**
   * Clear the data in any custom tables created.
   * 
   * WARNING! Do not drop any tables and do not clear the flights table.
   */
  public void clearTables() {
    try {
      // TODO: YOUR CODE HERE
      this.clearReservationsTable.execute();
      this.clearUsersTable.execute();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /*
   * prepare all the SQL statements in this method.
   */
  private void prepareStatements() throws SQLException {
    flightCapacityStmt = conn.prepareStatement(FLIGHT_CAPACITY_SQL);

    // TODO: YOUR CODE HERE
    // Statements for clearing the data from Users and Reservations
    String clearUsersTableString = "DELETE FROM Users_lizazak";
    this.clearUsersTable = conn.prepareStatement(clearUsersTableString);

    String clearReservationsTableString = "DELETE FROM Reservations_lizazak";
    this.clearReservationsTable = conn.prepareStatement(clearReservationsTableString);

    // Statement for creating a new user
    String createUserString = "INSERT INTO Users_lizazak (username, hashedPassword, balance) VALUES (?, ?, ?)";
    this.createUser = conn.prepareStatement(createUserString);

    // Statement for checking if a username already exists in the database
    String getUserIfExistsString = "SELECT * FROM Users_lizazak WHERE username = ?";
    this.getUserIfExists = conn.prepareStatement(getUserIfExistsString);

    // Statement for getting direct flights based on user input.
    String getDirectFlightsString = "SELECT TOP(?) f.fid, f.carrier_id, f.flight_num, f.actual_time, f.capacity, f.price " 
                                    + "FROM FLIGHTS AS f "
                                    + "WHERE f.canceled = 0 "
                                    + "AND f.origin_city = ? "
                                    + "AND f.dest_city = ? "
                                    + "AND f.day_of_month = ? "
                                    + "ORDER BY f.actual_time ASC, f.fid ASC";
    this.getDirectFlights = conn.prepareStatement(getDirectFlightsString);

    // Statement for getting indirect flights based on user input
    String getIndirectFlightsString = "SELECT TOP(?) f1.fid AS fid1, f1.carrier_id AS cid1, f1.flight_num AS fnum1, " 
                                    + "f1.dest_city AS dest1, f1.actual_time AS time1, f1.capacity AS cap1, f1.price AS price1, " 
                                    + "f2.fid AS fid2, f2.carrier_id AS cid2, f2.flight_num AS fnum2, "
                                    + "f2.actual_time AS time2, f2.capacity AS cap2, f2.price AS price2 "
                                    + "FROM FLIGHTS AS f1 "
                                    + "JOIN FLIGHTS AS f2 ON f1.dest_city = f2.origin_city "
                                    + "WHERE f1.canceled = 0 "
                                    + "AND f2.canceled = 0 "
                                    + "AND f1.origin_city = ? "
                                    + "AND f2.dest_city = ? "
                                    + "AND f1.day_of_month = ? "
                                    + "AND f1.day_of_month = f2.day_of_month "
                                    + "ORDER BY (f1.actual_time + f2.actual_time) ASC, f1.fid ASC, f2.fid ASC";
    this.getIndirectFlights = conn.prepareStatement(getIndirectFlightsString);

    // Statement for getting a reservation based on a specific day.
    String getReservationsForDayString = "SELECT * FROM Reservations_lizazak AS r JOIN Flights AS f ON r.flight1_id = f.fid WHERE f.day_of_month = ?";
    this.getReservationsForDay = conn.prepareStatement(getReservationsForDayString);

    // Statement for inserting a booking a new itinerary
    String addReservationString = "INSERT INTO Reservations_lizazak (rid, username, paid, flight1_id, flight2_id) VALUES (?, ?, ?, ?, ?)";
    this.addReserveration = conn.prepareStatement(addReservationString);

    // Statement for getting the current seats taken in flight 1
    String getSeatsTakenString1  = "SELECT COUNT(*) AS count FROM Reservations_lizazak WHERE flight1_id = ?";
    this.getSeatsTakenFlight1 = conn.prepareStatement(getSeatsTakenString1);

    // Statement for getting the current seats taken in flight 2
    String getSeatsTakenString2  = "SELECT COUNT(*) AS count FROM Reservations_lizazak WHERE flight2_id = ?";
    this.getSeatsTakenFlight2 = conn.prepareStatement(getSeatsTakenString2);

    String getReservationFromResIDString = "SELECT username, paid, flight1_id, flight2_id FROM Reservations_lizazak WHERE rid = ?";
    this.getReservationFromResID = conn.prepareStatement(getReservationFromResIDString);

    String updateReservationPaymentString = "UPDATE Reservations_lizazak SET paid = 1 WHERE rid = ?";
    this.updateReservationPayment = conn.prepareStatement(updateReservationPaymentString);

    String getFlightFromIDString = "SELECT fid, day_of_month, carrier_id, flight_num, origin_city, dest_city, actual_time, capacity, price FROM FLIGHTS WHERE fid = ?";
    this.getFlightFromID = conn.prepareStatement(getFlightFromIDString);

    String getUserBalanceString = "SELECT balance FROM Users_lizazak WHERE username = ?";
    this.getUserBalance = conn.prepareStatement(getUserBalanceString);

    String updateUserBalanceString = "UPDATE Users_lizazak SET balance = ? WHERE username = ?";
    this.updateUserBalance = conn.prepareStatement(updateUserBalanceString);

    String getNextResIDString = "SELECT COUNT(*) AS count FROM Reservations_lizazak";
    this.getNextResID = conn.prepareStatement(getNextResIDString);

    String getReservationListString = "SELECT r.rid, r.paid, r.flight1_id, r.flight2_id FROM Reservations_lizazak as r WHERE r.username = ? ORDER BY r.rid ASC";
    this.getReservationList = conn.prepareStatement(getReservationListString);
  }

  /* See QueryAbstract.java for javadoc */
  public String transaction_login(String username, String password) {
    // TODO: YOUR CODE HERE
    // Check if a user is already logged in during this session.
    if (this.loggedIn) {
      return "User already logged in\n";
    }
    
    ResultSet userFound;
    try {
      // Usernames are case-insensitive, so convert input to lowercase.
      username = username.toLowerCase();

      // Check that the user exists in the database.
      this.getUserIfExists.clearParameters();
      this.getUserIfExists.setString(1, username);
      userFound = this.getUserIfExists.executeQuery();
      // If the username does not exist, return an error.
      if (userFound.next()) {
        // Get the hashed password stored for the given username 
        byte[] hashedPassword = userFound.getBytes("hashedPassword");

        // Check that the provided password matches the stored one.
        boolean passwordIsCorrect = PasswordUtils.plaintextMatchesSaltedHash(password, hashedPassword);

        // If the password is incorrect, return an error.
        if (!passwordIsCorrect) {
          userFound.close();
          return "Login failed\n";
        }

        // Set logged in to true for the current session.
        this.loggedIn = true;
        // Set the current user to be used in book.
        this.currentUser = username;
        userFound.close();
        return "Logged in as " + username + "\n";
      }

      userFound.close();

      // If the username wasn't found, return an error.
      return "Login failed\n";

    } catch(SQLException e) {
      e.printStackTrace();
    }
    return "Login failed\n";
  }

  /* See QueryAbstract.java for javadoc */
  public String transaction_createCustomer(String username, String password, int initAmount) {
    // TODO: YOUR CODE HERE
    // Check that balance is non-negative.
    if (initAmount < 0) {
      return "Failed to create user\n";
    }

    // Usernames are case-insensitive, so convert input to lowercase.
    username = username.toLowerCase();

    ResultSet existingUsers = null;

    try {
      // Start a transaction
      this.conn.setAutoCommit(false);

      this.getUserIfExists.clearParameters();
      this.getUserIfExists.setString(1, username);
      existingUsers = this.getUserIfExists.executeQuery();
      // If the username already exists, return an error
      if (existingUsers.next()) {
        this.conn.rollback();
        return "Failed to create user\n";
      }

      // Otherwise, add a new username to the database.
      this.createUser.clearParameters();

      // Set the username to the given username.
      this.createUser.setString(1, username);

      // Get the hashed and salted password and set the saltedPassword.
      byte[] saltedPassword = PasswordUtils.saltAndHashPassword(password);
      this.createUser.setBytes(2, saltedPassword);

      // Set the balance to the initial amount.
      this.createUser.setInt(3, initAmount);

      // Execute the insert update.
      this.createUser.executeUpdate();

      // Commit this as a single transaction.
      this.conn.commit();

      return "Created user " + username + "\n";
    } catch(SQLException e) {
      if (isDeadlock(e)) {
        // return this.transaction_createCustomer(username, password, initAmount);
        return this.transaction_createCustomer(username, password, initAmount);
      }
      e.printStackTrace();
    } finally {
      if (existingUsers != null) {
        try {
          existingUsers.close();
        } catch (SQLException e) {}
      }
      try {
        this.conn.setAutoCommit(true);
      } catch (SQLException e) {}
    }
    return "Failed to create user\n";
  }

  /* See QueryAbstract.java for javadoc */
  public String transaction_search(String originCity, String destinationCity, 
                                   boolean directFlight, int dayOfMonth,
                                   int numberOfItineraries) {
    // WARNING: the below code is insecure (it's susceptible to SQL injection attacks) AND only
    // handles searches for direct flights.  We are providing it *only* as an example of how
    // to use JDBC; you are required to replace it with your own secure implementation.
    //
    // TODO: YOUR CODE HERE
    List<Itinerary> itineraryList = new ArrayList<>();

    // one hop itineraries
    try {
      this.getDirectFlights.clearParameters();
      // Set the number of itineraries to be retrieved.
      this.getDirectFlights.setInt(1, numberOfItineraries);
      // Set the origin city.
      this.getDirectFlights.setString(2, originCity);
      // Set the destination city.
      this.getDirectFlights.setString(3, destinationCity);
      // Set the day of month.
      this.getDirectFlights.setInt(4, dayOfMonth);
      ResultSet oneHopResults = this.getDirectFlights.executeQuery();

      while (oneHopResults.next()) {
        int result_fid = oneHopResults.getInt("fid");
        String result_carrierId = oneHopResults.getString("carrier_id");
        String result_flightNum = oneHopResults.getString("flight_num");
        int result_time = oneHopResults.getInt("actual_time");
        int result_capacity = oneHopResults.getInt("capacity");
        int result_price = oneHopResults.getInt("price");

        Flight newFlight = new Flight(result_fid, dayOfMonth, result_carrierId,
                                      result_flightNum, originCity, destinationCity,
                                      result_time, result_capacity, result_price);

        Itinerary newItinerary = new Itinerary(newFlight, null);

        itineraryList.add(newItinerary);

        // Decrease number of itineraries needed (since we always prefer direct flights).
        numberOfItineraries--;
      }
      oneHopResults.close();

      // If we can still generate more itineraries, start generating indirect itineraries.
      if (numberOfItineraries > 0 && !directFlight) {
        // two hop itineraries
        this.getIndirectFlights.clearParameters();
        // Set the number of itineraries to be retrieved.
        this.getIndirectFlights.setInt(1, numberOfItineraries);
        // Set the origin city.
        this.getIndirectFlights.setString(2, originCity);
        // Set the destination city.
        this.getIndirectFlights.setString(3, destinationCity);
        // Set the day of month.
        this.getIndirectFlights.setInt(4, dayOfMonth);
        ResultSet twoHopResults = this.getIndirectFlights.executeQuery();

        while (twoHopResults.next()) {
          int fid1 = twoHopResults.getInt("fid1");
          String carrierId1 = twoHopResults.getString("cid1");
          String flightNum1 = twoHopResults.getString("fnum1");
          // This is the intermediary city in the indirect flight
          String destCity1 = twoHopResults.getString("dest1");
          int time1 = twoHopResults.getInt("time1");
          int capacity1 = twoHopResults.getInt("cap1");
          int price1 = twoHopResults.getInt("price1");

          Flight flight1 = new Flight(fid1, dayOfMonth, carrierId1, flightNum1, 
                            originCity, destCity1, time1, capacity1, price1);

          int fid2 = twoHopResults.getInt("fid2");
          String carrierId2 = twoHopResults.getString("cid2");
          String flightNum2 = twoHopResults.getString("fnum2");
          // The destination of the first flight is the origin of the 2nd flight.
          String originCity2 = destCity1;
          int time2 = twoHopResults.getInt("time2");
          int capacity2 = twoHopResults.getInt("cap2");
          int price2 = twoHopResults.getInt("price2");

          Flight flight2 = new Flight(fid2, dayOfMonth, carrierId2, flightNum2, 
                            originCity2, destinationCity, time2, capacity2, price2);
          
          // Make a new itinerary based on the 2 flights
          Itinerary newItinerary = new Itinerary(flight1, flight2);

          itineraryList.add(newItinerary);
        }
        twoHopResults.close();
      }

      // Check if there are no possible itineraries for these parameters.
      if (itineraryList.isEmpty()) {
        return "No flights match your selection\n";
      }

      // Sort the itineraries based on the custom compareTo method
      Collections.sort(itineraryList);

      StringBuffer itineraryString = new StringBuffer();
      // Add all itineraries to string to returb.
      for (int i = 0; i < itineraryList.size(); i++) {
        Itinerary itinerary = itineraryList.get(i);
        itinerary.itinerary_number = i;
        itineraryString.append(itinerary.toString());
      }

      // Track last searched itineraries to be used if the user wants to book 
      // a specific flight
      this.lastSearchedItineraries = itineraryList;

      return itineraryString.toString();
    } catch (SQLException e) {
      e.printStackTrace();
    }

    return "Failed to search\n";
  }

  /* See QueryAbstract.java for javadoc */
  public String transaction_book(int itineraryId) {
    // TODO: YOUR CODE HERE
    if (!this.loggedIn) {
      return "Cannot book reservations, not logged in\n";
    }

    Itinerary currentItinerary = getItinerary(itineraryId);
    if (this.lastSearchedItineraries == null || currentItinerary == null) {
      // If a previous search was not performed or there is no itinerary with
      // the specified id, cannot make a reservation.
      return "No such itinerary " + itineraryId + "\n";
    }

    Flight flight1 = currentItinerary.flight1;
    Flight flight2 = currentItinerary.flight2;

    try {
      // Check if a booking for the same day already exists
      this.getReservationsForDay.clearParameters();
      this.getReservationsForDay.setInt(1, flight1.dayOfMonth);
      ResultSet existingBookings = this.getReservationsForDay.executeQuery();
      // If the username already exists, return an error
      if (existingBookings.next()) {
        existingBookings.close();
        return "You cannot book two flights in the same day\n";
      }

      // If the first flight on this reservation is at capacity, return an error.
      this.getSeatsTakenFlight1.clearParameters();
      this.getSeatsTakenFlight1.setInt(1, flight1.fid);
      ResultSet seatsTaken1 = this.getSeatsTakenFlight1.executeQuery();
      if (seatsTaken1.next()) {
        int currentCap1 = seatsTaken1.getInt("count");
        if (checkFlightCapacity(flight1.fid) <= currentCap1) {
          return "Booking failed\n";
        }
      }

      // If the 2nd flight exists, if it's at capacity, return an error.
      if (flight2 != null) {
        this.getSeatsTakenFlight2.clearParameters();
        this.getSeatsTakenFlight2.setInt(1, flight2.fid);
        ResultSet seatsTaken2 = this.getSeatsTakenFlight2.executeQuery();
        if (seatsTaken2.next()) {
          int currentCap2 = seatsTaken2.getInt("count");
          if (checkFlightCapacity(flight2.fid) <= currentCap2) {
            return "Booking failed\n";
          }
        }
      }

      int newResId = this.getNextResID();

      // Otherwise, add a new booking to the database.
      this.addReserveration.clearParameters();
      // Set the reservation id.
      this.addReserveration.setInt(1, newResId);
      // Set the username to the given username.
      assert this.currentUser != null : "We must have a user if we are logged in.";
      this.addReserveration.setString(2, this.currentUser);
      // Set it to be unpaid.
      this.addReserveration.setInt(3, 0);
      // Set the flight1 fid and flight2 fids.
      this.addReserveration.setInt(4, flight1.fid);
      // Set the flight2 fid to the fid, or null if it's a direct flight.
      if (flight2 == null) {
        this.addReserveration.setNull(5, Types.INTEGER);
      } else {
        this.addReserveration.setInt(5, flight2.fid);
      }

      // Execute the add update.
      this.addReserveration.executeUpdate();

      return "Booked flight(s), reservation ID: " + newResId + "\n";
    } catch(SQLException e) {
      e.printStackTrace();
    }
    return "Booking failed\n";
  }

  /* See QueryAbstract.java for javadoc */
  public String transaction_pay(int reservationId) {
    // TODO: YOUR CODE HERE
    if (!loggedIn) {
      return "Cannot pay, not logged in\n";
    }

    try {
      this.getReservationFromResID.clearParameters();
      this.getReservationFromResID.setInt(1, reservationId);

      ResultSet currentReservation = this.getReservationFromResID.executeQuery();
      if (currentReservation.next()) {
        String username = currentReservation.getString("username");
        int paid = currentReservation.getInt("paid");
        int fid1 = currentReservation.getInt("flight1_id");
        int fid2 = currentReservation.getInt("flight2_id");
        if (currentReservation.wasNull()) {
          fid2 = -1;
        }

        // Check if username does not match currently logged in user or the reservation is already paid.
        if (!username.equals(this.currentUser) || paid == 1) {
          return "Cannot find unpaid reservation " + reservationId + " under user: " + this.currentUser + "\n";
        }

        // Check if not enough balance for flight(s).
        int flightPrice1 = this.getItineraryPrice(fid1);
        int flightPrice2 = fid2 == -1 ? 0 : this.getItineraryPrice(fid2);
        int totalFlightPrice = flightPrice1 + flightPrice2;
        int currentBalance = this.getUserBalance();
        if (currentBalance - totalFlightPrice < 0) {
          return "User has only " + currentBalance + " in account but itinerary costs " + totalFlightPrice + "\n";
        }

        // Update reservation to be paid.
        this.updateReservationPayment(reservationId);

        // Update user balance to pay for reservation.
        int newBalance = currentBalance - totalFlightPrice;
        this.updateUserBalance(this.currentUser, newBalance);

        return "Paid reservation: " + reservationId + " remaining balance: " + newBalance + "\n";
      } else {
        // No existing res with this id
        return "Cannot find unpaid reservation " + reservationId + " under user: " + this.currentUser + "\n";
      }

    } catch(SQLException e) {
      e.printStackTrace();
    }
    return "Failed to pay for reservation " + reservationId + "\n";
  }

  /* See QueryAbstract.java for javadoc */
  public String transaction_reservations() {
    // TODO: YOUR CODE HERE
    if (!this.loggedIn) {
      return "Cannot view reservations, not logged in\n";
    }

    // If someone is logged in, there must be a current user;
    assert this.currentUser != null;

    try {
      this.getReservationList.clearParameters();
      this.getReservationList.setString(1, this.currentUser);
      ResultSet reservations = this.getReservationList.executeQuery();

      StringBuffer sb = new StringBuffer();
      while (reservations.next()) {
        int result_rid = reservations.getInt("rid");
        int result_paid = reservations.getInt("paid");
        int result_fid1 = reservations.getInt("flight1_id");
        int result_fid2 = reservations.getInt("flight2_id");
        boolean paid = result_paid == 1;
        String flightString1 = this.getFlightString(result_fid1);
        String flightString2 = this.getFlightString(result_fid2);
        
        if (flightString2 == null) {
          sb.append("Reservation " + result_rid + " paid: " + paid + ":\n" + flightString1 + "\n");
        } else {
          sb.append("Reservation " + result_rid + " paid: " + paid + ":\n" + flightString1 + "\n" 
                    + flightString2 + "\n");
        }
      }

      return sb.toString();

    } catch (SQLException e) {
      e.printStackTrace();
    }
    return "Failed to retrieve reservations\n";
  }

  /**
   * Example utility function that uses prepared statements
   */
  private int checkFlightCapacity(int fid) throws SQLException {
    flightCapacityStmt.clearParameters();
    flightCapacityStmt.setInt(1, fid);

    ResultSet results = flightCapacityStmt.executeQuery();
    results.next();
    int capacity = results.getInt("capacity");
    results.close();

    return capacity;
  }

  private Itinerary getItinerary(int id) {
    // Go through each itinerary and return the itinerary which matches
    // the given id
    for (Itinerary currentItinerary: this.lastSearchedItineraries) {
      if (currentItinerary.itinerary_number == id) {
        return currentItinerary;
      }
    }
    // If no itineraries matched the id, return null.
    return null;
  }

  private int getNextResID() {
    try {
      this.getNextResID.clearParameters();
      ResultSet nextID = this.getNextResID.executeQuery();
      if (nextID.next()) {
        return nextID.getInt("count") + 1;
      }
    } catch (SQLException e) {
      e.printStackTrace();;
    }
    return 1;
  }

  // Retrieves the cost for an itinerary with one flight
  private int getItineraryPrice(int fid) {
    try {
      this.getFlightFromID.clearParameters();
      this.getFlightFromID.setInt(1, fid);

      ResultSet flight = this.getFlightFromID.executeQuery();
      if (flight.next()) {
        return flight.getInt("price");
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return -1;
  }

  private String getFlightString(int fid) {
    try {
      this.getFlightFromID.clearParameters();
      this.getFlightFromID.setInt(1, fid);

      ResultSet flightResult = this.getFlightFromID.executeQuery();
      if (flightResult.next()) {
        // fid, day_of_month, carrier_id, flight, origin_city, dest_city, actual_time, capacity, price
        int result_fid = flightResult.getInt("fid");
        int dayOfMonth = flightResult.getInt("day_of_month");
        String cid = flightResult.getString("carrier_id");
        String flightNum = flightResult.getString("flight_num");
        String originCity = flightResult.getString("origin_city");
        String destCity = flightResult.getString("dest_city");
        int time = flightResult.getInt("actual_time");
        int capacity = flightResult.getInt("capacity");
        int price = flightResult.getInt("price");

        Flight flight = new Flight(result_fid, dayOfMonth, cid, flightNum, originCity, destCity, time, capacity, price);

        return flight.toString();
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return null;
  }

  // Get current balance of currently logged in user.
  private int getUserBalance() {
    try {
      this.getUserBalance.clearParameters();
      this.getUserBalance.setString(1, this.currentUser);

      ResultSet userBalance = this.getUserBalance.executeQuery();
      if (userBalance.next()) {
        return userBalance.getInt("balance");
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return 0;
  }

  // Update reservation to be paid.
  private void updateReservationPayment(int rid) {
    try {
      this.updateReservationPayment.clearParameters();
      this.updateReservationPayment.setInt(1, rid);
      this.updateReservationPayment.executeUpdate();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  // Update balance of currently logged in user.
  private void updateUserBalance(String username, int newBalance) {
    try {
      this.updateUserBalance.clearParameters();
      this.updateUserBalance.setInt(1, newBalance);
      this.updateUserBalance.setString(2, username);
      this.updateUserBalance.executeUpdate();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  /**
   * Utility function to determine whether an error was caused by a deadlock
   */
  private static boolean isDeadlock(SQLException e) {
    return e.getErrorCode() == 1205;
  }

  /**
   * A class to store information about a single flight
   *
   * TODO(hctang): move this into QueryAbstract
   */
  class Flight {
    public int fid;
    public int dayOfMonth;
    public String carrierId;
    public String flightNum;
    public String originCity;
    public String destCity;
    public int time;
    public int capacity;
    public int price;

    Flight(int id, int day, String carrier, String fnum, String origin, String dest, int tm,
           int cap, int pri) {
      fid = id;
      dayOfMonth = day;
      carrierId = carrier;
      flightNum = fnum;
      originCity = origin;
      destCity = dest;
      time = tm;
      capacity = cap;
      price = pri;
    }
    
    @Override
    public String toString() {
      return "ID: " + fid + " Day: " + dayOfMonth + " Carrier: " + carrierId + " Number: "
          + flightNum + " Origin: " + originCity + " Dest: " + destCity + " Duration: " + time
          + " Capacity: " + capacity + " Price: " + price;
    }
  }

  /**
   * A class that stores information about a single itinerary
   */
  class Itinerary implements Comparable<Itinerary> {
    // TODO: add instance variables
    public int itinerary_number;
    public Flight flight1;
    public Flight flight2;

    // TODO: add methods and implement Comparable
    public Itinerary(Flight flight1, Flight flight2) {
      this.itinerary_number = 0;
      this.flight1 = flight1;
      this.flight2 = flight2;
    }

    public int totalTime() {
      int total = flight1.time;
      if (!isDirect()) {
        total += flight2.time;
      }
      return total;
    }

    public boolean isDirect() {
      return flight2 == null;
    }

    public int numberOfFlights() {
      if (this.isDirect()) {
        return 1;
      } else {
        return 2;
      }
    }

    @Override
    public String toString() {
      String toReturn = "Itinerary " + itinerary_number + ": " + this.numberOfFlights() + " flight(s), " 
                        + this.totalTime() + " minutes\n" + flight1.toString() + "\n";
      if (!this.isDirect()) {
        toReturn += flight2.toString() + "\n";
      }
      return toReturn;
    }

    public int compareTo(Itinerary other) {
      int time_difference = this.totalTime() - other.totalTime();
      // If the total times are not equal, they can be sorted by increasing time.
      if (time_difference != 0) {
        return time_difference;
      }

      int fid1_difference = this.flight1.fid - other.flight1.fid;
      // If not equal, choose flight 1 with the smaller fid value.
      if (fid1_difference != 0) {
        return fid1_difference;
      }

      // Otherwise, choose flight 2 with the smaller fid value.
      return this.flight2.fid - other.flight2.fid;
    }
  }
}
