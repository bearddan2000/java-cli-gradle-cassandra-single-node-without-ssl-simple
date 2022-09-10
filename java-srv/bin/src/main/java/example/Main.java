package example;

import org.apache.log4j.PropertyConfigurator;

import org.apache.log4j.Logger;

import example.dto.*;

public class Main {

  private static final Logger logger = Logger.getLogger(Main.class);


  public static void main(String[] args) {
    String PWD = System.getenv("PWD");

//    PropertyConfigurator.configure(PWD + "/src/main/resources/log4j.xml");

    String serverIP = "db";
    Generic client = new Generic(serverIP, PWD);
    client.operation("00", CQLOPT.KEYSPACE);

    client.operation("01", CQLOPT.CREATE);
    client.operation("02", CQLOPT.INSERT);
    client.operation("03", CQLOPT.SELECT);

    client.close();
  }
}
