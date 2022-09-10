package example.dto;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.regex.*;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Metadata;

import com.datastax.driver.core.Statement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import java.util.*;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnDefinitions.Definition;

public class Generic {
  private static final Logger logger = Logger.getLogger(Generic.class);

	private Cluster cluster;
  private String PWD;

  public Generic(String node, String pwd)
  {
    final String connectionMsg = " to cluster";
    try {
  		cluster = Cluster.builder().addContactPoint(node).build();
      logger.info("Connected" + connectionMsg);
      getMetadata();
    } catch(Exception e) {
      logger.error("Failed to connect" + connectionMsg);
      System.exit(1);
    }
    PWD = pwd;
  }

  private void getMetadata()
  {
    Metadata metadata = cluster.getMetadata();
    metadata.getAllHosts()
    .forEach(host -> logger.info("Datatacenter: " + host.getDatacenter()
        + "| Host: " + host.getAddress() + "| Rack: "
        + host.getRack())
    );
  }

  private String readcqlFile(String file) throws IOException
  {
    // Now calling Files.readString() method to
    // read the file
    String fileName = PWD + "/src/main/resources/cql/" + file;
    // Now calling Files.readString() method to
    // read the file
    String str = new String(
      java.nio.file.Files.readAllBytes(
      java.nio.file.Paths.get(fileName)), java.nio.charset.StandardCharsets.UTF_8);

    str = str.trim().replace("\t", " ").replace("\r", " ").replace("\n", " ");

    return str;
  }

  private void getColumns(Row row, ColumnDefinitions columeDef)
  {
    List<Definition> columeDefList = columeDef.asList();
    for(Definition def: columeDefList){

      String name = def.getName();

      if(def.getType() == DataType.cint())
          logger.info(name+": "+row.getInt(name));
      else
          logger.info(name+": "+row.getString(name));

    }
  }

	private void querySchema(Session session, String keyspace, String columnFamily) {
		Statement statement = QueryBuilder.select()
    .all()
    .from(keyspace, columnFamily);
		ResultSet results = session.execute(statement);
		results.forEach(row ->
			getColumns(row, row.getColumnDefinitions())
		);
	}

  private void selectColumnFamily(Session session, String sql)
  {
    String keyspace = "?";
    String columnFamily = "?";

    Pattern p = Pattern.compile("FROM (.*);");   // the pattern to search for
    Matcher m = p.matcher(sql);

    // if we find a match, get the group
    if (m.find())
    {
      String[] findResult = m.group(1).split("\\.");
      // we're only looking for one group, so get it
      if(findResult.length == 2) {
        keyspace = findResult[0];
        columnFamily = findResult[1];
      } else {
        keyspace = "?";
        columnFamily=findResult[0];
      }
    }
    else
      logger.error("Using regex to find tbl name failed");

    if(!keyspace.equals("?") && !columnFamily.equals("?")){
      querySchema(session, keyspace, columnFamily);
    }

    logger.info("Keyspace: "+keyspace+", ColumnFamily: "+columnFamily);
  }
  public void operation(String fileName, CQLOPT opt)
  {
	  Session session;
    session = cluster.connect();
    try {
      String sql = readcqlFile(fileName+opt.cqlFile);
      if (opt == CQLOPT.SELECT)
        selectColumnFamily(session, sql);
      else
        session.execute(sql);
    } catch(IOException e) {
      logger.warn(String.format("Failed readcqlFile path: %s", PWD));
      logger.warn(String.format("Failed to readcqlFile: %s", fileName+opt.cqlFile));
    }
    logger.info(opt.operation+" complete");
    session.close();
  }

	public void close() {
		cluster.close();
	}
}
