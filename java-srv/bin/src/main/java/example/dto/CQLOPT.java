package example.dto;

public enum CQLOPT {
  KEYSPACE("-create-keyspace.cql", "Keyspace"),
  CREATE("-create-table.cql", "Create"),
  INDEX("-index-table.cql", "Index"),
  INSERT("-insert-table.cql", "Insert"),
  SELECT("-select-table.cql", "Select"),
  VIEW("-create-view.cql", "View");

  public final String cqlFile;
  public final String operation;

  private CQLOPT(String file, String op)
  {
    this.cqlFile = file;
    this.operation = op;
  }

}
