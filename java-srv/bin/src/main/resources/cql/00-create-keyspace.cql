CREATE KEYSPACE IF NOT EXISTS citizix
  WITH REPLICATION = {
   'class' : 'SimpleStrategy',
   'replication_factor' : 1
  };
