# DBIish::Pool - Database connection pooling for DBIish #

## SYNOPSIS ##

    my %connection-parameters = database => 'foo', user => 'bar', password => secret();
    my $pool = DBIish.new(driver => 'Pg', initial-size => 1, max-connections => 10, min-spare-connections => 1,
                         max-idle-duration => Duration.new(60), |%connection-parameters);

    my $dbh = $pool.get-connection()

    $dbh.do({SELECT 1});

    $dbh.dispose;


## DESCRIPTION ##

This module is useful for apps supporting multiple parallel users which arrive at an inconsistent rate, such as a
web application. In addition to connection reuse it allow configuring  a maximum number of simultaneous connections to
ensure the database does not go over capacity.

Some `DBIish` drivers, like `Pg`, allow database connection reuse. This improves performance significantly
for very simple transactions, or long distance networks using SSL encrypted connections.
300% has been seen within the same network for web requests, where each request was establishing
a new connection.

`DBIish` drivers which do not support connection reuse can still benefit from pooling as `DBIish::Pool` connects
early to allow queries to be performed immediately, and enforces limits to help prevent Denial-Of-Service of the
database. The statistics can also be useful for monitoring purposes.

To use, create a pool, then take a connection from that pool. The connection is returned to the pool when
dispose is called. Calling dispose is important as otherwise you may exhaust the pool due to garbage collection
being unpredictable.

See your database driver for a description of the connection parameters allowed. These are the same as the
`DBIish.connect` call.

    my $pool = DBIish::Pool.new(driver => 'Pg', max-connections => 20, max-idle-duration => Duration.new(60),
        min-spare-connections => 3,  initial-size => 5, |%dbiish-connection-parameters);

    sub do-db-work() {
      my $dbh = $pool.get-connection();

      my $sth = $dbh.prepare(q{ SELECT session_state FROM sessions WHERE session_id = ? });

      $sth.execute($session-id);
      my $ret = $sth.allrows();
      $dbh.dispose

      return $ret;
    }

### `new` ###

 - `min-spare-connections`
 
   The number of idle connections to keep around. These are ready for immediate use. Busy multi-threaded workloads
   will want to raise this above 1 as there is a short time between when a connection is disposed and when it
   will be ready for use again.

 - `initial-size`

   The number of connections to create when the pool is established. This may be kept equal to `min-spare-connections`
   unless your app requires a very fast initial response time and is regularly restarted during peak periods.
   
 - `max-idle-duration`
 
   Connections which have not been used in this time period will be slowly closed, unless required to meet the 
   `min-spare-connections`.
   
 - `max-connections`
 
   Maximum number of database connections, including those currently being scrubbed of session state for reuse.
   Overall performance is often better if the database has a consistent load and spikes are smoothed out.

 - `|%dbiish-connection-parameters` are whatever `DBIish` allows. For a pool for a PostgreSQL driver might be established
   like this:
 
   ```
   my $pool = DBIish::Pool.new(driver => 'Pg', dbname => 'dbtest', user => 'postgres', port => 5432, max-connections => 20);
   ```

### `get-connection` ###

Returns a connection from the pool, establishing a new connection if necessary, when one becomes available. The
connection is checked to confirm it is alive/active prior to giving it to the caller.

    my $dbh = $pool.get-connection();

Once `max-connections` is reached, this routine will block until a connection becomes available. Ensure you
call `dispose` after finished using the connection to shorten the idle timeframe as garbage collection is
not immediate.

If preferred, you may obtain a connection asynchronously.

    my $dbh = await $pool.get-connection(:async);

### Pool Statistics ###

A small Hash with pool connection statistics is available. This can be useful for automated monitoring purposes.

    my %stats = $pool.stats();

Statistics fields include:

  * inuse ➡ Number of connections currently in use.
  * idle ➡ Number available for immediate use.
  * starting ➡ Number starting up.
  * scrub ➡ Numbers recently disposed, currently being scrubbed.
  * total ➡ Total of the `inuse`, `idle`, `starting`, and `scrub` counters. Due to short race conditions, this may not add up at times.
  * waiting ➡ Number of unfilled `connect` calls.


