use v6;
use Test;

use DBIish;
use DBIish::Pool;

plan 3;

# If env var set, no parameter needed.
my %con-parms = :database<dbdishtest>, :user<testuser>, :password<testpass>;
%con-parms<host> = %*ENV<MYSQL_HOST> if %*ENV<MYSQL_HOST>;


# Test connection
my $dbh;
try {
    $dbh = DBIish.connect('mysql', |%con-parms);
    $dbh.dispose;
    CATCH {
        when X::DBIish::LibraryMissing | X::DBDish::ConnectionFailed {
            diag "$_\nCan't continue.";
        }
        default { .throw; }
    }
}
without $dbh {
    skip-rest 'prerequisites failed';
    exit;
}

my constant MAX_CONNECTIONS = 3;
my constant INITIAL_SIZE = 2;
my constant MIN_SPARE = 1;

my $pool = DBIish::Pool.new(driver => 'mysql', initial-size => INITIAL_SIZE, min-spare-connections => MIN_SPARE, max-connections => MAX_CONNECTIONS, |%con-parms);

# Let the pool initialize
await Promise.in(1);

subtest {
    $dbh = $pool.get-connection();

    # Confirm it's a real DBH
    my $sth = $dbh.prepare(q{SELECT 1 + 1 AS result});
    $sth.execute();
    my $row = $sth.row(:hash);
    is $row<result>, 2, 'Has a live connection';

    my $stats = $pool.stats;
    is $stats<total>, INITIAL_SIZE, 'Initial-size of connections';
    is $stats<starting>, 0, 'Connections all started';
    is $stats<inuse>, 1, 'Single connection in-use';
    is $stats<scrub>, 0, 'No connections being scrubbed';
    is $stats<idle>, INITIAL_SIZE - 1, 'Count Idle connections';
}, 'Initialize';

# MySQL does not support connection reuse. The pool shrinks to min-spare-connections in size
subtest {
    $dbh.dispose;

    my $stats = $pool.stats;
    is $stats<total>, MIN_SPARE, '%d connections'.sprintf(MIN_SPARE);
    is $stats<inuse>, 0, 'No connections in use';
}, 'Dispose';

subtest {
    my @connections;
    for ^MAX_CONNECTIONS {
        my $dbh = $pool.get-connection;
        @connections.push($dbh);
    }
    my $stats = $pool.stats;
    is $stats<total>, MAX_CONNECTIONS, 'Connection Limit';

    # Connect waits
    my $timed-out = True;
    my $wait-conn;
    my $p = start {
        $wait-conn = $pool.get-connection();
        $timed-out = False;
    };
    await Promise.anyof($p, Promise.in(1));
    ok !$wait-conn.defined, 'Connection not created';
    ok $timed-out, 'Connect waits after hitting Max Connections';


    # Thread eventually finishes
    @connections.pop.dispose;
    await Promise.anyof($p, Promise.in(1));
    ok $wait-conn.defined, 'Connection eventually obtained';
    ok !$timed-out, 'Connect obtained, clear timeout';

    # Async connect gives results when it can.
    my $pconn1 = $pool.get-connection(:async);
    my $pconn2 = $pool.get-connection(:async);
    is $pconn1.status, Planned, 'Async 1 is pending';
    is $pconn2.status, Planned, 'Async 2 is pending';

    # Dispose of connection. After a bit of time to refresh, conn1 should
    # have a connection.
    $wait-conn.dispose;
    await Promise.anyof($pconn1, Promise.in(5));
    is $pconn1.status, Kept, 'Asynce 1 has connection';
    is $pconn2.status, Planned, 'Async 2 is still pending';

    $stats = $pool.stats;
    is $stats<total>, MAX_CONNECTIONS, 'Connection Limit';

    my $connection;
    $p = start {
        $pool.get-connection();
    }
    await Promise.anyof($p, Promise.in(2));
    ok not $connection.defined, 'Connect() blocked at limit';
}, 'Connect blocks at limit';

#`[[

# Purposfully hold off connecting until mutiple threads are running. This trips up the driver
# loading mechanism in a way that 43-sqlite-threads.t misses.

my $skip-tests = False;
my @promises = do for ^5 -> $thread {
    start {
        my $dbh;
        try {
            $dbh = DBIish.connect('Pg', |%con-parms);
            CATCH {
                when X::DBIish::LibraryMissing | X::DBDish::ConnectionFailed {
                    diag "$_\nCan't continue.";
                }
                default { .rethrow; }
            }
        }
        # Skip work if there is no connection
        if $dbh {
            # Keep queries active by having them in sleep
            my $sth = $dbh.prepare('SELECT pg_sleep(0.3)');
            for ^4 {
                $sth.execute();
            }
            $sth.finish;
            $dbh.dispose;
        } else {
            $skip-tests = True;
        }
    }
}
await @promises;

if ($skip-tests) {
    skip-rest 'prerequisites failed';
} else {
    pass 'Pass multithread multiconnection survival test';
}

]]

done-testing;
