package RinhaBackend::Camel;

use strict;
use warnings;
use threads::shared;

use HTTP::Server::Simple::CGI::PreFork;
use base qw(HTTP::Server::Simple::CGI::PreFork);
use JSON;
use DBI;
use DateTime;
use DateTimeX::TO_JSON formatter => 'DateTime::Format::RFC3339';
use Tie::RegexpHash;

my %routers;

tie %routers, 'Tie::RegexpHash';

$routers{ qr/^GET\s\/clientes\/(\d+)\/extrato$/i } = \&statement;
$routers{ qr/^POST\s\/clientes\/(\d+)\/transacoes$/i } = \&transaction;

my $dbh_w = DBI->connect("DBI:SQLite:dbname=db.sqlite3", "", "", { RaiseError => 1 }) or die $DBI::errstr;
my $dbh_r = DBI->connect("DBI:SQLite:dbname=db.sqlite3", "", "", { RaiseError => 1 }) or die $DBI::errstr;

$dbh_w->prepare('PRAGMA journal_mode = WAL;')->execute;
$dbh_r->prepare('PRAGMA journal_mode = WAL;')->execute;
$dbh_w->prepare('PRAGMA threads = 4;')->execute;
$dbh_w->prepare('PRAGMA busy_timeout = 30000;')->execute;
$dbh_w->prepare('PRAGMA temp_store = MEMORY;')->execute;
$dbh_w->prepare('PRAGMA cache_size = 10000;')->execute;
$dbh_w->prepare('PRAGMA auto_vacuum = FULL;')->execute;
$dbh_w->prepare('PRAGMA automatic_indexing = TRUE;')->execute;
$dbh_w->prepare('PRAGMA count_changes = FALSE;')->execute;
$dbh_w->prepare('PRAGMA encoding = "UTF-8";')->execute;
$dbh_w->prepare('PRAGMA ignore_check_constraints = TRUE;')->execute;
$dbh_w->prepare('PRAGMA incremental_vacuum = 0;')->execute;
$dbh_w->prepare('PRAGMA legacy_file_format = FALSE;')->execute;
$dbh_w->prepare('PRAGMA optimize = On;')->execute;
$dbh_w->prepare('PRAGMA synchronous = NORMAL;')->execute;

sub handle_request {
    my $self = shift;
    my $cgi  = shift;
   
    my $path = $cgi->path_info();
    my $method = $cgi->request_method();
    my $handler = $routers{"$method $path"};

    my $id = "";

    if ($path =~ m/\/clientes\/(\d+)\/\w+/i) {
      $id = "$1";
    }

    if (ref($handler) eq "CODE") {
      $handler->($cgi, $id);
    } else {
      print "HTTP/1.1 404 Not found\r\n";
    }
}

sub statement {
  my $cgi = shift;   # CGI.pm object
  return if !ref $cgi;
  
  my $id = shift;
 
  unless ($id >= 1 && $id <= 5) {
    print "HTTP/1.1 404 NOT FOUND\r\n";
    return;
  }

  # my $dbh = DBI->connect("DBI:SQLite:dbname=db.sqlite3", "", "", { RaiseError => 1 , AutoCommit => 0, sqlite_use_immediate_transaction => 0}) or die $DBI::errstr;
  
  $dbh_r->do('begin transaction');
  my $stmt = qq(SELECT limit_amount, balance FROM accounts WHERE id = $id);
  my $t_stmt = qq(SELECT amount as valor, description as descricao, transaction_type as tipo from transactions where account_id = $id order by id desc limit 10);
  my $sth = $dbh_r->prepare($stmt);
  my $t_sth = $dbh_r->prepare($t_stmt);
 
  my $rv = $sth->execute() or die $DBI::errstr;

  my $row = $sth->fetch;
  my $t_rv = $t_sth->execute();

  my @ten_transactions = ();

  while (my @transaction = $t_sth->fetchrow_array()) {
    my $t = ();
    $t->{'tipo'} = $transaction[2];
    $t->{'valor'} = $transaction[0];
    $t->{'descricao'} = $transaction[1];

    push @ten_transactions, $t;
  }
 
  $dbh_r->do('commit');
  
  my %balance = (
    'total' => $row->[1],
    'data_extrato' => DateTime->now,
    'limite' => $row->[0]
  );

  my $json = JSON->new->convert_blessed(1);
  my %resp = (
    saldo => \%balance,
    ultimas_transacoes => \@ten_transactions,
  );

  print "HTTP/1.1 200 OK\r\n";
  print $cgi->header("application/json");
  print $json->encode(\%resp);
}

sub transaction {
  my $cgi = shift;
  return if !ref $cgi;

  my $id = shift;
  
  unless ($id >= 1 && $id <= 5) {
    print "HTTP/1.1 404 NOT FOUND\r\n";
    return;
  }

  my $body = $cgi->param('POSTDATA');
  
  my $json = JSON->new->convert_blessed(1);
  my $transaction = $json->decode($body);

  unless ($transaction->{'descricao'}) {
    print "HTTP/1.1 422 Unprocessable Entity\r\n";
    return;
  }

  unless (is_int($transaction->{'valor'})) {
    print "HTTP/1.1 422 Unprocessable Entity\r\n";
    return;
  }

  if (
    $transaction->{'tipo'} ne 'c' &&
    $transaction->{'tipo'} ne 'd' ||
    length($transaction->{'descricao'}) < 1 ||
    length($transaction->{'descricao'}) > 10
  ) {
    print "HTTP/1.1 422 Unprocessable Entity\r\n";
    return;
  }

  # my $dbh = DBI->connect("DBI:SQLite:dbname=db.sqlite3", "", "", { RaiseError => 1, AutoCommit => 0, sqlite_use_immediate_transaction => 0 }) or die $DBI::errstr;
  $dbh_r->do('begin transaction');
  $dbh_w->do('begin transaction');
  my $stmt = qq(SELECT limit_amount, balance FROM accounts WHERE id = $id);
  my $sth = $dbh_r->prepare($stmt);
  
  my $rv = $sth->execute() or die $DBI::errstr;
  my $row = $sth->fetch;
 
  if ($transaction->{'tipo'} eq "c") {
    $row->[1] += $transaction->{'valor'};
  }

  if ($transaction->{'tipo'} eq "d") {
    unless ($row->[0] + $row->[1] >= $transaction->{'valor'}) {
      print "HTTP/1.1 422 Unprocessable Entity\r\n";
      return;
    }

    $row->[1] -= $transaction->{'valor'};  
  }

  my $t_stmt = qq(INSERT INTO transactions (transaction_type, amount, description, account_id) values (?, ?, ?, ?));
  my $t_sth = $dbh_w->prepare($t_stmt);
  my $a_stmt = qq(UPDATE accounts SET balance=? WHERE id = $id);
  my $a_sth = $dbh_w->prepare($a_stmt);
  
  my $t_rv = $t_sth->execute($transaction->{'tipo'}, $transaction->{'valor'}, $transaction->{'descricao'}, $id) or die $DBI::errstr;
  $a_sth->execute($row->[1]) or die $DBI::errstr;

  $dbh_r->do('commit');
  $dbh_w->do('commit');

  my %resp = (
    limite => $row->[0],
    saldo => $row->[1],
  );

  my $json_text = $json->encode(\%resp);

  print "HTTP/1.1 200 OK\r\n";
  print $cgi->header("application/json");
  print $json->encode(\%resp);
}

sub is_int { 
    my $str = $_[0]; 
    $str =~ s/^\s+|\s+$//g;          

    if ($str =~ /^(\-|\+)?\d+?$/) {
        return 1;
    }

    return 0;
}


RinhaBackend::Camel->new(9999)->run(prefork => 1, max_servers => 6);
