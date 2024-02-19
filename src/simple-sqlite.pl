package RinhaBackend::Camel;

use strict;
use warnings;
use threads::shared;

use HTTP::Server::Simple::CGI;
use base qw(HTTP::Server::Simple::CGI);
use JSON;
use DBI;
use DateTime;
use DateTimeX::TO_JSON formatter => 'DateTime::Format::RFC3339';
use Tie::RegexpHash;

my %routers;

tie %routers, 'Tie::RegexpHash';

$routers{ qr/^GET\s\/clientes\/(\d+)\/extrato$/i } = \&statement;
$routers{ qr/^POST\s\/clientes\/(\d+)\/transacoes$/i } = \&transaction;

my %dbhs;

for (1..5) {
  my $dbh = DBI->connect("DBI:SQLite:dbname=database/$_.db", "", "", { RaiseError => 1 }) or die $DBI::errstr;

  $dbh->do('PRAGMA journal_mode = WAL;');
  $dbh->do('PRAGMA threads = 4;');
  $dbh->do('PRAGMA busy_timeout = 30000;');
  $dbh->do('PRAGMA temp_store = MEMORY;');
  $dbh->do('PRAGMA cache_size = 10000;');
  $dbh->do('PRAGMA auto_vacuum = FULL;');
  $dbh->do('PRAGMA automatic_indexing = TRUE;');
  $dbh->do('PRAGMA count_changes = FALSE;');
  $dbh->do('PRAGMA encoding = "UTF-8";');
  $dbh->do('PRAGMA ignore_check_constraints = TRUE;');
  $dbh->do('PRAGMA incremental_vacuum = 0;');
  $dbh->do('PRAGMA legacy_file_format = FALSE;');
  $dbh->do('PRAGMA optimize = On;');
  $dbh->do('PRAGMA synchronous = NORMAL;');

  $dbh->do(qq(DROP TABLE IF EXISTS accounts));
  $dbh->do(qq(DROP TABLE IF EXISTS transactions));

  my $create_accs_table = qq(CREATE TABLE IF NOT EXISTS accounts(
    id INTEGER PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    limit_amount INTEGER NOT NULL,
    balance INTEGER NOT NULL DEFAULT 0
  ));

  my $crate_transactions_table = qq(CREATE TABLE IF NOT EXISTS transactions(
    id INTEGER PRIMARY KEY,
    account_id INTEGER NOT NULL,
    amount INTEGER NOT NULL,
    transaction_type CHAR(1) NOT NULL,
    description VARCHAR(10) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_accounts_transactions_id
    FOREIGN KEY (account_id) REFERENCES accounts(id) ON DELETE CASCADE
  ));

  my $create_acc = qq(INSERT INTO accounts (name, limit_amount)
    VALUES
      ('o barato sai caro', 1000 * 100),
      ('zan corp ltda', 800 * 100),
      ('les cruders', 10000 * 100),
      ('padaria joia de cocaia', 100000 * 100),
      ('kid mais', 5000 * 100));

  $dbh->do($create_accs_table);
  $dbh->do($crate_transactions_table);
  $dbh->do($create_acc);

  $dbhs{$_} = $dbh;
}

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

  my $dbh = $dbhs{$id};

  my $stmt = qq(SELECT limit_amount, balance FROM accounts WHERE id = $id);
  my $t_stmt = qq(SELECT amount as valor, description as descricao, transaction_type as tipo from transactions where account_id = $id order by id desc limit 10);
  my $sth = $dbh->prepare($stmt);
  my $t_sth = $dbh->prepare($t_stmt);
 
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

  my $dbh = $dbhs{$id};

  my $stmt = qq(SELECT limit_amount, balance FROM accounts WHERE id = $id);
  my $sth = $dbh->prepare($stmt);
  
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

  $dbh->do('begin exclusive transaction');
  my $t_stmt = qq(INSERT INTO transactions (transaction_type, amount, description, account_id) values (?, ?, ?, ?));
  my $t_sth = $dbh->prepare($t_stmt);
  my $a_stmt = qq(UPDATE accounts SET balance=? WHERE id = $id);
  my $a_sth = $dbh->prepare($a_stmt);
  
  my $t_rv = $t_sth->execute($transaction->{'tipo'}, $transaction->{'valor'}, $transaction->{'descricao'}, $id) or die $DBI::errstr;
  $a_sth->execute($row->[1]) or die $DBI::errstr;
  $dbh->do('commit transaction');

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

my $PORT = defined($ENV{'PORT'}) ? $ENV{'PORT'} : 9999;

RinhaBackend::Camel->new($PORT)->run();
