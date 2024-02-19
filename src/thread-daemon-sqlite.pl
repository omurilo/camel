package RinhaBackend::BadCamel;

use strict;
use warnings;
use threads;
use threads::shared;

use DBI;
use HTTP::Daemon;
use HTTP::Response;
use HTTP::Status qw(:constants :is status_message);

use JSON;
use DateTime;
use DateTimeX::TO_JSON formatter => 'DateTime::Format::RFC3339';
use Tie::RegexpHash;

my %routers;

tie %routers, 'Tie::RegexpHash';

$routers{ qr/^GET\s\/clientes\/(\d+)\/extrato$/i } = \&statement;
$routers{ qr/^POST\s\/clientes\/(\d+)\/transacoes$/i } = \&transaction;

my $PORT = defined($ENV{'PORT'}) ? $ENV{'PORT'} : 9999;
my $d = HTTP::Daemon->new(LocalAddr => "0.0.0.0", LocalPort => $PORT, Listen => 20) or die "Error when start server";

print "Web Server started!\n";
print "Server Address: ", $d->sockhost(), "\n";
print "Server Port: ", $d->sockport(), "\n";

while (my $c = $d->accept) {
  threads->create(\&process_one_req, $c)->detach();
}

sub process_one_req {
    my $c = shift;
    my $r = $c->get_request;
    if ($r) {
      my $method = $r->method;
      my $path = $r->url->path();
      my $handler = $routers{"$method $path"};

      my $id = "";

      if ($path =~ m/\/clientes\/(\d+)\/\w+/i) {
        $id = "$1";
      }

      my $body = $r->content;

      if (ref($handler) eq "CODE") {
        $handler->($c, $id, $body);
      } else {
        $c->send_status_line(HTTP_NOT_FOUND);
      }
    }
    $c->close;
    undef($c);
}

sub statement {
  my $c = shift;
  my $id = shift;

  unless ($id >= 1 && $id <= 5) {
    $c->send_status_line(HTTP_NOT_FOUND);
    return;
  }

  my $dbh = DBI->connect("DBI:SQLite:dbname=db.sqlite3", undef, undef, { RaiseError => 1 }) or die $DBI::errstr;
  
  my $stmt = qq(SELECT limit_amount, balance FROM accounts WHERE id = $id);
  my $sth = $dbh->prepare($stmt);
  my $rv = $sth->execute() or die $DBI::errstr;

  my $row = $sth->fetch;
  unless ($row) {
    $c->send_status_line(HTTP_NOT_FOUND);
    return;
  }

  my %balance = (
    'total' => $row->[1],
    'data_extrato' => DateTime->now,
    'limite' => $row->[0]
  );

  my $t_stmt = qq(SELECT amount as valor, description as descricao, transaction_type as tipo from transactions where account_id = $id order by id desc limit 10);
  my $t_sth = $dbh->prepare($t_stmt);
  my $t_rv = $t_sth->execute();

  my @ten_transactions = ();

  while (my @transaction = $t_sth->fetchrow_array()) {
    my $t = ();
    $t->{'tipo'} = $transaction[2];
    $t->{'valor'} = $transaction[0];
    $t->{'descricao'} = $transaction[1];
    
    push @ten_transactions, $t;
  }

  my $json = JSON->new->convert_blessed(1);
  my %resp = (
    saldo => \%balance,
    ultimas_transacoes => \@ten_transactions,
  );

  my $response = HTTP::Response->new(HTTP_OK);
  $response->content($json->encode(\%resp));
  $response->header("Content-Type" => "application/json");

  $c->send_response($response);
}

sub transaction {
  my $c = shift;
  my $id = shift;
  my $body = shift;
  
  unless ($id >= 1 && $id <= 5) {
    $c->send_status_line(HTTP_NOT_FOUND);
    return;
  }

  my $json = JSON->new->convert_blessed(1);
  my $transaction = $json->decode($body);

  unless ($transaction->{'descricao'}) {
    $c->send_status_line(HTTP_UNPROCESSABLE_ENTITY);
    return;
  }

  unless (is_int($transaction->{'valor'})) {
    $c->send_status_line(HTTP_UNPROCESSABLE_ENTITY);
    return;
  }

  if (
    $transaction->{'tipo'} ne 'c' &&
    $transaction->{'tipo'} ne 'd' ||
    length($transaction->{'descricao'}) < 1 ||
    length($transaction->{'descricao'}) > 10
  ) {
    $c->send_status_line(HTTP_UNPROCESSABLE_ENTITY);
    return;
  }

  my $dbh = DBI->connect("DBI:SQLite:dbname=db.sqlite3", undef, undef, { RaiseError => 1 }) or die $DBI::errstr;
  
  my $stmt = qq(SELECT limit_amount, balance FROM accounts WHERE id = $id);
  my $sth = $dbh->prepare($stmt);
  my $rv = $sth->execute() or die $DBI::errstr;

  my $row = $sth->fetch;

  unless ($row) {
    $c->send_status_line(HTTP_NOT_FOUND);
    return;
  }
 
  if ($transaction->{'tipo'} eq "c") {
    $row->[1] += $transaction->{'valor'};
  }

  if ($transaction->{'tipo'} eq "d") {
    unless ($row->[0] + $row->[1] >= $transaction->{'valor'}) {
      $c->send_status_line(HTTP_UNPROCESSABLE_ENTITY);
      return;
    }

    $row->[1] -= $transaction->{'valor'};  
  }

  my $t_stmt = qq(INSERT INTO transactions (transaction_type, amount, description, account_id) values (?, ?, ?, ?));
  my $t_sth = $dbh->prepare($t_stmt);
  my $t_rv = $t_sth->execute($transaction->{'tipo'}, $transaction->{'valor'}, $transaction->{'descricao'}, $id) or die $DBI::errstr;
  
  my $a_stmt = qq(UPDATE accounts SET balance=? WHERE id = $id);
  $dbh->prepare($a_stmt)->execute($row->[1]);
  
  my %resp = (
    limite => $row->[0],
    saldo => $row->[1],
  );

  my $json_text = $json->encode(\%resp);

  my $response = HTTP::Response->new(HTTP_OK);
  $response->content($json_text);
  $response->header("Content-Type" => "application/json");

  $c->send_response($response);
}

sub is_int { 
    my $str = $_[0]; 
    $str =~ s/^\s+|\s+$//g;          

    if ($str =~ /^(\-|\+)?\d+?$/) {
        return 1;
    }

    return 0;
}
