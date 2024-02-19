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


my $accounts = &share({});
$accounts->{1} = new_client((
    {"limit" => 100000},
    {"balance" => 0},
    {"transactions" => &share([]) },
));
$accounts->{2} = new_client((
  {"limit" => 80000},
  {"balance" => 0},
  {"transactions" => &share([])}
));
$accounts->{3} = new_client((
    {"limit" => 1000000},
    {"balance" => 0},
    {"transactions" => &share([])},
));
$accounts->{4} = new_client((
  {"limit" => 10000000},
  {"balance" => 0},
  {"transactions" => &share([])},
));
$accounts->{5} = new_client((
    {"limit" => 500000},
    {"balance" => 0},
    {"transactions" => &share([])},
));

while (my $c = $d->accept) {
  threads->create(\&process_one_req, $c);
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

  lock($accounts);
  
  my %balance = (
    'total' => $accounts->{$id}->{'balance'},
    'data_extrato' => DateTime->now,
    'limite' => $accounts->{$id}->{'limit'}
  );

  my @ten_transactions = reverse(@{ $accounts->{$id}->{'transactions'} });

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

  lock($accounts);

  my $json = JSON->new->convert_blessed(1);

  my $transaction = &share({});
  my $data = $json->decode($body);

  foreach my $family ( keys %{ $data } ) {
      $transaction->{$family} = $data->{$family};
  }

  unless (is_int($transaction->{'valor'})) {
    $c->send_status_line(HTTP_UNPROCESSABLE_ENTITY);
    return;
  }

  unless ($transaction->{'descricao'}) {
    $c->send_status_line(HTTP_UNPROCESSABLE_ENTITY);
    return;
  }

  if ($transaction->{'tipo'} ne "c" && $transaction->{'tipo'} ne "d") {
    $c->send_status_line(HTTP_UNPROCESSABLE_ENTITY);
    return;
  }

  if (length($transaction->{'descricao'}) > 10 || length($transaction->{'descricao'}) < 1) {
    $c->send_status_line(HTTP_UNPROCESSABLE_ENTITY);
    return;
  }

  if ($transaction->{'tipo'} eq "c") {
    $accounts->{$id}->{'balance'} += $transaction->{'valor'};
  }

  if ($transaction->{'tipo'} eq "d") {

    unless ($accounts->{$id}->{'limit'} + $accounts->{$id}->{'balance'} >= $transaction->{'valor'}) {
      $c->send_status_line(HTTP_UNPROCESSABLE_ENTITY);
      return;
    }

    $accounts->{$id}->{'balance'} -= $transaction->{'valor'};  
  }

  push @{ $accounts->{$id}->{'transactions'} }, $transaction;

  my %resp = (
    limite => $accounts->{$id}->{'limit'},
    saldo => $accounts->{$id}->{'balance'}
  );

  my $json_text = $json->encode(\%resp);

  my $response = HTTP::Response->new(HTTP_OK);
  $response->content($json_text);
  $response->header("Content-Type" => "application/json");

  $c->send_response($response);
}

sub new_client {
  my $client = &share({});
  my @AoH = @_;
  for my $i ( 0 .. $#AoH) {
    for my $role ( keys %{ $AoH[$i] } ) {
      $client->{$role} = $AoH[$i]{$role};
    }
  }

  return $client;
}

sub is_int { 
    my $str = $_[0]; 
    $str =~ s/^\s+|\s+$//g;          

    if ($str =~ /^(\-|\+)?\d+?$/) {
        return 1;
    }

    return 0;
}

foreach my $thr (threads->list()) {
    $thr->join();
}
