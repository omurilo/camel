package RinhaBackend::BadCamel;

use strict;
use warnings;
use threads;
use threads::shared;

use Thread::Semaphore;
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

my $d = HTTP::Daemon->new(LocalAddr => "0.0.0.0", LocalPort => 9999, Listen => 20) || die "Error when start server";

print "Web Server started!\n";
print "Server Address: ", $d->sockhost(), "\n";
print "Server Port: ", $d->sockport(), "\n";

my $s = Thread::Semaphore->new(0);

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

  $s->up();

  unless ($accounts->{$id}) {
    $c->send_status_line(HTTP_UNPROCESSABLE_ENTITY);
    return;
  }

  # if (is_shared($accounts->{$id})) {
  #   print "Yey $accounts->{$id} is shared!";
  #   lock(%{ $accounts->{$id} });
  # }

  my %balance = (
    'total' => $accounts->{$id}->{'balance'},
    'data_extrato' => DateTime->now,
    'limite' => $accounts->{$id}->{'limit'}
  );

  my @ten_transactions = reverse(@{$accounts->{$id}{'transactions'}});

  my $json = JSON->new->convert_blessed(1);
  my %resp = (
    saldo => \%balance,
    ultimas_transacoes => \@ten_transactions,
  );

  my $response = HTTP::Response->new(HTTP_OK);
  $response->content($json->encode(\%resp));
  $response->header("Content-Type" => "application/json");

  $s->down();
  $c->send_response($response);
}

sub transaction {
  my $c = shift;
  my $id = shift;
  my $body = shift;
  
  $s->up();

  unless ($accounts->{$id}) {
    $c->send_status_line(HTTP_UNPROCESSABLE_ENTITY);
    return;
  }

  # if (is_shared($accounts->{$id})) {
  #   print "Yey $accounts->{$id} is shared!";
  #   lock(%{ $accounts->{$id} });
  # }

  my $json = JSON->new->convert_blessed(1);

  my $transaction = &share({});
  my $data = $json->decode($body);

  foreach my $family ( keys %{ $data } ) {
      $transaction->{$family} = $data->{$family};
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

  push @{$accounts->{$id}->{'transactions'}}, $transaction;

  my %resp = (
    limite => $accounts->{$id}->{'limit'},
    saldo => $accounts->{$id}->{'balance'}
  );

  my $json_text = $json->encode(\%resp);

  my $response = HTTP::Response->new(HTTP_OK);
  $response->content($json_text);
  $response->header("Content-Type" => "application/json");

  $s->down();
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
