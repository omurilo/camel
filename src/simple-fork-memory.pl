package RinhaBackend::Camel;

use strict;
use warnings;
use threads::shared;

use HTTP::Server::Simple::CGI::PreFork;
use base qw(HTTP::Server::Simple::CGI::PreFork);
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
  
  unless ($accounts->{$id}) {
    print "HTTP/1.1 404 Not Found\r\n";
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

  print "HTTP/1.1 200 OK\r\n";
  print $cgi->header("application/json");
  print $json->encode(\%resp);
}

sub transaction {
  my $cgi = shift;
  return if !ref $cgi;

  my $id = shift;
  
  unless ($accounts->{$id}) {
    print "HTTP/1.1 404 Not Found\r\n";
    return;
  }

  lock($accounts);

  my $json = JSON->new->convert_blessed(1);

  my $body = $cgi->param('POSTDATA');
  my $transaction = $json->decode($body);

  unless (is_int($transaction->{'valor'})) {
    print "HTTP/1.1 422 Unprocessable Entity\r\n";
    return;
  }

  unless ($transaction->{'descricao'}) {
    print "HTTP/1.1 422 Unprocessable Entity\r\n";
    return;
  }

  if ($transaction->{'tipo'} ne "c" && $transaction->{'tipo'} ne "d") {
    print "HTTP/1.1 422 Unprocessable Entity\r\n";
    return;
  }

  if (length($transaction->{'descricao'}) > 10 || length($transaction->{'descricao'}) < 1) {
    print "HTTP/1.1 422 Unprocessable Entity\r\n";
    return;
  }

  if ($transaction->{'tipo'} eq "c") {
    $accounts->{$id}->{'balance'} += $transaction->{'valor'};
  }

  if ($transaction->{'tipo'} eq "d") {

    unless ($accounts->{$id}->{'limit'} + $accounts->{$id}->{'balance'} >= $transaction->{'valor'}) {
      print "HTTP/1.1 422 Unprocessable Entity\r\n";
      return;
    }

    $accounts->{$id}->{'balance'} -= $transaction->{'valor'};  
  }

  my @transactions = $accounts->{$id}->{'transactions'};

  push @{ $accounts->{$id}->{'transactions'} }, $transaction;

  my %resp = (
    limite => $accounts->{$id}->{'limit'},
    saldo => $accounts->{$id}->{'balance'}
  );

  print "HTTP/1.1 200 OK\r\n";
  print $cgi->header("application/json");
  print $json->encode(\%resp);
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

RinhaBackend::Camel->new(9999)->run(prefork => 1);
