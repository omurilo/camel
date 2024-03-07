package RinhaBackend::Camel;

use strict;
use warnings;

use Slick;
use JSON;
use DBI;
use DateTime;
use DateTimeX::TO_JSON formatter => 'DateTime::Format::RFC3339';

my $s = Slick->new;

$s->get('/clientes/{id}/extrato' => \&statement);
$s->post('/clientes/{id}/transacoes' => \&transaction);

my %dbhs;

for (1..5) {
  my $dbh = DBI->connect("DBI:SQLite:dbname=database/$_.db", "", "", { RaiseError => 1 }) or die $DBI::errstr;

  $dbh->do('PRAGMA journal_mode = WAL;');
  $dbh->do('PRAGMA threads = 32;');
  $dbh->do('PRAGMA temp_store = MEMORY;');
  $dbh->do("PRAGMA mmap_size = 30000000000;");
  $dbh->do("PRAGMA page_size = 32768;");
  $dbh->do('PRAGMA synchronous = OFF;');

  # $dbh->do(qq(DROP TABLE IF EXISTS accounts));
  # $dbh->do(qq(DROP TABLE IF EXISTS transactions));

  my $create_accs_table = qq(CREATE TABLE IF NOT EXISTS accounts(
    id INTEGER PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    limit_amount INTEGER NOT NULL,
    balance INTEGER NOT NULL DEFAULT 0,
    UNIQUE(id)
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

  my $create_acc = qq(INSERT OR IGNORE INTO accounts (id, name, limit_amount)
    VALUES
      (1, 'o barato sai caro', 1000 * 100),
      (2, 'zan corp ltda', 800 * 100),
      (3, 'les cruders', 10000 * 100),
      (4, 'padaria joia de cocaia', 100000 * 100),
      (5, 'kid mais', 5000 * 100));

  my $validate_balance_before_insert_transaction = qq(
    CREATE TRIGGER if not exists validate_balance_before_insert_transaction
    BEFORE INSERT ON transactions
    BEGIN
      SELECT CASE WHEN (a.balance + (CASE WHEN NEW.transaction_type = 'c' THEN NEW.amount ELSE -NEW.amount END)) < -a.limit_amount
        THEN RAISE (ABORT, 'Invalid value')
      END
      FROM (SELECT balance, limit_amount FROM accounts) AS a;

      UPDATE accounts
      SET balance = accounts.balance + (case when NEW.transaction_type = 'c' then +NEW.amount else -NEW.amount end);
    END;
  );

  $dbh->do($create_accs_table);
  $dbh->do($crate_transactions_table);
  $dbh->do($create_acc);
  $dbh->do($validate_balance_before_insert_transaction);

  $dbhs{$_} = $dbh;
}


sub statement {
  my ($app, $context) = @_;

  my $id = $context->params->{'id'};

  unless ($id >= 1 && $id <= 5) {
    $context->status(404)->text('Client not found');
    return;
  }

  my $dbh = $dbhs{$id};

  my $stmt = qq(SELECT
      limit_amount as limite,
      balance as total,
      datetime('now', 'localtime') as data_extrato
      FROM accounts
      WHERE id = $id
  );
  my $t_stmt = qq(SELECT
      amount as valor,
      description as descricao,
      transaction_type as tipo,
      datetime('now', 'localtime') as realizada_em
      FROM transactions
      WHERE account_id = $id
      ORDER BY id DESC
      LIMIT 10
  );
  my $sth = $dbh->prepare($stmt);
  my $t_sth = $dbh->prepare($t_stmt);

  eval {
    $dbh->do('begin exclusive');
    my $rv = $sth->execute() or die $DBI::errstr;

    my $balance = $sth->fetchrow_hashref();
    my $t_rv = $t_sth->execute();

    my $ten_transactions = $t_sth->fetchall_arrayref({});
    $dbh->do('commit');

    my %resp = (
      saldo => $balance,
      ultimas_transacoes => $ten_transactions,
    );

    $context->json(\%resp);
  };

  if ($@) {
    $context->status(500);
  }
}

sub transaction {
  my ($app, $context) = @_;

  my $id = $context->params->{'id'};

  unless ($id >= 1 && $id <= 5) {
    $context->status(404);
    return;
  }

  my $body = $context->request->content;
  my $json = JSON->new->convert_blessed(1);
  my $transaction = $json->decode($body);

  unless (validate_transaction($transaction)){
    $context->status(422);
    return;
  }

  my $dbh = $dbhs{$id};

  my $t_stmt = qq(INSERT INTO transactions (transaction_type, amount, description, account_id) values (?, ?, ?, ?));
  my $t_sth = $dbh->prepare($t_stmt);
  my $stmt = qq(SELECT limit_amount as limite, balance as saldo FROM accounts WHERE id = ?);
  my $sth = $dbh->prepare($stmt);

  eval {
    my $balance;

    eval {
      $t_sth->execute($transaction->{'tipo'}, $transaction->{'valor'}, $transaction->{'descricao'}, $id) or die $DBI::errstr;
      $sth->execute($id);
      $balance = $sth->fetchrow_hashref();
    };

    if ($@) {
      $context->status(422);
      return;
    }

    $context->json($balance);
  };

  if ($@) {
    $context->status(500);
  }
}

sub is_int {
    my $str = $_[0];
    $str =~ s/^\s+|\s+$//g;

    if ($str =~ /^(\-|\+)?\d+?$/) {
        return 1;
    }

    return 0;
}

sub validate_transaction {
  my $transaction = shift;

  unless ($transaction->{'descricao'}) {
    return 0;
  }

  unless (is_int($transaction->{'valor'})) {
    return 0;
  }

  if (
    $transaction->{'tipo'} ne 'c' &&
    $transaction->{'tipo'} ne 'd' ||
    length($transaction->{'descricao'}) < 1 ||
    length($transaction->{'descricao'}) > 10
  ) {
    return 0;
  }

  return 1;
}

my $PORT = defined($ENV{'PORT'}) ? $ENV{'PORT'} : 9999;
$s->run(port => $PORT);
