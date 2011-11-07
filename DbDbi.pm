#?????? ?????? ? ????? ?????? ??????
package DbDbi;
use strict;

BEGIN
{
    use DBI;
    use Exporter();
    use ErrorHandler;
	
	use FindBin;
	use lib "$FindBin::Bin/";
	use Misc;
	use Loggy;
	
	use constant DB_PGSQL => 0;
	use constant DB_SQLITE => 1;
	
	
    @DbDbi::ISA = qw(Exporter);
    @DbDbi::EXPORT = qw(&db_connect  &db_disconnect &db_query  &db_select_row db_select_all_one
    		db_get_name_by_id db_get_value_by_id
    		&db_get_id_by_name &db_get_id_by_value &db_insert &db_delete_by_id 
    		&db_get_by_id &db_begin_transaction &db_commit_transaction &db_rollback_transaction
    		&db_select_all &db_process &db_select_one &db_select_all_hash
    		&db_select_one_by_id &db_select_row_by_id &db_exec
    		&db_set_encoding DB_PGSQL DB_SQLITE &db_reconnect);
}

my %defs = (
	user => '',
	dbname => '',
	password => '',
	host => '',
	port => '5432',
	dbtype => DB_PGSQL,
	debug => 0
);

my %connect_params;
my $inited = 0;
my $dbh;
my $tryed_reconnect = 0;
sub db_begin_transaction(){#?????? ??????????
	$dbh->{AutoCommit} = 0;
	$dbh->{RaiseError} = 0;
}

sub db_commit_transaction(){#????????? ??????????
	$dbh->commit or abort($dbh->errstr);
	$dbh->{AutoCommit} = 1;
}

sub db_rollback_transaction(){#????? ??????????
	$dbh->rollback or abort($dbh->errstr);
	$dbh->{AutoCommit} = 1;
}

sub db_set_encoding($){
	db_query("set client_encoding TO '".{shift}."'");
}

sub db_get_encoding(){
	return db_select_one("SHOW client_encoding");
}

sub db_get_id_field_name($) {#?????? ???????? ??????? ?????????????? ?? ????? ??????? $1
    my ($table_name) = @_;
    my $id_name = $table_name;
    $id_name =~ s/^(?:e_|r_)(.*)/n_$1/;
    return $id_name;
}

sub db_select_row($) {#????????? ?????? ? ??????? ?????? ?????? ??????????, ??? ?????? ?????? ???? ????? ?? ???????
	my $query = shift;
	my $sth = db_exec( $query );
	my @row_ary = $sth->fetchrow_array;
	$sth->finish();
# 	$sth->err or db_abort( $sth );
	return @row_ary;
}

sub db_select_one($){
	my ($query) = @_;
	my @row = db_select_row($query);
	return shift @row;
}

sub db_process($&;$){
	my ($query, $block, $no_found_msg) = @_;
	my $res = db_query( $query );
	abort($no_found_msg) if( !$res->rows && defined($no_found_msg) );
	my @arr =  @{$res->fetchall_arrayref}; 
#	while( my @row =;){
	for my $row (@arr){
		$block->( @{$row} );
	}
	
	return $res->rows ? 1 : 0;
}

sub db_exec($){#returns sth
	my $query = shift;
	if( $defs{debug}){
		blah "Executing query: $query";
	}
	my $sth = $dbh->prepare($query);
	unless( $sth ){
		if( $sth->err == 7){
			db_abort($dbh) if($tryed_reconnect);
			warn "Disconnected, trying to reconnect and reexec";
			$tryed_reconnect = 1;
			db_reconnect();
			return db_exec($query);
		}else{
			db_abort($dbh);
		}
	}
	
 	my $res = $sth->execute();
	unless( $res ){
		if( $sth->err == 7){
			db_abort($dbh) if($tryed_reconnect);
			$tryed_reconnect = 1;
			db_reconnect();
			return db_exec($query);
		}else{
			db_abort($dbh);
		}
	}
	$tryed_reconnect = 0;	
	return $sth;
}

sub db_select_all($;$){#??????? ?????? ?? ????? ?????????? ??????? $1 (?????? ?????)
	my ($query, $slice) = @_;
	$slice = [] unless( defined($slice));
	my $sth = db_exec( $query );
	my $arr_ref = $sth->fetchall_arrayref($slice);
	return $arr_ref;
}

sub db_select_all_hash($){
	my $query = shift;
	my $sth =db_exec($query);
	my $res = [];
	while(my $h = $sth->fetchrow_hashref() ){
		push @$res, $h;
	}
	return $res;
}

sub db_select_all_one($){
	my $sth = db_exec( shift );
	my @res;
	while( my @row = $sth->fetchrow_array ){
		push @res, shift @row;
	}
	return @res;
}

sub db_connect(%) {#?????????? ? ????? ??????
    my (%hash) = @_;
	%hash = %connect_params unless( %hash );
	
	%hash = merge_hashes_def( \%defs, \%hash);

    my $host = $hash{host};
    my $dbname = $hash{dbname};
    my $user = $hash{user};
    my $password = $hash{password};
    my $port = $hash{port};
  	my $db_type;
	$hash{dbtype} = DB_PGSQL unless( defined( $hash{dbtype} ) );

	$db_type = 'DBI:Pg' if(  $hash{dbtype} == DB_PGSQL);
	$db_type = 'DBI:SQLite' if( $hash{dbtype} == DB_SQLITE);
	
	my $host_str = $host ? "host=$host;" : '';
	my $port_str = $host ? "port=$port;" : '';
    $dbh = DBI->connect("$db_type:dbname=$dbname;
			 $host_str;
			 $port_str", $user, $password,
			 {PrintError => 0} )
			  or abort("DBI Error: $DBI::errstr");
	$inited = 1;
	%connect_params = %hash;
# # 	DBI->trace(3);
}

sub db_reconnect(){
	db_abort("Not connected") unless($dbh);
	db_connect( %connect_params );
}

sub db_disconnect() {#????????????? ? ????? ??????
    $dbh->disconnect(); 
}

sub db_query($) { #????? ????????? ??????? $1 
    my $query_str = shift;
    
    my $sth = db_exec( $query_str);
#     my $res = $sth->execute() or db_abort( $query_str );
    return $sth;
}

sub db_select_row_by_id($$;$){
	my ($table_name, $id, $id_field_name) = @_;
	$id_field_name ||= db_get_id_field_name($table_name);
	return db_select_row("SELECT * FROM $table_name WHERE $id_field_name = $id");
}

sub db_select_one_by_id($$$;$){
	my ($table_name, $field_name, $id, $id_field_name) = @_;
	$id_field_name ||= db_get_id_field_name($table_name);
	return db_select_one("SELECT $field_name FROM $table_name WHERE $id_field_name = $id");
}


sub db_get_id_by_value($$;$) {
    my ($table_name, $value, $field_name) = @_;
    $field_name||='name';
    
    my $id_field_name = db_get_id_field_name($table_name);
    
    my $res = db_query("SELECT $id_field_name FROM $table_name WHERE $field_name = $value;");
    if( !$res->rows) {
        return 0;
    }
    my @row = $res->fetchrow_array;
    my $id = shift( @row );
    
    $res->finish();
    
    return $id;
}

sub db_get_value_by_id($$$;$){
	my ($table_name, $value_field, $id, $id_field_name) = @_;
	$id_field_name = db_get_id_field_name($table_name) unless defined $id_field_name;
	return db_select_one("select $value_field from $table_name where $id_field_name = $id");
}

sub db_get_name_by_id($$;$){
	my ($table_name, $id, $id_field_name) = @_;
	return db_get_value_by_id($table_name, 'name', $id, $id_field_name);
}

sub db_get_id_by_name($$;$) {#??????? id ?????? ?? ??????? $1 ? ????? ?????? $2
    #??? ???? ?? ?????????? ????? 'name' ??? ???????? $3
    #value - ??????
    my ($table_name, $value, $field_name) = @_;
    $field_name||='name';
    
    my $id_field_name = db_get_id_field_name($table_name);
    my $res = db_select_one("SELECT $id_field_name FROM $table_name WHERE $field_name = '$value'");


    return $res;
}

sub db_insert($%) {#????? ????????? ??????? ? ??????? $1 ????? %2
    
    my ($table_name, %fields) = @_;

    my @names;
    my @values; 
    my $name;
    foreach $name ( keys ( %fields )) {
#    	next if($name =~ m/^\d*$/);
        my $value = $fields{$name};

		abort("Value for key $name undefined") unless(defined($value));

        push(@names, $name);
        push(@values, $value);
    }
    if($#names == -1) {
        abort('Empty insert hash');
    }
    
    my $names_str = join(', ', @names);
    my $values_str = join(', ', @values);
    
    my $query_str = "INSERT INTO $table_name ($names_str) VALUES ($values_str);";
    $dbh->do($query_str) or db_abort( $dbh );

    return $dbh->last_insert_id(undef, undef, $table_name, db_get_id_field_name($table_name));
}

sub db_delete_by_id($$;$) {#???????? ?????? ?? ??????? $1 ? id ?????? $2
                           #??? ????????? ??????? - $3
    my ($table_name, $id, $id_field_name) = @_;
    
    if(! $id_field_name) {
        $id_field_name = db_get_id_field_name($table_name);
    }
    return db_query("DELETE FROM $table_name WHERE $id_field_name = $id;");
}

sub db_abort($){
	my $sth = shift;
	my $errstr  = "Error in query:\n '".$sth->{Statement}."' \nError ".$sth->err.": " . $sth->errstr . "\n";
	db_disconnect();
	abort($errstr);
}

return 1;
END {}
