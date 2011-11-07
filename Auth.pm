package Auth;

BEGIN {
	#use File::Basename;
#	use DateTime;
	use File::Basename;
	use Digest::MD5  qw(md5 md5_hex md5_base64);
# 	use Proc::Background;
	#use Cwd 'abs_path';
	#use File::Temp qw/ tempfile tempdir /;
	#use File::Path;
	use Exporter();
	#use Time::HiRes qw(gettimeofday);
	use lib "$FindBin::Bin/";
	use ErrorHandler;
	use DbDbi;
	use Loggy;
	use Misc;
	
    use strict;
	
    @Auth::ISA = qw(Exporter);
    @Auth::EXPORT = qw(&add_user &get_users &user_auth &del_user);
    					
	our $script_path = $FindBin::Bin;
	
}


sub add_user($$){
	my ($user, $password) = @_;
	abort("Пользователь '$user' уже существует") if(db_select_one("select name from e_arc_user where name = '$user'"));
	$password = md5_base64($password);
	my $user_id = db_insert('e_arc_user', (name=> qstr($user), password => qstr($password)));
	return $user_id;
}

sub del_user($){
	my ($user) = @_;
	db_query("delete from e_arc_user where name = '$user'");
}

sub get_users(){
	my $res = db_select_one("select name from e_arc_user");
	return split /\n/, $res;
}

sub user_auth($$){
	my ($user, $password) = @_;
	$password = md5_base64($password);
# 	abort("select name from e_arc_user where name='$name' and password='$password' " );
	return db_select_one("select name from e_arc_user where name='$user' and password='$password' ") ne '';
}



return 1;

END {}

