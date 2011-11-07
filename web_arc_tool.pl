#!/usr/bin/perl
use strict;
use Date::Format;
use FindBin;
use File::Basename;
use File::Temp qw/ tempfile tempdir /;
use Archive::Tar;
use Text::Iconv;
use JSON;
use Cwd qw(abs_path realpath);
use CGI::Session;
use CGI qw/:standard -debug/;
use DBI;

use lib "$FindBin::Bin/";
use Misc;
use ErrorHandler;
use DbDbi;
use ArcSvrk;
use Auth;
use Loggy;
use TaskMan;

my $q = CGI->new;
my $json = JSON->new;
$json = $json->pretty( 1 );
my $inner_encoding = 'utf-8';
my $outer_encoding = 'koi8-r';
my $converter = Text::Iconv->new($inner_encoding, $outer_encoding);
my $document_root = $ENV{DOCUMENT_ROOT};
# my $user = $q->remote_user();
my $session = new CGI::Session("driver:File", $q, {Directory=>'/tmp'});
my $user = $session->param('user');

my $sun_server = 'sun-25.vver';

set_error_handler(\&myabort);
set_warn_handler( \&mywarn );
db_connect( dbname => 'kaponir_dev2', host => 'storage.vver', user => 'kaponir', password => 'kaponir123', dbtype => DB_PGSQL);
#ArcInit( taskman_dir => $document_root.'/out/');
ArcInit( suppress_not_found_index_warnings => 1, sort_indexes => 1);

my $srv_home = ArcVar("srv_home");
my $warn = '';

#db_init($FindBin::Bin.'/db/arc.db');


set_log_file("$srv_home/out/log.txt");
set_log_level(LOG_NOTHING);

sub get_tvs_vars($){
	my ($layer_id) = @_;
	my @vars;
	my ($layer_dir, $n_campaign) = db_select_row("select dir, n_campaign from e_arc_layer where n_arc_layer = $layer_id") or myabort("Слой не найден") ;
	my $tvs_dir = ArcVar("arc_home").'/'.$layer_dir.'/../pos/';
	
	my @tvs_list = glob($tvs_dir.'*.pos');
	
	my $var_name;
	
	for( @tvs_list ){
		$var_name = basename($_);
		$var_name =~ s/\.pos//;
		push @vars, $var_name;
	}
	return @vars;
}

sub files_line_count($;$){
	my $kks_dir = shift;
	my $remote_server = shift;
	
	my %counts = ();
	
	my $res;
	if( $remote_server ){
		$kks_dir = str_cut($kks_dir, ArcVar('arc_home'));
		$res = remote_run_res( $remote_server, "cd \$ARCHOME/$kks_dir && wc -l `ls -1`" );
	}else{
		my $wd = pwd;
		chdir( $kks_dir );
		
		$res = system_run_res("wc -l `ls -1`");
		chdir( $wd );	
	}
	
	
	
	for my $line (split /\n/, $res){
		$line = trim $line;
		my ($count, $kks) = split m/\s+/, $line;
		$kks =~ s/\.kks$//;
		$counts{$kks} = $count;
	}
	
	return %counts;
}

sub get_servers(){
	my $servers = db_select_all_hash("select n_computer, name from e_computer");
	return $servers;
}

#sub get_files($){
#	my ($layer_id) = @_;
#	my @files = db_select_all_one("select file
#}

sub get_variables($){
	my ($layer_id) = @_;
	my @vars;
	#my $res = db_query("select dir, n_campaign from e_arc_layer where n_arc_layer = $layer_id");
	#myabort("Слой не найден") unless($res);
	#my ($idb_dir, $n_campaign) = split_line( $res );
	my ($idb_dir, $n_campaign, $remote_server) = db_select_row("select dir, n_campaign, descr from e_arc_layer where n_arc_layer = $layer_id")
		or myabort("Слой не найден");
	
	my $kks_dir = ArcVar("arc_home").'/'.$idb_dir.'/../kks/';
	
	my $seg_id = db_get_value_by_id('e_arc_layer', 'n_arc_seg', $layer_id );
	
	my %counts = files_line_count( $kks_dir, $remote_server);
	
#	$res = db_query("select b.n_block from e_block b, e_campaign c where c.n_campaign = $n_campaign #
#				and c.n_block = b.n_block");
#	myabort("Блок не найден") unless($res);
	
	#my $block_id = db_select_one("select b.n_block from e_block b, e_campaign c where c.n_campaign = $n_campaign 
	#	and c.n_block = b.n_block") or myabort("Блок не найден");
	
# 	my @kks_list = glob($kks_dir.'*.kks');
# 	
# 	my $var_name;
# 	for( @kks_list ){
# 		$var_name = basename($_);
# 		$var_name =~ s/\.kks$//;
# 		push @vars, $var_name;
# 	}
#	$res = db_query("select v.n_var, v.var_name, v.cols, v.rows, v.layers 
#					from e_var v, r_var_for_block r where r.n_block = $block_id and v.n_var = r.n_var order by name desc");
# 	abort("select v.n_var, v.var_name, v.cols, v.rows, v.layers 
# 					from e_var v, r_var_for_block r where r.n_block = $block_id and v.n_var = r.n_var order by name desc");
	#my $all_block_id = db_select_one("select n_block from e_block where name='all'");
	
	my $sth = db_exec( "select v.n_var, v.var_name, v.cols, v.rows, v.layers 
					from e_var v, r_var_for_seg r where r.n_seg = $seg_id and v.n_var = r.n_var order by name desc");
	while( my ($n_var, $var_name, $cols, $rows, $layers) = $sth->fetchrow_array ){
		my $kks_file =  $kks_dir.'/'.$var_name.'.kks';
		#next unless( -f $kks_file);
		
		
		my $len = $cols * $rows;
		my $filelen = $counts{$var_name};
		#($filelen) = split(/\s/, $filelen);

		
		push @vars, {name => $var_name,
					 cols => $cols,
					 rows => $rows,
					 layers => $layers,
					 badArray => $filelen == $len ? '' : '1',
					 n_var => $n_var};
	}
  	return @vars;
}

sub get_tvs_file($$){
	my ($layer_id, $pos_name) = @_;
	my ($layer_dir, $n_campaign) = db_select_row("select dir, n_campaign from e_arc_layer where n_arc_layer = $layer_id") or myabort("Слой не найден");
	my $pos_dir = ArcVar("arc_home").'/'.$layer_dir.'/../pos/';
	my $pos_file = "$pos_dir/$pos_name.pos";
	myabort("Переменная '$pos_name' не найдена") unless(-f $pos_file );
	
	
	my ($block, $camp) = db_select_row("select b.name, c.name from e_block b, e_campaign c where b.n_block = c.n_block and c.n_campaign = $n_campaign") or myabort("Кампания не найдена");
	my $out;
	$out.="# Block: $block\n";
	$out.="# Camp : $camp\n";
	$out.="# Param: $pos_name\n\n# NTvs\$pos_name\n";
  	open my $fd, $pos_file or myabort("Cannot open '$pos_file' for reading");
   	
	while(my $l = <$fd>){
		$out.=$l;
	}
	close($fd);
	return $out;
}

sub get_passport($$){
	my ($pst, $istxt) = @_;
	my $pst_dir = '../passport/';
	
	my $file = 	"$pst_dir/$pst.pst";
	my $txt = "$pst_dir/$pst.txt";
	
	myabort('Passport not found') unless( -f $file);
	
	
	my $fd;
	
	if( $istxt ){
		$fd = open_read($txt);
		print $q->header( -status=>'200', -type=>'text/plain'  );
	}else{
		$fd = open_read($file);
		print $q->header( -status=>'200', -type=>'application/ostet-stream', -attachment => basename($file), -filename => basename($file) );
	}
	
	while(<$fd>){
		print $_;
	}
	close $fd;
	exit;
	
}

sub get_passports(){
	my $pst_dir = '../passport/';
	my @res;
	for my $file (glob "$pst_dir/*.pst"){
		my $name = basename( $file );
		$name =~ s/\.pst//;
		push @res, {
			name => $name
		};
	}
	return \@res;
}

sub get_blocks(){
	my @blocks = ();
	
#	db_process('select n_block, name from e_block', sub{
	my $sth_block = db_exec( 'select n_block, name from e_block' );
	while(my ($n_block, $block_name) = $sth_block->fetchrow_array){
		my @camps = ();
		my $sth_camp = db_exec('select n_campaign, name from e_campaign where n_block = '.$n_block.' order by b_date');
		while( my ($n_campaign, $camp_name) = $sth_camp->fetchrow_array ){
			my @layers = ();
			my $sth_layer = db_exec('select n_arc_layer, name, begin_time, end_time, online, descr, dir from e_arc_layer where n_campaign = '.$n_campaign.' order by begin_time');
			while( my ($n_layer, $layer_name, $date_start, $date_end, $online, $descr, $dir) = $sth_layer->fetchrow_array ){
				my $layer = {id => $n_layer,
						   name => $layer_name,
						   date_start => $date_start,
						   date_end => $date_end,
						   descr => $descr,
							dir => $dir};
				#if( $descr ){
				#	$online &&= check_is_layer_online( $layer );
				#}
				push (@layers, $layer ) if($online) ;
			}
			next if( $#layers == -1 );
			@layers = sort { $a->{name} cmp $b->{name} }  @layers;
			push @camps, {id => $n_campaign,
					  name => $camp_name,
					  layers => \@layers};
		}
		next if($#camps==-1);
		@camps = sort { $a->{name} cmp $b->{name} } @camps;
		push @blocks, {id => $n_block,
				  name => $block_name,
				  camps => \@camps};
	}
  	
	@blocks = sort { $a->{name} cmp $b->{name} } @blocks; 
	return @blocks;
}


sub parse_vars($$){
	my ($params_ref, $replace_index_ref ) = @_;
	my $i = 0;
	my @res;
	for my $param_str (@$params_ref){
		my $param = {};
		abort("Wrong param notation: '$param_str'") unless($param_str =~m/([\w\|]+)(?:\(([^\;]+)(?:\;([^\;]+)|)\)|)/);

		my ($param_name, $index_str1, $index_str2) = ($1, $2, $3);
		$param = {
			name => $param_name,
			index1 => $index_str1,
			index2 => $index_str2
		};
		
		my $replace_index = $replace_index_ref->[$i++];
		if( $replace_index != -1 ){
			$param->{pos_var} = $replace_index;
		}
		push @res, $param;
	}
	return \@res;
}

sub get_data($$$$$$$$$$$$$){
	my ( $camp_name, $date_start, $date_end, $layer_id, $step, $params_ref,$kks_templates, $time_locale, $compress, $format, $time_progress, $arc_location,  $replace_index_ref)  = @_;
	
	my $vars = parse_vars( $params_ref, $replace_index_ref );
	
	if($kks_templates eq '' && !@$vars){
		myabort("Укажите переменную или ККС");
	}
	
# 	my @params;
	my @kks_templates;
# 	@params = prepare_params($param, $index) if($param ne '');
	#@params = @$params_ref;
	@kks_templates = split m/[\n\,\s]+/, $kks_templates;

	my ($out_file, $task_name, $task_params_ref);
	my $params = {begin_time => $date_start,
				end_time => $date_end,
											   camp => $camp_name,
											   layer_id => $layer_id,
											   step => $step,
											   vars => $vars,
 											   zip => $compress,
											   time_correction => $time_locale,
							     	   		   output_format => $format,
											   time_progress => $time_progress,
											   close_stdout => 1,
											   user => $user};
	if( $arc_location){
		$params->{remote_server} = $arc_location;#db_get_value_by_id('e_computer', 'hostname', $n_computer );
		ArcVar('remote_server', $arc_location);
	}
	
	if( @kks_templates ){
		my @params = vars_from_kks_list( @kks_templates );
		$params->{vars} = \@params;
		$params->{merge_params} = 1;
		$params->{kks_mode} = 1;
	}
	
	 
	my ($task) = extract_data( $params, 0 );								   
	create_link( $task->{dir}, $document_root.'/out/'.$task->{name} );
	
	my $task_path = "$document_root/out/$task_name/task.ini";
	#my $log_file = "$document_root/out/exec_log.txt";

	#write_to_exec_log($user, $task_path, $log_file);

	return $task;
# 	myabort("Ошибка экспорта данных") unless( $out_file );;
# 	if( $compress ){
# 		my $tempdir = tempdir ( "ImGHXXXXX", TMPDIR => 1, UNLINK => 1 );
# 		`mv $out_file $tempdir/output.txt`;
# 		chdir $tempdir;
# 		`gzip output.txt`;
# 		return "output.txt.gz";
# 	}
# 	#my $out_str = `cat $out_file`;
# 	return `cat $out_file`;
}

sub get_results($$){
	my ($task_name, $zipped) = @_;
	my $out_file = $document_root."/out/$task_name/output.txt";
 	my $pid_file = $document_root."/out/$task_name/running";
	
	if(! -f $pid_file ){
		myabort("Ошибка выгрузки") unless( -f $out_file );

		#предваритено форматируем
		my $input_idb = "$document_root/out/$task_name/idb/$task_name.idb";
		
		my $task = ReadTask( "$document_root/out/$task_name/" );
		my $stderr_file = $task->{params}->{stderr};
		my $s =  (stat($stderr_file))[7];;
		myabort( cat $stderr_file ) if( -s $stderr_file );
#		my $input_idb = $task->{}
#		my $task_params_ref = read_json( $ini_file );
		#system_run("cat $out_file | head -n -1 >> $out_file.tmp");
		#rename( "$out_file.tmp", $out_file );
		my @out_files_formated = format_output($out_file, $task->{params});
		
		
		if( $zipped ){
# 			my $zipped_file = "$document_root/out/$task_name/output.tar.gz";
			my $zipped_file = "$document_root/out/$task_name/output.tgz";
			zip_files( $zipped_file, @out_files_formated);
			$zipped_file =~ s/$document_root//;
			return { complete => 'yes',
				 out_files => [ $zipped_file ]};
		}else{
# 			my @out_files;
			for my $out_file (@out_files_formated){
				$out_file =~ s/$document_root//;
# 				push @out_files, $out_file;
			}
			return { complete => 'yes',
				 out_files => \@out_files_formated};
		}

		

# 		`mv $out_file_formated $out_file`;
# 		zip_file($out_file) if($zipped);
# 		return 'complete';
	}else{
		return { complete => '' };
	}
	
# 	return -f $out_file ? 'complete' : 'not complete';
}

sub login($$){
	my ($login, $password) = @_;
	if(user_auth($login, $password)){
		$user = $login;
		$session->param('user', $user);	
		return 'success';
	}
	return 'fail';
}

sub if_logged(){
	return {user => $user, success=> $user?'yes':'no'};
}

sub logout(){
	$session->delete();
	$session->flush();
	return 'success';
}

sub encode($){
	return $converter->convert( shift );
}

sub get_ses_cookie(){
	my $cookie = $q->cookie(CGISESSID => $session->id);
    return $cookie;
}

sub myabort($){
	my ($str) = @_;
	$str=~s/\'/\\\'/g;
	$str = encode( $str );
	my $json = JSON->new;
	print $q->header( -status=>'200', -type=>'text/html', charset=> $outer_encoding, -cookie=> get_ses_cookie());
	print $json->encode({status => 'error', message => $str});
    exit;
}

sub mywarn($){
	$warn .= shift;	
}
sub ajax_res($;$){
	my ($data_ref, $dont_exit) = @_;
	$dont_exit = 0 unless( defined( $dont_exit ) );
	print $q->header( -status=>'200', -type=>'text/html',  -cookie=> get_ses_cookie() );
	print $json->encode({status=> 'ok', data => $data_ref, warning => $warn});
	exit unless( $dont_exit );
}

my $action = $q->param('action');

if($action eq 'login' ){
	my $login = $q->param('login');
	my $password = $q->param('password');
	myabort('Not enought params') unless( $login && $password);
	
	my $res = login($login, $password);

	ajax_res($res);

}

if($action eq 'if_logged'){
	my $res = if_logged();
	ajax_res($res);
}

if($action eq 'logout'){
	my $res = logout();
	ajax_res($res);
}

#myabort("Error in auth") unless($user);





if($action eq 'view'){
	my @res = get_blocks();
	print ajax_res(\@res);
}

if($action eq 'get_variables'){
	my $layer_id = $q->param('layer_id');
	
	myabort('Not enought params') unless( $layer_id );
	
	my @res = get_variables($layer_id);
	ajax_res(\@res);
}
if($action eq 'get_tvs_vars'){
	my $layer_id = $q->param('layer_id');
	
	myabort('Not enought params') unless( $layer_id );
	
	my @res = get_tvs_vars($layer_id);
	ajax_res(\@res);
}
if($action eq 'get_tvs_file'){
	my $layer_id = $q->param('layer_id');
	my $tvs_name = $q->param('tvs_name');
	
	myabort('Not enought params') unless( $layer_id && $tvs_name);
	
	my $res = get_tvs_file($layer_id, $tvs_name);
	ajax_res($res);
}

if($action eq 'get_results'){
	
	my $task_name = $q->param('task_name');
	my $zipped = $q->param('zipped');
	my $res  = get_results($task_name, $zipped);
	ajax_res( $res );
}

if($action eq 'get_data'){
	my $camp = $q->param('camp');
	my $layer_id = $q->param('layer_id');
	my $date_end = $q->param('date_end');
	my $date_start = $q->param('date_start');
	my @params = $q->param('params[]');
# 	my $index = $q->param('index');
	my $step = $q->param('step');
	my $time_locale = $q->param('time_locale');
	my $time_progress = $q->param('time_progress');
	my $format = $q->param('format');
	my $zipped = $q->param('zipped');
	my $kks_templates = $q->param('kks_templates');
	#my $n_computer = $q->param('n_computer');
	my $arc_location = $q->param('arc_location');
	my @replace_index = $q->param('replace_index[]');
	
# 	my $ch_res;
# 	my $ch_res = check_cgi_params($camp , $date_end , $date_start , $layer_id , @params, $step ,$zipped, $time_locale, $time_progress,$format,$kks_templates);

#  	mabort("Not enought params $ch_res") unless($ch_res);
# 	myabort( defined($q->param('time_locale')) ? "def":"ndef");
	myabort('Not enought params') unless( $camp && $date_end && $date_start && $layer_id 
			&& $step && defined($zipped)  && defined($time_locale)
			&& defined($time_progress) && defined($format)	&& defined($kks_templates) && defined( $arc_location) );
		
	my $task = get_data($camp, $date_start, $date_end, $layer_id, 
										  $step, \@params, $kks_templates, $time_locale, $zipped, $format, $time_progress, $arc_location, \@replace_index);
	ajax_res( $task->{name}, 1 );
	RunTask( $task );
	exit;
}

if( $action eq 'get_servers'){
	my $servers = get_servers();
	ajax_res( {
		'servers' => $servers 
	});
}

if( $action eq 'get_passports'){
	my $pst = get_passports();
	ajax_res( {
		'passports' => $pst 
	});
}

if( $action eq 'get_passport'){
	my $pst = $q->param('pst');
	my $txt = $q->param('txt');
	get_passport($pst, $txt);
}

myabort("Invalid action");
#print $q->header( -status=>'200', -type=>'text/html' );

