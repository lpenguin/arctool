package Misc;

BEGIN {
	#use File::Basename;
#	use DateTime;
#	use Date::Format;
	use Time::Local;
	use File::Basename;
	use File::Spec;
	#use Proc::Background;
	use Cwd 'abs_path';
	use JSON;
	#use File::Temp qw/ tempfile tempdir /;
	#use File::Path;
	use Exporter();
	use Time::HiRes;
	use Time::HiRes qw(gettimeofday);
	
	use FindBin;
	use lib "$FindBin::Bin/";
	use ErrorHandler;
	use Loggy;
    use strict;
	
    @Misc::ISA = qw(Exporter);
    @Misc::EXPORT = qw( files_count str2ymd str2dmy cat_to copy_hash arr_cmp_str touch
    					&template &dmy2ymd &ymd2dmy  &qstr &dialog_yn &dmy2time &str2ymd
    					&time2dmy &find_file_i
    					&convert_date_to_nornal &validate_dates	&read_ini &write_ini
    					&system_run &system_run_res &convert_date_to_nornal
    					&ymd2time &time2ymd &validate_dates
    					&array_contains &index_str2array &remove_msecs 
    					&merge_hashes &merge_hashes_def &pwd &zip_files &parse_idb &parse_idb_index
    					&open_read &open_write &open_read_idb &array_get &print_hash &array_find_in_array &array_find
    					&get_idb_fields_fd &check_cgi_params &write_to_exec_log &whoami &cat &dump_hash
    					&dump_hash_ref
    					&remote_copy &remote_copy_back &remote_run &remote_run_res &array_each &ltrim &rs &env_str
    					&iconv &dialog_string &test_time &convert_date_to_nornal_ymd &set_dialog_say_yes &read_last_line
    					&str2time &write_json &read_json &generate_id &symlink_tpl &is_array_filled_nulls
    					&make_time_correction_ymd &make_time_correction &time2str &create_link &append_file 
    					&rse &expand &merge &d &unique &wc &trim &crop_last_line &str_cut &check_is_layer_online);
    					
	our $script_path = $FindBin::Bin;
	our $dialog_say_yes = 0;
}

sub crop_last_line($){
	my ($filename) = @_;
	
	system_run("cat $filename | head -n -1 >> $filename.tmp");
	rename( "$filename.tmp", $filename );			
}

sub wc($){
	my $file = shift;
	return system_run_res('wc -l $file');
	
}
# sub copy($$){
# 	my ($from, $to) = @_;
# 	system_run("cp '$from' '$to'");
# }
sub unique {
    return keys %{{ map { $_ => 1 } @_ }};
}

sub d($){
	return dump_hash_ref(shift @_);
}

sub expand($$){
    my ($hr1, $hr2) = @_;
    for my $key (keys(%{$hr1})){
        $hr1->{$key} = $hr2->{$key} if( exists( $hr2->{$key} ) );
    }
    return $hr1;
}

sub merge($$){
    my ($hr1, $hr2) = @_;
    for my $key (keys(%{$hr1})){
        $hr1->{$key} = $hr2->{$key};
    }
    return $hr1;

}

sub str_cut($$){
	my ($str, $piece) = @_;
	$str =~ s/^$piece//;
	return $str;
}
sub touch($){
	my $fd = open_write(shift);
	close $fd;
}

sub arr_cmp_str($$){
	my ($a1, $a2) = @_;
	for( my $i = 0; ($i <@$a1) && ($i < @$a2); $i++ ){
		return 0 unless( $a1->[$i] eq $a2->[$i]);
	}
	return 1;
}
sub copy_hash($){
	my $href = shift;
	my %hash = %{$href};
	return \%hash;
}
sub files_count($){
	my $dir = shift;
	my @files = glob "$dir/*";
	my $count = @files;
	return $count;
}
sub create_link($$){
	my ($old_file, $new_file) = @_;
	#link $old_file, $new_file or abort("Cannot create link '$new_file' from '$old_file'. $!");
	system_run("ln -fs $old_file $new_file") or abort("Cannot create link '$new_file' from '$old_file'. $!") unless( -e $new_file);
}

sub append_file($$){
	my ($to, $what) = @_;
	cat_to( $what, $to );
}

sub make_time_correction($;$$$$){
	my ($timestr, $hours, $mins, $secs, $use_timelocal) = @_;
	$hours ||= 0;
	$min ||=0;
	$secs ||= 0;
	
	my $time = str2time( $timestr, $use_timelocal ) or abort("Err in dmy time str '$timestr' ");
	my $total = $time + $hours*60*60 + $mins*60 + $secs;

	return  time2dmy($total, $use_timelocal);
}

sub make_time_correction_ymd($;$$$$){
	my ($timestr, $hours, $mins, $secs, $use_timelocal) = @_;
	$hours ||= 0;
	$min ||=0;
	$secs ||= 0;
	
	my $time = str2time( $timestr, $use_timelocal ) or abort("Err in ymd time str '$timestr' ");
	my $total = $time + $hours*60*60 + $mins*60 + $secs;
	
	return time2ymd($total, $use_timelocal);
}

sub is_array_filled_nulls(@){
	for my $v (@_){
		return 0 if($v);
	}
	return 1;
}
sub symlink_tpl($$$){
	my ($dir, $dest, $ext) = @_;
	$dir = abs_path($dir);
	$dest = abs_path($dest);
	
	for my $file ( glob "$dir/*.$ext" ){
		my $link = File::Spec->abs2rel( $file, $dest );
		unlink( $dest.'/'.basename($file) ) if( -f $dest.'/'.basename($file) );
		create_link($link, $dest.'/'.basename($file));
		#symlink File::Spec->abs2rel( $file, $dest ), $dest.'/'.basename($file);
	}
}
sub generate_id(){
	my ($sec, $msec) = gettimeofday();
	return $sec.$msec;
}

sub write_json($$){
	my ($file, $scalar ) = @_;
	unlink $file if( -f $file );
	my $fd = open_write( $file );
	print $fd dump_hash_ref( $scalar );
	#print $fd to_json( $scalar, {allow_unknown => 1, pretty => 1} );
	close ($fd );
}

sub read_json($){
	my $file = shift;
	
	my $fd = open_read( $file );
	local $/;
	return JSON->new->allow_unknown->allow_nonref->pretty->decode( <$fd> );
#	return from_json( <$fd>, {allow_nonref => 1});
}

sub read_last_line($){
	my $file = shift;
	return '' unless -f $file;
	return system_run_res("cat $file | tail -n 1");
#	my $fd = open_read( $file );

#	my $size = -s $file;
#	#sd@####.$$
#	
#	my $found;####
#	my $offset = 512;

#	while( ! defined( $found ) ){
#	  # prevent seek from overshooting the start of the file.
#	  $offset = $size if $offset > $size;
#	  seek( $fd, -$offset, SEEK_END ) or warn "seek failed: $!";

#	  # read line(s) from filehandle. Ideally, offset was chosen
#	  # such that @lines contains part of the penultimate line
#	  # and the last line.
#	  my @lines = <$fd>;

#	  
#	  if( @lines > 1 ){
#		# more than one lines means we can be sure we do have the last one.
#		$found = $lines[ -1 ];
#	  } elsif( $offset >= $size ) {
#		# if we have read the whole file and it's just one line, then that is the last line.
#		$found = $lines[ 0 ];
#	  } else {
#		# try again with a bigger offset
#		$offset += 256;
#	  }
#	}
#	
#	close $fd;
#	return $found;
}

sub set_dialog_say_yes($){
	$dialog_say_yes = shift;
}

sub tintterval($$){
	my ($t1, $t2) = @_;
	return $t1 - $t2;
}
sub test_time(&&){
	my ($sub1, $sub2) = @_;
	my $t0 = Time::HiRes::time;
	$sub1->();
	my $t1 = Time::HiRes::time;
	my $int1 = tintterval $t1, $t0;
	$sub2->();
	my $t2 = Time::HiRes::time;
	my $int2 = tintterval $t2, $t1;
	return ($int1, $int2);
  
}
sub iconv($$$){
	my ($str, $from, $to) = @_;
	return system_run_res("echo '$str' | iconv -f=$from -t=$to");
}

sub array_each(&@){
	my ($sub, @array) = @_;
	for $arr (@array){
		$_ = $arr;
		$sub->($arr);	
	}
	
}

sub ltrim($;$){
	my ($str, $erase_str) = @_;
	$erase_str = ' ' unless( defined( $erase_str ));
	my $res = $str;
	$res =~ s/^$erase_str//;
	return $res;
}

sub rs($){#remove double slashes
	my ($str) = @_;
	
	$str =~ s/\/\//\//g;
	return $str;
}

sub rse($){#remove slash at end, and remove double slashes
	my ($str) = @_;
	
	$str =~ s/\/+$//;
	return rs( $str );
}

sub env_str($$$){
	my ($str, $erase, $env) = @_;
	return rs( $env.'/'.ltrim( $str, $erase) );
	
}

sub remote_copy($$$){
	my ($host, $from, $to) = @_;
	my $dir_flag = -d $from ? '-r' : '';
	my $user_string = '';
#	if($ENV{USER}){
#		$user_string = $ENV{USER}.'@';
#	}
	system_run("scp $dir_flag -p $from $user_string$host:$to >> /dev/null");
}

sub remote_copy_back($$$){
	my ($host, $from, $to) = @_;
	my $dir_flag = -d $from ? '-r' : '';
	my $user_string = '';
#	if($ENV{USER}){
#		$user_string = $ENV{USER}.'@';
#	}
	system_run("scp $dir_flag -p $user_string$host:$from $to >> /dev/null");
}

sub remote_run($$){
	my ($host, $command) = @_;
	my $user_string = '';
#	if($ENV{USER}){
#		$user_string = $ENV{USER}.'@';
#	}
	return system_run("ssh $user_string$host '$command'");
}

sub remote_run_res($$){
	my ($host, $command) = @_;
	my $user_string = '';
#	if($ENV{USER}){
#		$user_string = $ENV{USER}.'@';
#	}
	return system_run_res("ssh $user_string$host '$command'");
}

sub print_hash(%){
	my (%hash) = @_;
	info dump_hash_ref(\%hash);
}

sub dump_hash(%){
	my (%hash) = @_;
	return dump_hash_ref(\%hash);
}

sub dump_hash_ref($){
	return JSON->new->allow_unknown->allow_nonref->pretty->encode( shift );
}

sub cat_to($$){
	my ($from, $to) = @_;
	my $from_fd = open_read( $from);
	my $to_fd = open_write( $to );
	while(<$from_fd>){
		print $to_fd $_;
	}
	close $from_fd;
	close $to_fd;
	
}
sub cat($){
	my ($file_name) = @_;
	
	abort("File '$file_name' doesn't exists") unless( -f $file_name );
	my $fd = open_read($file_name);
	my $res;
	while(<$fd>){
			$res .= $_;	
	}
	close($fd);
	return $res;
# 	return system_run_res("cat '$file_name' ");
}

sub whoami(){
	my $res = system_run_res("whoami");
	chomp( $res );
	return $res;
}

sub write_to_exec_log($$$){
	my ($user, $description_file, $log) = @_;

	my $time =  time2str("%Y.%m.%d %H:%M:%S", time);
	system_run("echo '$time' >> $log");
	system_run("echo 'user = $user' >> $log");
	system_run("echo 'ip = $ENV{REMOTE_ADDR}' >> $log") if( exists($ENV{REMOTE_ADDR}) );
	system_run("cat '$description_file' >> $log");
	system_run("echo '' >> $log");
	system_run("echo '----------------------------------' >> $log");
	system_run("echo '' >> $log");
# 	db_insert( 'e_exec_log', ( name => qstr($user), descr => qstr($description)));
}

sub check_cgi_params(@){
	my (@params) = @_;
	
	abort( join "\n", @_);
	my $c = 1;
	for my $param (@params){
# 		return $c unless( defined($param) );
		$c++;
	}
	return 0;
}

sub array_get($@){
	my ($arr_ref, @index) = @_;
	my @a = @$arr_ref;
	my @res;
	for (@index){
		next unless(exists($a[$_]));
		push @res, $a[$_];
	}
	return @res;
}

sub open_read($){
	my ($file_name) = @_;
	open my $fd, $file_name or abort("Cannot open for reading '$file_name'");
	return $fd;
}

sub open_read_idb($;@){
	my ($idb_file, @fields) = @_;
	
	my $fd = open_read($idb_file);
	
	my @res_fields = get_idb_fields_fd($fd);

	if($#fields != -1){
# 		blah join ' ', @res_fields;
# 		blah join ' ', @fields;
		
		@res_fields = array_find_in_array(\@res_fields, \@fields);
# 		blah join ' ', @res_fields;
	}
	
	return ($fd, @res_fields);
}

sub array_find($@){
	my ($str, @array) = @_;
	my $c=0;
	for my $s (@array){
		return $c if($s eq $str);
		$c++;
	}
	return undef;
}

sub array_find_in_array($$){
	my ($arr_ref, $strs_ref) = @_;
	my @res;
	my $c = 0;
	for my $str (@$arr_ref){
		push @res, $c if(defined(array_find($str, @$strs_ref)));
		$c++;
	}
	return @res;
}

sub open_write($;$){
	my ($file_name, $erase_first) = @_;
	my $fd;
	if( $erase_first ){
		open $fd, '>', $file_name or abort("Cannot open for writing '$file_name'");
	}else{
		open $fd, '>>', $file_name or abort("Cannot open for writing '$file_name'");
	}
	
	return $fd;
}

sub get_idb_fields($){
	my ($idb_name) = @_;
	my $fd = open_read($idb_name);
	my @fields = get_idb_fields_fd($fd);
	close($fd);
	return @fields;
}

sub get_idb_fields_fd($){
	my ($fd) = @_;
	
	<$fd>;<$fd>;
	my $line = <$fd>;
	my @fields = split m/\s*\|/, $line;

	return @fields;
}


sub get_idb_fields_hash($){
	my ($idb_name) = @_;
	
	my $fd = open_read($idb_name);
	
	<$fd>;<$fd>;
	my $line = <$fd>;
	my @fields = split m/\s+\|/, $line;
	my %hash;
	my $c = 0;
	for (@fields){
		$hash{$_} = $c++;
	}
	close($fd);
	return %hash;
}

sub parse_idb_index($@){
	my ($idb_name, @fields) = @_;
	
	my ($fd, @fields_num) = open_read_idb($idb_name, @fields);
# 	blah join ' ', @fields_num;
	my @values;
	my %res;
	while(my $line = <$fd>){
		my %res_hash;
		@values = split m/\s*\|/, $line;
		my $key = $values[0];
		for(@fields_num){
			$res_hash{$fields[$_]} = $values[$_];
# 			next unless(exists($idb_fields{$_}) && exists($values[$idb_fields{$_}]));
# 			$res_hash{$_} = $values[$idb_fields{$_}];
		}
		$res{$key} = \%res_hash;
	}
	
	close($fd);
	return %res;
}


sub parse_idb($@){
	my ($idb_name, @fields) = @_;

	my ($fd, @idb_fields_num) = open_read_idb($idb_name, @fields);
	
	my @values;
	my @res;
	while(my $line = <$fd>){
		my %res_hash;
		@values = split m/\s*\|/, $line;
		my $c = 0;
		for my $field_num (@idb_fields_num){
			my $field_name = $fields[$c++];
			$res_hash{$field_name} = $values[$field_num];
# 			if(m/\d+/ && exists($values[$_])){
# 				$res_hash{$_} = $values[$_];
# 				next;
# 			}
			
# 			next unless(exists($idb_fields{$_}) && exists($values[$idb_fields{$_}]));
# 			$res_hash{$_} = $values[$idb_fields{$_}];
		}
		push @res, \%res_hash;
	}
	
	close($fd);
	return @res;
}

sub merge_hashes_def($$){ #hash1 <= hash2
	my ($hr1, $hr2) = @_;
	
	for my $key ( keys(%{$hr1})){
		$hr1->{$key} = $hr2->{$key} if( exists($hr2->{$key}));
	}
	return %{$hr1};
}

sub merge_hashes($$){ #hash1 <= hash2 (keys from all );
	my ($hr1, $hr2) = @_;
	my (%hash1, %hash2) = (%$hr1, %$hr2);
	my %res;
	for my $key ( keys(%hash1)){
		$res{$key} = $hash1{$key} 
	}
	
	for my $key ( keys(%hash2)){
		$res{$key} = $hash2{$key} 
	}
	
	return \%res;
}

sub zip_files($@){
	my ($out_name, @files) = @_;
# 	for my $file (@files){
	my $wd = pwd();	
	my $dir_name;
	my $str;
	for my $file (@files){
		my $name = basename($file);
		$dir_name = dirname( $file );
		$str .= $name.' ';
	}
	
	chdir($dir_name);
	
	system_run("tar czvf $out_name $str > /dev/null");
	chdir $wd;
# 	}
	return $out_name;
	# 	cd `dirname $out_file`;
#     gzip `basename $out_file`;
}
# sub write_to_ini($$$){
# 	my ($ini_path, $key, $value) = @_;
# 	open( my $fd, '>>', $ini_path) or abort "Cannot open file '$ini_path' for writing";
# 	print $fd "$key = $value\n";
# 	close($fd);
# }

sub pwd(){
	my $wd = `pwd`;
	chomp($wd);
	return $wd;
}

sub write_ini($%){
	my ($ini_file, %params) = @_;
	my $fd = open_write($ini_file);
	for ( keys(%params) ){
		print $fd "$_ = $params{$_}\n";
		
	}
	close $fd;
}

sub read_ini($){
	my ($ini_path) = @_;
	open( my $fd, $ini_path) or abort "Cannot open file '$ini_path' for reading";

	my %hash;

    while(<$fd>) {
    	last if(m/^files/);
	    chomp;
        next unless( $_ );
        
	    $hash{$1} = $2 if( m/(\S+)\s*=\s*(.*)/ );
    }
	
	close $ini_path;
	
	return %hash;
}

sub trim($){
	my $string = shift;
	$string =~ s/^\s+//;
	$string =~ s/\s+$//;
	return $string;
}

sub template($$%){
	my ($tpl_file, $out_file, %vars) = @_;
	my $tpl = `cat $tpl_file`;
	abort("Cannot read tpl file '$tpl_file'. $!") if($?);
	for my $name (keys(%vars)){
		my $value = $vars{$name};

        $value =~ s/\/\//\//;
		$tpl =~ s/\<$name\>/$value/gi;

	}

	#print $out_file."\n";
	open (my $file, '>'.$out_file) or abort("Cannot write out file '$out_file'. $!");
	print $file $tpl;
	close ($file); 
	
	return $tpl;
}

sub is_dmy($){
	return 0 unless (shift =~ m/(\d\d)\.(\d\d)\.(\d\d\d\d)\s(\d\d):(\d\d):(\d\d)/ );
	return 1;
}

sub is_ymd($){
	return 0 unless (shift =~ m/(\d\d)\D(\d\d)\D(\d\d\d\d)\s(\d\d):(\d\d):(\d\d)/ );
	return 1;
}

sub str2dmy($){
	my $str = shift;
	return $str if( is_dmy($str ) );
	return ymd2dmy( $str );
}

sub str2ymd($){
	my $str = shift;
	return $str if( is_ymd($str) ) ;
	return dmy2ymd( $str );
}


sub dmy2ymd($){
	#08.06.2006 18:17:18 2010-03-24 10:28:26
	return "" unless(shift =~ m/(\d\d)\D(\d\d)\D(\d\d\d\d)\s(\d\d):(\d\d):(\d\d)/ );
	return "$3.$2.$1 $4:$5:$6";
}

sub dmy2mdy($){
	#30.12.2006 18:17:18 -> 12.30.2006 18:17:18
	return "" unless(shift =~ m/(\d\d)\.(\d\d)\.(\d\d\d\d)\s(\d\d):(\d\d):(\d\d)/ );
	return "$2.$1.$3 $4:$5:$6";
}

sub ymd2dmy($){
	#2010-03-24 10:28:26 08.06.2006 18:17:18 
	return "" unless(shift =~ m/(\d\d\d\d).(\d\d).(\d\d)\s(\d\d):(\d\d):(\d\d)/ );
	return "$3.$2.$1 $4:$5:$6";
}

sub dmy2time($;$){
	return 0 unless(shift =~ m/(\d\d)\D(\d\d)\D(\d\d\d\d)\D(\d\d)\D(\d\d)\D(\d\d)/ );
	my $use_timelocal = shift;
	$use_timelocal = 0 unless( defined( $use_timelocal ) );
	return timelocal($6,$5,$4,$1,$2-1,$3) if( $use_timelocal);
	return timegm($6,$5,$4,$1,$2-1,$3);
}

sub ymd2time($;$){
	return 0 unless(shift =~ m/(\d\d\d\d)\D(\d\d)\D(\d\d)\D(\d\d)\D(\d\d)\D(\d\d)/ );
	my $use_timelocal = shift;
	$use_timelocal = 0 unless( defined( $use_timelocal ) );
	return timelocal($6,$5,$4,$3,$2-1,$1) if( $use_timelocal);
	return timegm($6,$5,$4,$3,$2-1,$1)
}

sub str2time($;$){
	my $str = shift;
	my $use_timelocal = shift;
	my $res = ymd2time($str, $use_timelocal );
	return $res if( $res );
	return dmy2time( $str, $use_timelocal );
}

sub time2ymd($;$){
	my ($time, $use_localtime) = @_;
	return time2str("%Y-%m-%d %H:%M:%S", $time, $use_localtime);
#	my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(shift);
#	my $dl = shift;
#	$dl=' ' unless(defined($dl)); 	
#    $year += 1900;
#    $mon++;
#    return sprintf("%04d.%02d.%02d$dl%02d:%02d:%02d", $year,$mon,$mday,$hour,$min,$sec);
}

sub time2dmy($;$){
	my ($time, $use_localtime) = @_;
	return time2str("%d-%m-%Y %H:%M:%S", $time, $use_localtime);
#	my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) =
#                                                localtime(shift);
#	my $dl = shift;
#	$dl=' ' unless(defined($dl)); 	
#	
#    $year += 1900;
#    $mon++;
#	return sprintf("%02d.%02d.%04d$dl%02d:%02d:%02d", $mday,$mon,$year,$hour,$min,$sec);
}

sub time2str($$;$){
	my ($format, $time, $use_localtime) = @_;
	$use_localtime = 0 unless( defined( $use_localtime ) );
	
	my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst);
	my @res;
	if( $use_localtime ){
		@res = localtime($time) 
	}else{
		@res = gmtime($time);
	}
	my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = @res;
	$year += 1900;
    $mon++;
    $year = sprintf('%04d', $year);
    ($mon, $mday, $hour, $min, $sec)  = map {sprintf('%02d', $_)} ($mon, $mday, $hour, $min, $sec);
	$format =~ s/\%Y/$year/;
	$format =~ s/\%m/$mon/;
	$format =~ s/\%d/$mday/;
	$format =~ s/\%H/$hour/;
	$format =~ s/\%M/$min/;
	$format =~ s/\%S/$sec/;
	return $format;
}

#sub dmy2datetime($){
#	return 0 unless(shift =~ m/(\d\d)\D(\d\d)\D(\d\d\d\d)\D(\d\d)\D(\d\d)\D(\d\d)/ );

#	return DateTime->new( year => $3,
#						  month => $2,
#						  day => $1,
#						  hour => $4,
#						  minute => $5,
#						  second => $6);
#}
#sub ymd2datetime($){
#	return 0 unless(shift =~ m/(\d\d\d\d)\D(\d\d)\D(\d\d)\D(\d\d)\D(\d\d)\D(\d\d)/ );

#	return DateTime->new( year => $1,
#						  month => $2,
#						  day => $3,
#						  hour => $4,
#						  minute => $5,
#						  second => $6);
#}

#sub datetime2ymd($){
#	my ($dt) = @_;
#	return $dt->ymd('.').' '.$dt->hms(':');
#}

#sub datetime2dmy($){
#	my ($dt) = @_;
#	return $dt->dmy('.').' '.$dt->hms(':');
#}

sub find_file_i($@){
	my ($dir, @files) = @_;
	my $res_file;
	opendir my $dd, $dir or abort("error in open dir $dir");
	while( $file = readdir($dd) ){
		next unless( -f "$dir/$file" );
		for my $f (@files ){
#			info "$file $f";
			$res_file = $file if( lc $file eq lc $f );
		}
	}
	closedir($dd);
	return $res_file;
}

sub validate_dates($$){
	my ($begin_time, $end_time) = @_;
	
	abort("Некорректно введено время! (t1,t2='')") unless( $begin_time && $end_time );
	
	#abort("Некорректно введено время! (t1=t2)") if($begin_time eq $end_time);
	
	my $bt = dmy2time($begin_time);
	my $et = dmy2time($end_time);	
	abort("Некорректно введено время! (t1>t2)") if($bt > $et);

}

sub index_str2array($){
	my ($index_str) =@_;
	my @out_index;
	my @sub_index = split m/,/, $index_str;
	for $sub_index (@sub_index ){
	
		if( $sub_index =~ m/^(\d+)\:(\d+)$/ ){
			abort("Error in index notation ($1 > $2), '$index_str' -> '$sub_index'") if($1 > $2);
			push @out_index, ( $1 .. $2 );
			next;
		}
		if( $sub_index =~ m/^(\d+)$/ ){
			push @out_index,  $1 ;
			next;
		}
		
		abort("Error in index notation '$index_str' -> '$sub_index'");
	}

	return @out_index;	
}

sub array_contains($@){
	my ($var, @array) = @_;
	#info "var $var, values: ".join ' ', @array;
	return grep {$_ eq $var} @array;
}

sub qstr($){
	return "\'".$_[0]."\'";
}

sub remove_msecs($){
	my $date = shift;
	$date =~ s/_/ /;
	$date =~ s/\.\d*$//;
	return $date;
}

sub dialog_yn($){
	my $message = $_[0].'[y/n]: ';	
	
	print $message;	
	if( $dialog_say_yes ){
		print "y\n";
		return 1;
	}else{
		while( <STDIN> ){
	 		chomp;
			last if($_ eq 'y' or $_ eq 'n');
			print $_;	
		}
		return $_ eq 'y';
	}
}

sub dialog_string($){
	my $message = shift;
	print $message.': ';
	my $str = <STDIN>;
	chomp $str;
	return $str;
}
sub system_run($;$){
    my ($command, $detached) = @_;
    #print $command."\n";
    $detached ||= 0;
   	if($detached){
       # system 'nohup '.$command.' ';
	   Proc::Background->new($command);
    }else{
	system $command;
	abort("Error running '$command'.") if($?);
    }
    return 1;
}

sub system_run_res($){
    my ($command) = @_;
    #print $command."\n";
    my $res = `$command`;
    abort("Error running '$command'.") if($?);
	chomp($res);
    return $res;
}

sub str2ymd($){
	my $timestr = shift;
	my $time = str2time($timestr);
	return undef unless $time;
	return time2ymd $time;
}


sub convert_date_to_nornal($){#Приведение даты к виду DD.MM.YYYY HH:MM:SS
	my ($date) = @_;
	
	#DD.MM.YYYY
	return ("$1.$2.$3 $4:$5:$6") if($date =~ m/(\d\d)\D(\d\d)\D(\d\d\d\d)\D(\d\d)\D(\d\d)\D(\d\d)/);
	
	#YYYY.MM.DD
	return ("$3.$2.$1 $4:$5:$6") if($date =~ m/(\d\d\d\d)\D(\d\d)\D(\d\d)\D(\d\d)\D(\d\d)\D(\d\d)/);
	
	return "";
}

sub convert_date_to_nornal_ymd($;$){#Приведение даты к виду YYYY.MM.DD HH:MM:SS
	my ($date, $delim) = @_;
	$delim = '.' unless(defined($delim));
	#DD.MM.YYYY
	return ("$3$delim$2$delim$1 $4:$5:$6") if($date =~ m/(\d\d)\D(\d\d)\D(\d\d\d\d)\D(\d\d)\D(\d\d)\D(\d\d)/);
	
	#YYYY.MM.DD
	return ("$1$delim$2$delim$3 $4:$5:$6") if($date =~ m/(\d\d\d\d)\D(\d\d)\D(\d\d)\D(\d\d)\D(\d\d)\D(\d\d)/);
	
	return "";
}

sub check_is_layer_online($$){
	my ($host, $layer_dir) = @_;
	my $res;
	$res = eval {
		remote_run_res($host, "ls \$ARCHOME/$layer_dir");
	};
	return 0 if $@;
	return 1 if $res;
	return 0;
}
return 1;

END {}

