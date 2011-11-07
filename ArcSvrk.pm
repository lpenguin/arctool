package ArcSvrk;
our $VERSION = '0.6.0.3';
use strict;

BEGIN {

	use List::Util qw(max);
	use File::Basename;
	use Cwd qw(abs_path);
	use File::Temp qw(tempfile tempdir);
	use File::Path;
	use Exporter();
	use Time::HiRes qw(gettimeofday);
	use File::Copy;
	use File::Basename;
	
	use FindBin;
	use lib "$FindBin::Bin/";
	use Misc;
	use Loggy;
	use TaskMan;
	use ErrorHandler;
	use Benchmark;
	use DbDbi;
	use LogParser;

    @ArcSvrk::ISA = qw(Exporter);
    @ArcSvrk::EXPORT = qw( &ArcInit ArcVar SrvHome ArcHome connect_disk connect_block connect_camp connect_seg create_layer_link_dir connect_layer 
    					   disconnect_disk disconnect_disk_by_name disconnect_layer connect_layer
    					   &extract_from_disk &extract_class &extract_data &extract_data_imghort &run_imghort
    					   format_class format_output get_layer_intervals get_layer_holes &find_layer_id
    					   vars_from_kks_list get_layers get_layer passport_disk format_output parse_param_str
    					    seg_remove layer_remove);
    					
	$File::Temp::KEEP_ALL = 1;
}

my ($arc_home, $srv_home, $img_hort, $out_plugin, $kks_field_length,
	 $input_idb_format, $inv_notation, $remote_server, $taskman_dir, $disk_id_file, $remote_user);
my $init_complete = 0;
my $script_path = $FindBin::Bin;
my $scripts_dir =  $script_path.'/scripts/';
my $hortman_name = 'hortman.sh';
my $hortman_path = $scripts_dir.$hortman_name;

my %arc_svrk_vars;

my $checkarch_script = $scripts_dir.'/checkarch.pl';
my $format_tasks = [
{
			name => 'table',
			func => sub{
				my ($out_file, $params) = @_;
				my ($form_file) = format_output($out_file, $params);
				
				my $file = $params->{out_dir}.'/'.$params->{camp}.'.'.$params->{server_name}.'.'.$params->{server_type}.
						'.'.$params->{task_number}.'.txt';
				unlink $file if -f $file;
				rename( $form_file, $file) or abort $!;
				info "Checking passport for errors...";
				system_run($checkarch_script.' '.$file);
			}
		}
		,
		{
			name => 'graph',
			func => sub{
				my ($out_file, $params) = @_;
				$params->{graph_format} = 1;
				for my $p (@{$params->{vars}}){
					$p->{avg_type} = 'n';
				}				
				my ($form_file) = format_output($out_file, $params);
				my $file = $params->{out_dir}.'/'.$params->{camp}.'.'.$params->{server_name}.'.'.$params->{server_type}.
						'.'.$params->{task_number}.'.pst';
				unlink $file if -f $file;
				rename( $form_file, $file) or abort $!;
				info "Checking passport for errors...";
				system_run($checkarch_script.' '.$file);
			}
		}
		];
		
sub ArcInit(%){
	my (%init_vars) = @_;
	
	my %defs = ( out_plugin => '',
				 srv_home => '',
				 arc_home => '',
				 inv_notation => 0,
				 kks_field_length => '16',
				 remote_server => '',
				 taskman_dir => '',
				 layer_notation => 'std',
				 skip_units => 0,
				 skip_vars => 0,
				 task_dir => '',
				 auto_consts => 0,
				 ini_only => 0,
				 create_sym_links => 1,
				 disk_id_file => 'arc_disk_id.txt',
				 format_tasks => $format_tasks,
				 dialog_say_yes => 0,
				 auto_consts_gen => 0, 
				 remote_user => undef, 
				 suppress_not_found_index_warnings => 0,
				 sort_indexes => 0,
				 remove_bad_output => 1,
				 arc_location => '');
	
	my $arc_ini_path = exists($init_vars{arc_ini_path})?
			$init_vars{arc_ini_path}:
			"$script_path/.arc_vars.ini";
			
	my %hash = read_ini($arc_ini_path);
		
	%defs = merge_hashes_def(\%defs, \%hash);
	%defs = merge_hashes_def(\%defs, \%init_vars);
	
	
	
	$arc_home = $defs{arc_home};
	$srv_home = $defs{srv_home};
	$defs{out_plugin} = $srv_home.'/'.$defs{out_plugin};
	$defs{img_hort} = $srv_home.'/ImGHort';
	
	$img_hort = $defs{img_hort};
	$out_plugin = $defs{out_plugin};
	$ENV{ARCHOME} = $arc_home;
	$ENV{SRVHOME} = $srv_home;
	$ENV{LC_NUMERIC} = 'C';
	$ENV{LD_LIBRARY_PATH} = $defs{srv_home};
	$defs{tmp_dir} = $srv_home.'/out/';

	mkpath( $defs{tmp_dir} );
	$remote_server = $defs{remote_server};
	$remote_user = $defs{remote_user};
	$kks_field_length = $defs{kks_field_length};
	$inv_notation =  $defs{inv_notation};
	$input_idb_format = "%-${kks_field_length}s|%-14s|%-14s|\n";
	$disk_id_file = $defs{disk_id_file};
	
		$taskman_dir = $srv_home.'/out/';
	$taskman_dir = $defs{taskman_dir} if( $defs{taskman_dir} );
	$defs{taskman_dir} = $taskman_dir;
	$defs{running_tasks_dir} = $srv_home.'/running_tasks/';
	taskman_init( (task_dir => $taskman_dir, pid_dir => $srv_home.'/pid/', running_tasks_dir =>  $defs{running_tasks_dir} ) ) ;
	set_dialog_say_yes( $defs{dialog_say_yes} );
	
# 	$input_idb_format = "%-${kks_field_length}s|\n";
	
	%arc_svrk_vars = %defs;
	$init_complete = 1;
	return \%arc_svrk_vars;
}

sub ArcVar($;$){
	my ($name, $value ) = @_;
	my $res;
	$res =  ""  unless( exists( $arc_svrk_vars{$name} ) );
	$res = $arc_svrk_vars{$name};
	$arc_svrk_vars{$name} = $value if( defined( $value ));
	return $res;
}

sub SrvHome(;$){
	my $value = shift;
	my $res = $srv_home;
	$srv_home = $value if( defined( $value ) );
	return $res;
}

sub ArcHome(;$){
	my $value = shift;
	my $res = $arc_home;
	$arc_home = $value if( defined( $value ) );
	return $res;
}

sub get_layer($$$){
	my @res = get_layers( @_ );
	return undef unless @res;
	return shift @res;
}
sub get_layers($$$){
	my ($begin_time, $end_time, $options) = @_;
	my $hole = 30;
	my $and = "";

	$options->{camp_id} = db_get_id_by_name( 'e_campaign', $options->{camp} ) 
				or abort("Campaign '$options->{camp}' not found") if $options->{camp}; 
	$options->{block_id} = db_get_id_by_name( 'e_block', $options->{block} ) 
				or abort("Block '$options->{block}' not found") if $options->{block};
	
	$and .= " and n_campaign = $options->{camp_id}" if $options->{camp_id};
	$and .= " and layer_id = $options->{layer_id_ms}" if $options->{layer_id_ms};
	$and .= " and n_arc_layer = $options->{layer_id}" if $options->{layer_id};
	if( $options->{block_id} ){
		my @camps = db_select_all_one("select n_campaign from e_campaign where n_block = $options->{block_id}");
		$and .= " and n_campaign in (".join( ", " ,@camps ).")";
	}
	
	$and .= " and name like '$options->{node_type}%'" if $options->{node_type};
	$and .= " and LOWER(name) like LOWER('\%_$options->{node_name}%')" if $options->{node_name};
	
	abort "Empty conditions for layer" unless $and;
	my @layers =  ();
	for my $layer_id ( db_select_all_one("select n_arc_layer from e_arc_layer where 
			begin_time <= '$begin_time' and end_time >= '$end_time' $and") ) {
		my @intervals = get_layer_intervals($layer_id, $hole, $begin_time, $end_time);
		push( @layers, $layer_id ) if( @intervals );
	}
	return @layers;
}

sub vars_from_kks_list(@){
	my @list = @_;
	my @res;
	for my $kks (@list){
		push @res, {
			name => $kks,
			kks => $kks
		}
	}
	return @res;
}




sub template_env($){
	my ($str) = @_;
	if( $str =~ m/^$arc_home/ ){
		$str =~ s/^$arc_home/\$ARCHOME\//;
		return rs($str);
	}elsif($str =~ m/^$srv_home/ ){
		$str =~ s/^$srv_home/\$SRVHOME\//;
		return rs($str);
	}
	return rs($str);
}

sub var_info($$){
	my ($var, $seg_id) = @_;
	
	my @altnames = @{$var->{altnames}} if( $var->{altnames} );
	my $names_str = "'".(join "','", ($var->{name}, @altnames))."'";
	
	my ($var_id, $layers, $cols, $rows, $units) = db_select_row("select v.n_var, v.layers, v.cols, v.rows, u.name 
						from e_var v, r_var_for_seg r, e_unit u
						where var_name in ($names_str) and r.n_var = v.n_var and r.n_seg = $seg_id and v.n_unit = u.n_unit") 
		or abort("Variable '$var->{name}' not found");
	
	#$units = db_get_name_by_id( 'e_unit', $units ); 
	$units ='' if( $units eq 'default');
	
	
	my $dim = 2;
	$dim = 1 if($rows == 1);
	$dim = 0 if($rows == 1 && $cols == 1);
	return ($layers, $cols, $rows, $var_id, $units, $dim);
}


sub connect_disk($){
	#TODO: generate disk id
	my ($disk_dir, $to_update) = @_;
	$disk_dir = abs_path( $disk_dir );
	$to_update ||= 0;
	
	my $disk_id_file_abs = $disk_dir.'/'.$disk_id_file;
	
	my $disk_id;
	if( -f $disk_id_file_abs ){
		$disk_id = cat( $disk_id_file_abs ) or abort("Wrond disk id");
		info "Disk id: $disk_id";
	}else{
		$disk_id = generate_id;
		info "Generating disk id: $disk_id";
		system_run("echo '$disk_id' > $disk_id_file_abs");
	}
	
	my $disk_name = basename( $disk_dir );
	$disk_name =~ s/disk_//;
	#Проверить есть ли диск в базе
	my $disk_db_id;
	unless($disk_db_id = db_get_id_by_name('e_arc_disk', $disk_name) ){
		dialog_yn("Диск '$disk_name' doesn't exist in database, create new one?") or return;
		db_insert('e_arc_disk' , ( name => qstr($disk_name), descr => qstr(''), disk_id => qstr($disk_id), online => 'true' ) );
	}else{
		#TODO: disable in future
		db_query("update e_arc_disk set disk_id=$disk_id, online=true where n_arc_disk=$disk_db_id");
	}
	
	
	#Пройти по всем блокам
	blah "Processing $disk_dir...";
	opendir my($disk_dh), $disk_dir or abort "Couldn't open dir '$disk_dir': $!";
	my @blocks = grep { !/^\.\.?$/ } readdir $disk_dh;
	for my $block (@blocks){#Для каждого блока
		next if( $block eq 'lost+found');
		log_indent();
		eval{
			connect_block($disk_dir.'/'.$block);
		};
		if($@){
			warn "Block connect error: $@";
		}
		log_unindent();
	}
	info "disk $disk_name connected";
}

sub connect_block($){
	my ($block_dir) = @_;
	$block_dir = abs_path( $block_dir );
	return unless(-d $block_dir );
	my $block = basename($block_dir);
	
	#Пройти по всем кампаниям блока
	info("Processing block $block...");
	opendir my($block_dh), $block_dir or abort "Couldn't open dir '$block_dir': $!";
	
	#Проверить есть ли блок в базе
	unless(db_get_id_by_name('e_block', $block) ){
		dialog_yn("Block '$block' doesn't exist in database, create new one?") or return;
		my $block_num = $block;
		$block_num =~ s/\D+//;
		db_insert('e_block' , ( name => qstr($block), descr => qstr(''), block_num => $block_num ) );
	}
	
	my @camps = grep { !/^\.\.?$/ } readdir $block_dh;
	for my $camp (@camps){
		next unless( $camp =~ m/^k/);
		log_indent();
		eval{
			connect_camp($block_dir.'/'.$camp);		
		};
		if($@){
			warn "Campaign connect error: $@";
		}
		log_unindent();
	}
	info "block $block connected";
}

sub connect_camp($){
	#TODO: update end and start dates for campaign
	#TODO: support for both mysql and postgresql dbases
	my ($camp_dir) = @_;
	$camp_dir = abs_path( $camp_dir );
	return unless(-d $camp_dir );
	
	my $block = basename(abs_path($camp_dir.'/../'));
	abort('Incorrect bloc kname') unless($block);
	
	my $camp_name = basename($camp_dir);
		
	$camp_name=~ s/^k//;
	my $camp_num = $camp_name;
	$camp_name = $block.'_'.$camp_name;
	info("Processing campaign $camp_name...");

	#Проверить есть ли такая кампания в базе
	my $camp_id = db_get_id_by_name('e_campaign', $camp_name);
	
	my $inserted = 0;
	unless( $camp_id ){
		dialog_yn("Campaign '$camp_name' doesn't exist in database, create new one?") or return;
		#Узнать идентификатор блока в базе
		my $block_id = db_get_id_by_name('e_block', $block);
			abort("Block '$block' doesn't exist in database") unless($block_id);;
		$camp_id = db_insert('e_campaign', ( name => qstr( $camp_name ), 
											 descr => qstr(''), 
											 b_date => qstr('1970.01.01'),
											 e_date => qstr('1970.01.01'),
											 b_date_flag => qstr('false'),
											 num_camp_npp => $camp_num,
											 num_camp_calc => $camp_num,
											 n_block => $block_id ));
		$inserted = 1;
	}
	#Пройти по всем сегментам
	opendir my($camp_dh), $camp_dir or abort "Couldn't open dir '$camp_dir': $!";
	my @segs = grep { !/^\.\.?$/ } readdir $camp_dh;
	
	my ($camp_start, $camp_end);
	
	for my $seg (@segs){	
		log_indent();
		my ($seg_start, $seg_end);
		eval{
			($seg_start, $seg_end) = connect_seg($camp_dir.'/'.$seg);		
		};
		log_unindent();
		if($@){
			warn "Segment connect error: $@";
			next;
		}
		$camp_start = $seg_start if( !defined($camp_start) || $seg_start le $camp_start);
		$camp_end = $seg_end if( !defined($camp_end) || $seg_end gt $camp_end);
	}
	if( $camp_end && $camp_start){
		db_query("update e_campaign set b_date = '$camp_start', e_date = '$camp_end'"); 
	}
 	
	info "campaign $camp_name connected";
}


sub create_layer_link_dir($$$){
	my ($server_type, $server_name, $seg_dir) = @_;
	
	$seg_dir = abs_path( $seg_dir );
	my @layers = glob "$seg_dir/${server_type}_*_$server_name";
	return unless( @layers );
	
	my $link_dir = $seg_dir.'/'.$server_type .'_'.uc( $server_name );
	my $link_ndx_dir = $link_dir.'-NDX';
	mkdir($link_dir) unless( -f $link_dir );
	mkdir($link_ndx_dir) unless( -f $link_ndx_dir );
	$link_dir = abs_path($link_dir);
	$link_ndx_dir = abs_path($link_ndx_dir);
	
	for my $layer (@layers){
		symlink_tpl($layer, $link_dir, "DAT");
		symlink_tpl($layer, $link_ndx_dir, "NDX");
		symlink_tpl("$layer-NDX/", $link_ndx_dir, "NDX");
	}

}
sub connect_seg($){
	my ($seg_dir) = @_;
		
	$seg_dir = abs_path( $seg_dir );
	return unless(-d $seg_dir );
	my $seg = basename($seg_dir);
	my $block = basename(abs_path($seg_dir.'/../../'));
	my $camp  = basename(abs_path($seg_dir.'/../'));
	info "Processing segment $seg...";
	
	#узнать идентификатор блока
	abort("Block '$block' doesn't exists in base") 
		unless( my $block_id =db_get_id_by_name('e_block', $block));

	#Создать общие директории для долговременого и оперативного архивов
	if( $arc_svrk_vars{create_sym_links} ){
		blah "Creating symbolic links";
		create_layer_link_dir("oper", "VK1", $seg_dir);
		create_layer_link_dir("long", "VK1", $seg_dir);
		create_layer_link_dir("oper", "VK2", $seg_dir);
		create_layer_link_dir("long", "VK2", $seg_dir);
		create_layer_link_dir("long", "SSDI", $seg_dir);
		create_layer_link_dir("oper", "SSDI", $seg_dir);
	}
		
	#Импортировать ИДБ-файлы
	my $idb_dir = $seg_dir.'/idb/';
	$idb_dir =~ s/\/\//\//;
	abort("IDB directory '$idb_dir' doesn't exists in segment '$seg'") unless( -d $idb_dir);

	
	my $kks_dir = $seg_dir.'/kks/';
	
	my $pos_dir =  $seg_dir.'/pos/';
	unless( -d $pos_dir ){
		warn "Pos directory '$pos_dir' doesn't exists in segment '$seg', creating new";
		gen_pos_files($idb_dir);
	}
	
	my $db_seg_dir = $seg_dir;
	$db_seg_dir =~ s/$arc_home//;
	$db_seg_dir = rs($db_seg_dir);
	my $seg_id = undef;
	my $seg_id_ms = undef;
	
	if( -f $seg_dir.'/seg_info.dat'){
		$seg_id_ms = cat($seg_dir.'/seg_info.dat');
		my $dir;
		($seg_id, $dir) = db_select_row('select n_arc_seg, dir from e_arc_seg where seg_id = '.$seg_id_ms)
			or abort("Segment with seg_id_ms = $seg_id_ms not found in dbase");
		if( $dir ne $db_seg_dir ){
			warn("Segment changed directory from '$db_seg_dir' to $dir. Updating dbase");
			$dir = $db_seg_dir;
			db_exec("update e_arc_seg set dir = '$dir' where n_arc_seg = .$seg_id");
		} 
		#$seg_id = db_get_id_by_name('e_arc_seg', $db_seg_dir, 'dir');
	}else{
		info "Creating new segment...";
		my @seg_ids;
		if( @seg_ids = db_select_all_one("select n_arc_seg from e_arc_seg where dir = '$db_seg_dir'")){
			if(dialog_yn('Segment with current directory already exists in dbase, delete it?')){
				#db_exec("delete from e_arc_seg where dir = '$db_seg_dir'");
				for my $id (@seg_ids){
					seg_remove($id);
				}
			}else{
				abort('aborted');
			}
		}
		my ($sec, $msec) = gettimeofday;
		$seg_id_ms = $sec.$msec;
		#warn "segment dir '$db_seg_dir' doesn't exist in database, creating new";
			$seg_id = db_insert('e_arc_seg', ( name => qstr( $camp.'_'.$seg.'_seg' ),
										descr => qstr(''),
										dir => qstr($db_seg_dir),
 										begin_time => qstr('1970.01.01'),
 										end_time => qstr('1970.01.01'),
 										seg_id => $seg_id_ms));
		system_run("echo $seg_id_ms >> $seg_dir/seg_info.dat");
	}
	
	vars_load_from_idb($idb_dir, $seg_id) unless( $arc_svrk_vars{skip_vars} );

	#Пройти по всем слоям
	opendir my($seg_dh), $seg_dir or abort "Couldn't open dir '$seg_dir': $!";
	my @layers;
	
	if( $arc_svrk_vars{create_sym_links} ){
		@layers = ("oper_VK1", "long_VK1", "long_VK2", "oper_VK2", "long_SSDI", "oper_SSDI");
	}else{
		@layers = grep { !/^\.\.?$/ } readdir $seg_dh;
	}
	
	log_indent();
	my ($seg_start, $seg_end);
	for my $layer (@layers){
		next if( $layer =~ m/NDX$/);
		my $layer_dir = $seg_dir.'/'.$layer;

		next if( !( -d $layer_dir)  ||  $layer eq 'idb' || $layer eq 'kks' || $layer eq 'tvs');
		log_indent();
		my ($date_start, $date_end) = connect_layer( $layer_dir );
		log_unindent();
		
		next unless( defined( $date_start ) );
		
		$seg_start = $date_start if( !defined($seg_start) || $date_start le $seg_start);
		$seg_end = $date_end if( !defined($seg_end) || $date_end gt $seg_end);

	}
	db_query("update e_arc_seg set begin_time = '$seg_start', end_time = '$seg_end' where n_arc_seg = $seg_id");
	log_unindent();
	closedir $seg_dh;
	info "segment $seg connected";
	return ($seg_start, $seg_end);
}

sub connect_layer($){
	my ($layer_dir) = @_;

	$layer_dir = abs_path( $layer_dir );
	my $layer = basename($layer_dir);
	my $db_layer_dir = $layer_dir;
	$db_layer_dir =~ s/$arc_home//;
	$db_layer_dir = rs($db_layer_dir);
	
	info "Processing layer $layer...";
	#Проверить, есть ли в директории слоя *.dat файлы
	my @dir_files = glob("$layer_dir/*.DAT");
	if($#dir_files == -1){
		 blah "Layer '$layer_dir' is empty!" ;
		 return;
	}
	
	my ($block, $camp, $server, $type, $layer_id, $date_start, $date_end, $layer_db_id, @files);
	
	if(-f $layer_dir.'/layer_info.dat' ){
		
		#Если описатель слоя уже существует, то описатель обновить
		($block, $camp, $server, $type, $layer_id, $date_start, $date_end, @files) = gen_layer_info($layer_dir, 1);

		$layer_db_id = db_select_one("Select n_arc_layer from e_arc_layer where layer_id=$layer_id");#db_get_id_by_name('e_arc_layer', $layer_id, 'layer_id');
		if( $layer_db_id ){
			#Если слой существует базе, удалить все его файлы
			layer_remove_files( $layer_db_id );
			my $descr = ArcVar('arc_location');
			
			db_query("update e_arc_layer set begin_time='$date_start',end_time='$date_end', online=true, descr = '$descr', dir='$db_layer_dir' where n_arc_layer = $layer_db_id");
		}else{
			#Если не существует, создать новый слой в базе
			$layer_db_id = layer_add( $block, $camp, $server, $type, $layer_id, $date_start, $date_end, $layer_dir );
		}
	}else{
		#Если описателя слоя нет, то создать новый
		($block, $camp, $server, $type, $layer_id, $date_start, $date_end, @files) = gen_layer_info( $layer_dir );		
		#Добавить новый слой в базу
		$layer_db_id = layer_add( $block, $camp, $server, $type, $layer_id, $date_start, $date_end, $layer_dir );
	}
	
	#Добавить файлы слоя в базу
	layer_add_files( $layer_db_id, @files );
	info "layer $layer connected";
	return ($date_start, $date_end);
}

sub get_layer_intervals($$;$$){
	my ($layer_id, $max_hole_length, $begin_time, $end_time) = @_;
	my @res;
	my ($length, $hole_start, $hole_end);
	unless( defined($begin_time)){
		($begin_time, $end_time) = db_select_row("select begin_time, end_time from e_arc_layer where n_arc_layer = $layer_id");
	}
	$begin_time = convert_date_to_nornal_ymd($begin_time,'-');
	$end_time = convert_date_to_nornal_ymd($end_time,'-');
	return () if( $begin_time gt $end_time );
	my ($min_btime, $max_etime) = db_select_row("select min( begin_time ), max( end_time ) from e_arc_file where n_arc_layer = $layer_id 
			");#"and end_time > '$begin_time' and begin_time < '$end_time'" );

	$begin_time = $min_btime if( str2time( $begin_time) < str2time( $min_btime ) );
	$end_time = $max_etime if( str2time($end_time ) > str2time($max_etime) );
	
	my ($int_b, $int_e) = ($begin_time, undef);
	
		
	my $all_ref = db_select_all( "select begin_time, end_time from e_arc_file where n_arc_layer = $layer_id 
			and end_time >= '$begin_time' and begin_time <= '$end_time'
			order by begin_time" );
	my @all = @$all_ref;
	if( $#all ==- 1){
		return ();
	}
	my ($file_b, $file_e);
	for( my $i = 0; $i < $#all && $#all; $i++){
		my ($file_b, $file_e) = @{$all[$i]};
#		info "begin_time: $file_b, end_time: $file_e";
		my ($next_file_b) = @{$all[$i+1]};
		$int_b = $file_b unless( defined( $int_b ));
#		info "diff $next_file_b - $file_e: ".( ymd2time( $next_file_b ) - ymd2time( $file_e ) );
		if( ymd2time( $next_file_b ) - ymd2time( $file_e ) > $max_hole_length){
			$int_e = $file_e;
			if( str2time($int_e) - str2time($int_b) >= 1 ){
				push @res, { begin_time => convert_date_to_nornal_ymd($int_b),
							 end_time => convert_date_to_nornal_ymd($int_e)};
				
			}
			$int_b = undef;
		}
	}
	$int_b = $all[$#all]->[0] unless( defined( $int_b ) );
	$int_e = $end_time;#$all[$#all]->[1];
	push @res, { begin_time => convert_date_to_nornal_ymd($int_b),
				 end_time => convert_date_to_nornal_ymd($int_e)};
	
	return @res;
}

sub gen_layer_info($;$){

	log_indent();
	#NOTE: Формат даты в базе Sqlite YYYY.MM.DD 
	#	   Формат даты в ini-файле ImGHort 	DD.MM.YYYY
	
	my ($layer_dir, $to_update) = @_;
	$layer_dir = abs_path($layer_dir);
	my $layer_info = $layer_dir.'/layer_info.dat';
	
	my $idb_dir = $layer_dir.'/../idb/'; $idb_dir = abs_path( $idb_dir );
	
	my $input_idb = $idb_dir.'/ImgHort.idb'; $input_idb =~ s/\/\//\//;
	my $index_dir = rse($layer_dir).'-NDX/';
	#Идентификатор слоя - количество секунд на момент создания
	my $layer_id;
	
	
	if($to_update){#Если нужно обновить слой, 
					#то надо сохранить информацию о идентификаторе слоя
		#читаем файл описания слоя
		open(my $fd, "$layer_info") or abort "Cannot read file $layer_info";
		
		
		while(<$fd>) {
			last if(m/^files/);
			chomp;
			next unless( $_ );
			m/(\S*)\s*(.*)/;
			
			$layer_id = $2 if($1 eq 'layer_id');
		}
		abort("Нет параметра layer_id в файле описателе") unless($layer_id);
	
	}

	my $task = CreateTask('ImGHorttimes_', { remote_server => $remote_server}, sub{
		my $cur_task = shift;
		log_indent;
		run_imghort( $cur_task, 0 );
		log_unindent;
	});
	my $ini_file = $task->{dir}.'/'.$task->{name}.'.ini';
	my $out_file = $task->{output_file};
	template $script_path.'/templates/ImGHorttimes.tpl.ini', $ini_file ,
		(
			INPUT_IDB => template_env($input_idb ),
			LAYER_PATH => template_env($layer_dir),
			IDB_PATH => template_env($idb_dir),
			OUT_FILE => template_env($out_file),
			INDEX_PATH => template_env($index_dir),
			LOG_DIR => template_env($task->{dir})
		);
	
	copy( $ini_file, $srv_home.'/ini/'.$task->{name}.'.ini' );
	
	unless($to_update){
		blah("Generating layer info...");
	}else{
		blah("Updating layer info...");
	}

	RunTask( $task );
	
	#if remote
# 	if( $remote_server ){
# 		#TODO: Make index dir on remote servers
# 		blah "Copying input files on remote '$remote_server'";
# 		remote_copy($remote_server, $ini_file, 'skif/ini');
# 		blah "Running task $ini_name on '$remote_server'";
# 		#run_imghort($ini_name, $rout_file, 0, $remote_server);
# 		blah "Copying results from remote '$remote_server'";
# 		remote_copy_back( $remote_server, env_str($out_file, $srv_home, 'skif/' ), $out_file ); 
# 		blah "Task end";
# 	}else{
# 		blah "Running task $ini_name on local";
# 		
# 		#run_imghort($ini_name, undef, 0, $remote_server);
# 	}
	
	my $in_fd = open_read($out_file);
	my @files;
	my ( $b_time, $e_time, $lines );
	my ($name, $file_begin_time, $file_end_time);
	<$in_fd>;<$in_fd>;<$in_fd>;<$in_fd>;
	while( <$in_fd> ){
		next if (/^#/);
		s/^\s+//;
		($name, $file_begin_time, $file_end_time) = split /\|/; 
		if( $name =~ m/\S*DAT/ ){
			$name=~s/\s+//;
		    $file_end_time=~s/\s/_/;
		    $file_begin_time =~ m/\.(\d\d\d)$/;
		    my $msecs = $1;
			$file_begin_time = make_time_correction($file_begin_time, 0, 0, 1);
			$file_begin_time=~s/\s/_/;
			$file_begin_time.='.'.$msecs;
			$lines .= "$name\t$file_begin_time\t$file_end_time\n";
			push @files, {
				file => $name, 
				begin_time => dmy2ymd(remove_msecs( $file_begin_time )), 
				end_time => dmy2ymd(remove_msecs( $file_end_time )) }; 
			$b_time = $file_begin_time unless( defined( $b_time ) );
		}
	}
	close ($in_fd );
	
	$e_time = $file_end_time;
	
	my $seg = dirname($layer_dir);
	my $server_kind=basename($layer_dir);
	$server_kind=~ m/^([^_]+)_([^_]+)/;
	
	my $camp  = dirname($seg);
	my $block = basename( dirname($camp) );
	$camp = basename( $camp );
	
	#Если не обновлять, вычислить $layer_id заново
	unless( $to_update ){
		my ($sec, $msec) = gettimeofday;
		$layer_id = $sec.$msec;
	}

	my ( $server, $kind );
	if( $server_kind =~ m/(oper|long)_[\d\-\_]+_([a-zA-z\d\-]+)/ ){
		$server = $2;
		$kind = $1;
	}elsif($server_kind =~ m/(oper|long)_([a-zA-z\d\-]+)_[\d\-]+/){
		$server = $2;
		$kind = $1;
	}elsif($server_kind =~ m/([a-zA-z\d\-]+)_[\d\-]+_(oper|long)/){
		$server = $1;
		$kind = $2;
	}elsif($server_kind =~ m/([a-zA-z\d\-]+)_(oper|long)_[\d\-]+/){
		$server = $1;
		$kind = $2;
	}if( $server_kind =~ m/(oper|long)_([a-zA-z\d\-]+)/ ){
		$server = $2;
		$kind = $1;
	}else{
		abort("Wrong layer notation '$server_kind'");
	}

	unlink $layer_info if -f $layer_info;
	
	my $fd = open_write( $layer_info );
	print $fd sprintf("%-30s%s\n", 'block', $block);
	print $fd sprintf("%-30s%s\n", 'camp', $camp);
	print $fd sprintf("%-30s%s\n", 'server', $server);
	print $fd sprintf("%-30s%s\n", 'kind', $kind);
	print $fd sprintf("%-30s%s\n", 'layer_id', $layer_id);
	print $fd sprintf("%-30s%-30s%s\n", 'range', $b_time, $e_time);
	print $fd sprintf("%-30s\n", 'files');
	print $fd $lines;	

	$b_time = dmy2ymd(remove_msecs( $b_time)); 
    $e_time = dmy2ymd(remove_msecs( $e_time ));
		
	$b_time = make_time_correction_ymd($b_time, 0, 0, 1);
	$e_time = make_time_correction_ymd($e_time, 0, 0, -1);
	
	blah "Block: $block";
    blah "camp: $camp";
    blah "server: $server";
    blah "arc_type: $kind";
	blah "layer_id: $layer_id";
	blah "layer range: $b_time - $e_time";
	log_unindent();
	
	unlink $out_file;
	return ($block, $camp, $server,$kind, $layer_id, $b_time, $e_time, @files);
}

sub disconnect_layer($;$){
	my ($layer_id, $use_as_db_id) = @_;
	$layer_id = db_get_id_by_name('e_arc_layer', $layer_id, 'layer_id') or abort("layer '$layer_id' not found") if( $use_as_db_id );
	db_query("update e_arc_layer set online=false where n_arc_layer=$layer_id");
}

sub seg_remove($){
    my ($seg_id) = @_;
	for my $layer_id ( db_select_all_one("select n_arc_layer from e_arc_layer where n_arc_seg = $seg_id") ){
        info "removing $layer_id";
        layer_remove($layer_id);
    }
	db_exec("delete from r_var_for_seg where n_seg = $seg_id");
	db_exec("delete from e_arc_seg where n_arc_seg = $seg_id");
}

sub disconnect_disk($;$){
	my ($disk_id, $use_as_db_id) = @_;
	$disk_id = db_get_id_by_name('e_arc_disk', $disk_id, 'disk_id') or abort("disk '$disk_id' not found") if( $use_as_db_id );
	db_query("update e_arc_disk set online=false where n_arc_disk=$disk_id");
	db_process("select n_arc_layer from e_arc_layer where n_arc_disk = $disk_id", sub{
		my ($layer_id) = @_;
		disconnect_layer($layer_id);
	});
}

sub disconnect_disk_by_name($){
	my ($name) = @_;
	my $disk_id = db_get_id_by_name('e_arc_disk', $name) or abort("Disk '$name' not found");
	disconnect_disk($disk_id);
}

sub load_units(%){
	my (%units) = @_;
	my $outer_encoding = 'utf-8';
	my $inner_encoding = 'koi8-r';

	for my $munit (keys(%units)){
		my $name = iconv( $units{$munit}->{Sym}, 'koi8-r', 'utf-8' );
		my $unit_id =db_select_one("select n_unit from e_unit where name = '$name' and munit = $munit");
		if($unit_id){
			$units{$munit}->{unit_id} = $unit_id;
		}else{
			$units{$munit}->{unit_id} = db_insert('e_unit', ( name => qstr($name),
							  munit => $munit)) 		}
	}
}

sub layer_add($$$$$$$$){
	my ($block, $camp, $server, $arc_type, $layer_id, $begin_time, $end_time, $layer_dir, @files) =@_;

	#Узнать id кампании
	my $camp_name = $camp;
	$camp_name =~ s/k//;
	$camp_name = $block.'_'.$camp_name;
	
	my $camp_id = db_get_id_by_name('e_campaign', $camp_name);
	unless( $camp_id ){
		abort("Не существует кампании $camp_name");
		if( dialog_yn("Не существует кампании $camp_name, создать новую?") ){
			#Создать кампанию
			$camp_id = db_insert('e_campaign', ( name => qstr( $camp_name ) ));
		}else{
			exit 1;
		}
		
	}
	
	#Узнать id сервера
	my $server_id = db_get_id_by_name('e_svrk_node', lc $server);
	unless( $server_id ){
		info "Adding new server $server";
		$server_id = db_insert('e_svrk_node', (name => qstr(lc $server), descr => qstr('')));
	}
	
	#Узнать id типа архива
	#my $arc_type_id = db_get_id_by_name('e_arc_type', $arc_type);
	#abort "Не существует типа архива $arc_type" unless( $arc_type_id );
	
	#Узнать id ИДБ-файла
	#my $idb_dir = $layer_dir.'/../idb/';
	#$idb_dir = abs_path($idb_dir).'/';
	
	#Узнать идентификатор диска
	my $disk_name = basename(abs_path($layer_dir.'/../../../../'));
	$disk_name =~ s/^disk_//;
	
	my $disk_id = db_get_id_by_name('e_arc_disk', $disk_name) or abort("Диск с именем '$disk_name' не найден в базе данных");	
	
	#Узнать id сегмента
	my $seg_dir = $layer_dir.'/..';
	$seg_dir = abs_path($seg_dir);
	$seg_dir =~ s/$arc_home//;
	
	my $seg_id = db_get_id_by_name('e_arc_seg', $seg_dir, 'dir');	
	abort "Не существует сегмента '$seg_dir'" unless( $seg_id );
	#my $idb_id = db_get_id_by_name('e_idb_set', $idb_dir, 'dir');
	#abort "Не существует ИДБ-файла $idb_dir" unless( $idb_id );
	
	$layer_dir =~ s/$arc_home\/{0,1}//;
	my $bt = $begin_time; $bt =~ s/\s/-/g;
	my $layer_name = "${arc_type}_${server}_${bt}";
	#basename($layer_dir);#"$camp_name-".lc($server)."-$arc_type";
	my $arc_location = ArcVar('arc_location');
	my $layer_db_id = db_insert('e_arc_layer', (name => qstr($layer_name),
								descr => qstr($arc_location),
								begin_time => qstr($begin_time),
								end_time => qstr($end_time),
								dir => qstr($layer_dir),
								online => 'true',
								layer_id => $layer_id,
								n_campaign => $camp_id,
								n_svrk_node => $server_id,
	#							n_arc_type => $arc_type_id,
								n_arc_disk => $disk_id,
								n_arc_seg => $seg_id
								));
	blah "layer_id $layer_db_id";
	return $layer_db_id;
}

sub layer_add_files($@){
	my ($layer_db_id, @files) = @_;
	
	my $files_count = $#files + 1;
	blah "importing $files_count files...";
	
	for my $file (@files) {
		my $name = $file->{file};

		#blah "importing file: $name";
		$file->{begin_time} = make_time_correction_ymd( $file->{begin_time}, 0, 0, 1 );
		$file->{begin_time} =~ s/\./-/g;
		$file->{end_time} =~ s/\./-/g;
		db_insert('e_arc_file', (
									name => qstr($file->{file}),
									descr => qstr(''),
									begin_time => qstr($file->{begin_time}),
									end_time => qstr($file->{end_time}),
									n_arc_layer => $layer_db_id
								));
	}	
}

sub layer_remove_files($){
	my ($layer_db_id) = @_;
	
	#Удалить все старые файлы слоя 
	db_query("delete from e_arc_file where n_arc_layer = $layer_db_id");
}

sub layer_remove($;$){
	my ($layer_id, $use_as_db_id) = @_;
	
	$use_as_db_id = 1 unless(defined($use_as_db_id));

	my $db_layer_id;
	
	if($use_as_db_id){
		$db_layer_id = $layer_id;
	}else{
		$db_layer_id = db_select_one("select n_arc_layer from e_arc_layer where layer_id = $layer_id");#db_get_id_by_name('e_arc_layer', $layer_id, 'layer_id');
	}
	
	unless($db_layer_id){
		abort("Слоя $layer_id не существует в базе");
	}
	
	#Удалить все старые файлы слоя 
	layer_remove_files($db_layer_id);
	
	#Удалить слой
	db_query("delete from e_arc_layer where n_arc_layer = $db_layer_id");	
	
	blah("Удален слой $layer_id");
}

sub gen_pos_files($){
	my ($idb_dir) = @_;
	
	my $pos_dir = $idb_dir.'/../pos/';
	rmtree($pos_dir);
	mkpath($pos_dir) or abort("Cannot mkdir '$pos_dir'");
	my $pos_file = $idb_dir.'/SrvCoreTvs.idb';
	open my $fd, $pos_file or abort("Cannot open for reading '$pos_file'");
	<$fd>;
	<$fd>;
	my $l = <$fd>;
	my @cols = split m/\s+\|/, $l;
	my %field_names; my $c=0;
	for my $name (@cols){
		$field_names{$name} = $c++;
	}
	
	my @param_names = ('NOrb', 'NKni', 'NTp', 'NGrSuz', 'NSuz');


	while(<$fd>){
		@cols = split m/\s+\|/;
		for my $name (@param_names) {
			my $n_par = $cols[$field_names{$name}];
			$n_par = 0 unless($n_par);
			my $par_fd = open_write( $pos_dir.'/'.$name.'.pos' );
			#open my $par_fd, '>>', $tvs_dir.'/'.$name.'.tvs' or abort("Cannot open for writing '$tvs_dir/$name.tvs'");
			print $par_fd "$cols[0]\t$n_par\n";
			close $par_fd;
		}
	}
	close $fd;
}

sub var_add($$){
	#TODO: добавить сокращения для типов переменных
	#TODO: избегать дублирования переменных (если добавляемая переменная существует, но размерность другая)
	#TODO: избегать дублирования переменных (если добавляемая переменная существует с размерностью 1)
	
	my ($var, $seg_id) = @_;

	my ($name, $type, $l, $r, $c, $unit_id);
	
	$unit_id = $var->{unit_id};
	$name = $var->{name};
	$type = "float";
	$l = 1;
	$c = $var->{cols};
	$r = $var->{rows};

	my $short_type = $type;
	$short_type = 'fl' if( $type eq 'float' );
	my $full_name = "${name}_${short_type}_${l}x${r}x${c}";

	#my $block_name = db_select_one_by_id('e_block', 'name', $block_id );
	
	my ( $var_id, $cols, $rows, $layers );
	$layers = 1;
	#Узнать существует ли переменная с таким именем
	if( ($var_id, $cols, $rows, $layers) = db_select_row(
		"select n_var, cols, rows, layers from e_var where var_name = '$name' and layers= $l and cols = $c and rows = $r") ){
		$var->{id} = $var_id;
		connect_var($var_id, $seg_id);
	}else{
		#Создать новую запись для переменной
		# узнать какие константы соответствуют размерностям переменной
		blah "Adding var $full_name";

		$var_id = db_insert( 'e_var', (name => qstr($name), 
							 descr => qstr(''),
							 var_name => qstr($name),
							 var_type => qstr($type),
							 layers => $l,
							 rows => $r,
							 cols => $c,
							 n_unit => $unit_id));
		
		
		#свяжем переменную с *#)@(*FJ
		connect_var($var_id, $seg_id);
	}

						 
}

sub connect_var($$){
	my ($var_id, $seg_id) = @_;
	unless( db_select_row("select n_var_for_seg from r_var_for_seg where n_seg = $seg_id and n_var = $var_id")){
		db_insert( 'r_var_for_seg', ( n_var => $var_id, n_seg => $seg_id) );
	}
}

sub vars_load_from_idb($$){
		
	my ($idb_dir, $seg_id) = @_;
	
	my $kks_dir = $idb_dir."/../kks/";

	blah "Reading units from $idb_dir/MUint.idb";
	my %units = parse_idb_index($idb_dir.'/MUnit.idb', 'MUnit', 'Sym'); 
	
	load_units(%units);

	blah "Reading variables from $idb_dir/GateHortIn.idb";
	my $varname;
	my %kks_in = read_kks_from_idb("$idb_dir/GateHortIn.idb", \%units);
	
	blah "Reading variables from $idb_dir/GateHortOut.idb";
	my %kks_out= read_kks_from_idb("$idb_dir/GateHortOut.idb", \%units);
	
	my (%kks_uir, %kks_vars);
#	if( -f "$idb_dir/gc_uir_in.idb"){
#		blah "Reading variables from $idb_dir/gc_uir_in.idb";
#		my %kks_uir= read_kks_from_idb("$idb_dir/gc_uir_in.idb", $block_id, \%units);
#	}

	if( -f "$idb_dir/_vars.idb"){
		blah "Reading variables from $idb_dir/_vars.idb";
		%kks_vars= read_kks_from_idb("$idb_dir/_vars.idb", \%units);
	}

	my $kks_all = merge_hashes( \%kks_in, \%kks_out);
	$kks_all = merge_hashes( $kks_all, \%kks_vars);
	
	blah "Writing variables to base and kks files";
	mkdir $kks_dir;
	$kks_dir = abs_path( $kks_dir );
	while( my ($key,$var)=each(%{$kks_all})){
		if($var->{max_index2} == 1){#Одномерная переменная
			$var->{cols} = $var->{max_index1};
			$var->{rows} = 1;
		}else{#Двумерная переменная
			$var->{cols} = $var->{max_index2};
			$var->{rows} = $var->{max_index1};
		}
		
		var_add($var, $seg_id);
		open(FILEHANDLE,">", "$kks_dir/$key.kks") or abort("Cannot open for writing $kks_dir/$key.kks");	
		for my $kks (@{$var->{kks}}){
			print FILEHANDLE sprintf( "%s\t%s\t%s\n", $kks->{kks}, $kks->{index1}, $kks->{index2});
		}
		close(FILEHANDLE);
		
	}
}

sub read_kks_from_idb($$){
	my ($idb_file,  $units_ref) = @_;
	my %units = %{$units_ref};
	my ($fd, @fields_num) = open_read_idb($idb_file,
									  ('ShortName', 'VarName', 'Index1', 'Index2', 'MUnit'));
	my %kks_hash;
	while( <$fd> )
	{
		my @v = split m/\s*\|/;
		
		my ($kks, $key, $index1, $index2, $munit) = array_get(\@v, @fields_num);
		my $unit_id = $units{$munit}->{unit_id};
		$unit_id = 1 unless($unit_id);
		unless( exists($kks_hash{$key}) ){
			$kks_hash{$key} = { max_index1 => $index1+1,
							  max_index2 => $index2+1,
							  name => $key,
							  munit => $munit,
							  unit_id => $unit_id,
							  kks => [{ kks=> $kks, index1=> $index1, index2 => $index2}]
							  };
		}else{
			my $var = $kks_hash{$key};
			$var->{max_index1} = $index1+1 if( $index1+1 > $var->{max_index1} );
			$var->{max_index2} = $index2+1 if( $index2+1 > $var->{max_index2} );
			push @{$var->{kks}}, { kks=> $kks, index1=> $index1, index2 => $index2};
		}
	
	}
	
	close ($fd);
	
	for my $var_name (keys( %kks_hash)){
		my $var = $kks_hash{$var_name};
		my @kks = @{ $var->{kks} };
		@kks = sort { $a->{index2} <=> $b->{index2} or $a->{index1} <=> $b->{index1} } @kks;
		$var->{kks} = \@kks;
	}
	
	#@res = sort { $a->{index1} cmp $b->{index1} or $a->{index2} cmp $b->{index2} } @res;
	
	return %kks_hash;
}

sub compare_vars_kks($$){
	my ($vars1, $vars2) = @_;
	my $i = 0;
	for( my $i = 0; $i < @$vars1; $i++){
		abort("Different vars names. '$vars1->[$i]->{name}' ne '$vars2->[$i]->{name}")
			if( $vars1->[$i]->{name} ne $vars2->[$i]->{name});
			
			for( my $j = 0; $j < @{$vars1->[$i]->{kks}}; $j++){
			return 0 if( $vars1->[$i]->{kks}->[$j]->{kks} ne $vars2->[$i]->{kks}->[$j]->{kks} );
		}
			
#			return 0 unless( arr_cmp_str( $vars1->[$i]->{kks} , $vars2->[$i]->{kks} ) );  
	}
	return 1;
}

sub run_imghort($;$){
	my ($task, $remove_last_line) = @_;
	$remove_last_line = 1 unless( defined( $remove_last_line));
	
	if($remote_server){
		unlink $task->{log_file};
		info "Copying task files on  $remote_server";
		remote_copy( $remote_server, $task->{dir}.'/', 'skif/out/'.$task->{name});
		remote_copy( $remote_server, "$task->{dir}/$task->{name}.ini", "skif/ini/$task->{name}.ini");

		#ImGHort Run
		info "Running ImGHort on $remote_server";
		remote_run( $remote_server, '$HOME/skif/'.$hortman_name.' '.$task->{name} );
		info "Copying results";
		remote_copy_back( $remote_server, 
			"skif/out/$task->{name}/output.txt", $task->{output_file} );
		remote_copy_back( $remote_server, 
			"skif/out/$task->{name}/log.txt", $task->{log_file} );
	}else{
		system_run("$srv_home/ImGHort $task->{name} > $task->{output_file}");
		db_connect();#reconnect dbi interface after child process run;
	}
	if( $remove_last_line && ArcVar('remove_bad_output')){
	    info "REMOWING LAST LINE (ImGHort bug)";
	    crop_last_line($task->{output_file});
	}
}

sub passport_disk(%){
	my (%input) = @_;
	
	my $disk_name = $input{disk};
	my $disk_id = db_get_id_by_name('e_arc_disk', $disk_name) or abort("Disk '$disk_name' not found in base");
	my $out_dir = "$srv_home/scan/$disk_name/";
	$input{disk_id} = $disk_id;
	mkpath( $out_dir.'/log' );
	my $time = time2ymd(time);
	$time =~ s/\s/_/g;
	set_log_file("$out_dir/log/extract_log_$time.txt");
	
	my $camps_ref = db_select_all("select distinct n_campaign from e_arc_layer where n_arc_disk = $disk_id order by n_campaign");
	my @camps = @$camps_ref;
	
	$input{camp_id} = db_get_id_by_name("e_campaign", $input{camp}) if( $input{camp} );
	for my $camp_row (@camps) {
		my $camp_id = shift @$camp_row;
		db_process("select distinct substr(name, 1, 4), (select name from e_svrk_node where n_svrk_node = l.n_svrk_node) 
			from e_arc_layer l where n_campaign=$camp_id and n_arc_disk = $disk_id", sub{
			my ($type, $name) = @_;
			my $camp_name = db_select_one("select name from e_campaign where n_campaign = $camp_id") or abort("Camp not found");
			
			return if( defined( $input{node_type} ) && $input{node_type} ne $type  );
			return if( defined( $input{node_name} ) && $input{node_name} ne $name  );
			return if( defined( $input{camp_id} ) && $input{camp_id} != $camp_id  );
			
			my $class = "$camp_name/$type/$name";
			return if( defined( $input{class} ) && $input{class} ne $class);			
			info "Processing class $class";
			log_indent;
			extract_class($camp_id, $type, $name, $out_dir, %input);
			log_unindent;
		});
	}
}

sub extract_class($$$$%){
	my ($camp_id, $type, $name, $out_dir, %input) = @_;
		
	my $camp_name = db_select_one("select name from e_campaign where n_campaign = $camp_id") or abort("Camp not found");
	my $short_camp = $camp_name; $short_camp =~ s/[^_]+_//; $short_camp = 'k'.$short_camp;
	
	my $camp = $camp_name;$camp =~ s/^.*_//;$camp = 'k'.$camp;
	$out_dir = $out_dir.'/'.$camp;
	my $task_dir = $out_dir.'/tasks/';
	mkpath $task_dir;
	my $type_name = lc "${type}_${name}";
	my ($begin_time, $end_time) = db_select_row("select min(begin_time), max(end_time) from e_arc_layer where n_campaign=$camp_id and LOWER(name) like '${type_name}_%'");
#	my $layer_id = get_layer( $begin_time, $end_time, $params) or abort("Layer not found");
	my $disk_id = $input{disk_id};

	$input{server_name} = $name;
	$input{server_type} = $type;
	$input{begin_time} = $begin_time;
	$input{end_time} = $end_time;
	$input{camp} = $camp_name;
	$input{block} = db_select_one("select b.name from e_block b, e_campaign c where b.n_block = c.n_block and c.n_campaign = $camp_id");
	my @layer_tasks;
	
	
#	my @seg_ids = db_select_all_one("select n_arc_seg from e_arc_seg where n_arc_disk = $disk_id");
#	my $seg_ids = join ",", @seg_ids;
	my $class_task = CreateTask("Class_", \%input, sub{
		my $current_task = shift;
		my $layer_end_time;
		db_process("select begin_time, end_time, dir, n_arc_layer, name, layer_id from e_arc_layer 
					where n_campaign=$camp_id and n_arc_disk = $disk_id and UPPER( name ) like UPPER( '${type}_${name}_%' ) order by begin_time", sub{
		
			my ($begin_time, $end_time, $layer_dir, $layer_id, $layer_name, $layer_id_ms) = @_;
			if( defined( $layer_end_time ) and ( str2time( $begin_time ) < str2time( $layer_end_time ) + 1 ) ){
				$begin_time = time2ymd( str2time($layer_end_time) + 1 );
			}
			$layer_end_time = $end_time;
			
			#$begin_time = $input{class_begin} if($input{class_begin});
			info "Processing layer: $layer_dir, camp: $camp_name, \n time range: $begin_time - $end_time, \n layer_id: $layer_id_ms";
			log_indent;
				#safely extracting data
				eval{
					my $task = extract_data(merge_hashes(\%input, 
							{begin_time => $begin_time, 
							end_time=> $end_time,
							layer_id => $layer_id,
							hole => 30 }));
					push @layer_tasks, $task;
					LinkTask( $current_task, $task );
					RunTask( $task );
# 					AppendLog( $current_task, $task );
				};
				if($@){
					warn "Error: $@";
				}else{
					#system_run("cat '$res_file' >> $layer_out_file");
					info "Layer OK.";
				}
			log_unindent;
		}) or abort("Layers not found for camp: $camp_name, type: $type, name: $name");
	});
	
	my $tasks_count = files_count(  $task_dir );
	create_link( $class_task->{dir},  $task_dir.($tasks_count+1).'.'.$class_task->{name} );
	RunTask( $class_task );
	
	unless( @layer_tasks){
		warn "Empty class";
		return;
	}
	format_class( $class_task, \@layer_tasks,  $input{tasks});
}

sub format_class( $$$ ){

	my ($class_task, $layer_tasks, $format_tasks) = @_;

	my $c = 0;
	my $prev_task = $layer_tasks->[0];
	my $begin_time = $prev_task->{params}->{begin_time};
	my @format_tasks;
	my ($camp, $name, $type, $disk) =
	 ($class_task->{params}->{camp}, $class_task->{params}->{server_name}, $class_task->{params}->{server_type}, $class_task->{params}->{disk} );

 	unlink for glob "$class_task->{dir}/output*";
	my $short_camp = $camp;
	$short_camp =~ s/^.*_//;$short_camp = 'k'.$short_camp;
	my $out_dir = "$srv_home/scan/$disk/$short_camp/";	
	
	while(  my $task = shift @$layer_tasks ){
		unless( compare_vars_kks( $task->{params}->{vars}, $prev_task->{params}->{vars} ) ){
			info "KKS changed";
			unless( $c ){
				rename "$class_task->{dir}/output.0.txt", "$class_task->{dir}/output.1.txt";
				$c = 1 ;
			}
			my $params = copy_hash( $prev_task->{params} );
			$params->{begin_time} = $begin_time;
			$params->{task_number} = $c;
			$params->{out_dir} = $out_dir;

			for my $format_task (@$format_tasks ){
				info "running format task: '$format_task->{name}'"; 
				$format_task->{func}->("$class_task->{dir}/output.$c.txt", $params);
			}
			$c++;
			$begin_time = $task->{params}->{begin_time};
		}
		cat_to( $task->{output_file}, "$class_task->{dir}/output.$c.txt" );
		$prev_task = $task;
	}
	my $params = copy_hash( $prev_task->{params} );
	$params->{begin_time} = $begin_time;
	$params->{task_number} = $c;
	$params->{out_dir} = $out_dir;

	for my $format_task (@$format_tasks ){
		info "running format task: '$format_task->{name}'"; 
		$format_task->{func}->("$class_task->{dir}/output.$c.txt", $params);
	}
			

#	my $params = copy_hash( $prev_task->{params} );
#	$params->{begin_time} = $begin_time;
#	$params->{task_number} = $c;
#			
#	for my $format_task (@$format_tasks ){
#		info "running format task: '$format_task->{name}'"; 
#		$format_task->{func}->("$class_task->{dir}/output.$c.txt", $params);
#	}
}

sub extract_data($;$){#Выборка данных с учетом временных промежутков между файлами
	my ($params, $run_task) = @_;

	my %input = %{$params};
	
	
	my $begin_time =  str2ymd( $params->{begin_time} );
	my $end_time = str2ymd( $params->{end_time} );
	$params->{begin_time} = $begin_time;
	$params->{end_time} = $end_time;
	
	my $layer_id = get_layer( $begin_time, $end_time, $params) or abort("Layer not found");
	$params->{layer_id} = $layer_id;

	my $max_hole = $params->{hole};
	$max_hole = 30 unless( defined( $max_hole ) );
	my @intervals = get_layer_intervals($layer_id, $max_hole, $begin_time, $end_time);
	
	#Если работа будет на удаленном сервере, скопировать туда менеджер запуска
	if( $params->{remote_server}){
		$remote_server = $params->{remote_server};
		info "Copying hortman to $remote_server";
		remote_copy( $remote_server, $hortman_path, 'skif/'.$hortman_name);
		remote_run( $remote_server, 'chmod +x $HOME/skif/'.$hortman_name);
		
	}
	
	#Подготовить параметры для выборки
	$params = prepare_task_params( $params );

	
	my $step = $params->{step} / 1000;
	blah "layer: ". $params->{layer_dir};
	my $layer_task = CreateTask('Layer_', $params, sub{
		my $current_task = shift;
		
		my $c = 1;
		while(my $int = shift @intervals){
			blah "Interval ($c\/".($#intervals + 2).") $int->{begin_time}\t$int->{end_time}";
			my $n1 = int( ( str2time($int->{begin_time}) - str2time( $begin_time) ) / $step );
			my $n2 = int( ( str2time($int->{end_time}) - str2time( $begin_time) ) / $step );
			
			log_indent;
			if( $c != 1 &&   $n2 <= $n1 ){
				info "n2 $n2 <= n1 $n1, no usefull data, skipping";
				$c++;
				log_unindent;
				next;
			}
		
			my $task = extract_data_imghort( merge_hashes($params, $int) );
			LinkTask( $current_task, $task );
			RunTask( $task );
			log_unindent;

			
			#Проверяем не было ли ошибок при выводе
			my $hole = check_output_end($task->{output_file}, str2time( $int->{end_time} ) );
			my $error = parse_log( $task->{log_file});
			if( $error->{status} eq 'error' ){
				if( $error->{type} eq 'file'){
					my $file = $task->{params}->{layer_dir}.'/'.$error->{file};
					warn "Bad file: $file";
					if( -s "$file" ){
						my $sym = $file;
						$file = readlink( $file ) or abort("Bad link: $file");
						unlink $sym;
					}
					info "renaming file $error->{file}";
					rename $file, $file.'.BAD';
					my $file_id = db_select_one("select n_arc_file from e_arc_file where name = '$error->{file}'");
					db_delete_by_id('e_arc_file', $file_id);
					info "Recalculating intervals from $int->{begin_time} to $end_time";
					@intervals = get_layer_intervals($layer_id, $max_hole, $int->{begin_time}, $end_time);
					AppendOutput( $current_task, $task ) if($hole != -1);
					AppendLog( $current_task, $task );
					$c++;
					next;
				}else{
					abort "Unknown error: $error->{message}";
				}
			}
			if( $hole > $max_hole || $hole == -1){
				if( $hole == -1){
					abort("Bad output");
					#warn "Probably bad file, setting hole to whole interval";
					#$hole = str2time($int->{end_time}) - str2time( $int->{begin_time}) - 1;
				}
				warn "Hole at output, $hole sec. Recalculating intervals.";
				my $hole_start_time = time2ymd( str2time( $int->{end_time} ) - $hole);
				
				if( str2time( $hole_start_time ) < str2time( $int->{begin_time}) ){
					warn "hole start time: $hole_start_time < interval begin time: $int->{begin_time}";
					warn "setting hole to begin of interval";
					$hole_start_time = $int->{begin_time};
				}
				$hole_start_time =~ s/\./\-/g;
				my ($file_end_time, $file_id) = db_select_row("select end_time, n_arc_file from e_arc_file where n_arc_layer = $layer_id 
				and begin_time <= '$hole_start_time' and end_time > '$hole_start_time' ") 
					or abort("No file found for time '$hole_start_time' ");
				@intervals = get_layer_intervals($layer_id, $max_hole, $file_end_time, $end_time);
				info "Extraction starts from '$file_end_time' to '$end_time'" if($#intervals >= 0);
				
			}
			AppendOutput( $current_task, $task ) if($hole != -1);
			AppendLog( $current_task, $task );
			$c++;
		}
	});
	return $layer_task unless($run_task);
	return RunTask($layer_task);
}

sub extract_data_imghort($;$){
	my ($params, $run_task) = @_;
	my $task = CreateTask('ImGHort_', $params, sub{
		my ($curTask) = @_;
		prepare_files_for_exctract( $curTask );
		run_imghort( $curTask );
	});
	
	return $task unless($run_task);
	return RunTask($task);
}

sub prepare_task_params($){
	#NOTE: Фоiрмат даты в базе YYYY-MM-DD 
	#	   Формат даты в ini-файле ImGHort 	DD.MM.YYYY
	#	   Формат даты в входных переменных либо YYYY.MM.DD, либо DD.MM.YYYY
	#	   В начале входные даты приводятся к DD.MM.YYYY

	my ($params) = @_;
	$params = merge_hashes({
		step => 1,
		output_format => '%.6e'
	}, $params );
	my %params  = %{$params};

	#####begin_time, end_time
	my $begin_time = convert_date_to_nornal( $params{begin_time} );
	my $end_time = convert_date_to_nornal( $params{end_time} );
# 	$begin_time = make_time_correction($begin_time, -$params{time_correction});
# 	$end_time = make_time_correction($end_time, -$params{time_correction});
	
	validate_dates($begin_time, $end_time );
	
	my $db_begin_time = dmy2ymd( $begin_time );
	my $db_end_time = dmy2ymd( $end_time );

	my $layer_id = $params{layer_id};
	
	my $where;
	if( defined( $layer_id ) ){
		$where = " AND n_arc_layer = $layer_id";
	}else{
		if( defined( $params{layer_id_ms} ) ){
			$where = " AND layer_id = $params{layer_id_ms} ";
		}elsif( defined( $params{camp} ) ){
			my $camp_id = db_get_id_by_name('e_campaign', $params{camp});
			$where = " AND n_campaign = $camp_id";
		}else{
			abort('Cannot find layer. Set layer id or campaign')
		}
	}
	
# 	my ($layer_dir, 
 	my ($layer_dir, $seg_id, $camp_id);
	($layer_id, $layer_dir, $seg_id, $camp_id) = db_select_row("select n_arc_layer, dir, n_arc_seg, n_campaign from e_arc_layer where 
													  begin_time <= '$db_begin_time' and end_time >= '$db_end_time' $where ")
									   or abort('Layer not found');
	$layer_dir = rse( $arc_home.'/'.$layer_dir );
	my $index_dir = $layer_dir.'-NDX';
	$layer_dir = $layer_dir.'/';
	
	
	my $block_id = db_select_one("select n_block from e_campaign where n_campaign = $camp_id");
	$params->{block} = db_select_one("select name from e_block where n_block = $block_id")
		unless( $params->{block} );
	$params->{camp} = db_select_one("select name from e_campaign where n_campaign = $camp_id")
		unless( $params->{camp} );
	#### @vars
	abort('Empty vars') unless($params{'vars'});
	
	my $vars = validate_vars($params{'vars'}, $seg_id,);
#	my $units_str = gen_units_str(@$vars);
	

	######Узнать путь к ИДБ-файлам
	my $seg_dir = db_select_one_by_id('e_arc_seg', 'dir', $seg_id);
	$seg_dir = $arc_home.'/'.$seg_dir;
	my $idb_dir = rs($seg_dir.'/idb/');
		
	####step
	my $step = $params{step} * 1000; 
	
	$out_plugin = "ImGHAvgPlugin.so" if( $params{average_mode} );
	
	### Генрация входного idb-файла
	my $kks_dir = rs($seg_dir."/kks");
	vars_kks( $vars, $kks_dir );
# 	abort("Find 0 signals") if( $#kks_lines == -1);
	
	return merge_hashes($params, {
		layer_dir => $layer_dir,
		index_dir => $index_dir,
		begin_time => $begin_time,
		end_time => $end_time,
		idb_dir => $idb_dir,
		out_plugin => $out_plugin,
		step => $step,
		vars => $vars
#		units => $units_str
	});
}

sub prepare_files_for_exctract($){

	#      совместить %extract_params и %ini
	my $task = shift;
	my $params = $task->{params};
	
	###Подготовка входного idb-файла
	my $input_idb = rs( "$task->{dir}/signals.idb" );

	write_kks($params->{vars}, $input_idb);
	
	my $ini_file = $task->{dir}.'/'.$task->{name}.'.ini';

	#добавить лишнюю секунду к концу слоя
# 	my $end_time = make_time_correction($params->{end_time}, 0, 0, 1);
	
	my $node_type = basename( rse( $params->{layer_dir} ) );
#	info $params->{layer_dir}.' '.$node_type;
	$node_type =~ m/(\w+)_/;
	$node_type = $1;
#	info "node type: $node_type";
	if( $node_type eq 'oper'){
		$node_type = 1;
	}
	elsif( $node_type eq 'long'){
		$node_type = 2;
	}
	else{
		abort("Invalid arc type: $node_type");
	}
	
	
	#local
	template $script_path.'/templates/ImGHort.tpl.ini', $ini_file ,	(
		INPUT_IDB =>  template_env($input_idb) ,
		BEGIN_TIME => str2dmy( $params->{begin_time} ),
		END_TIME => str2dmy( $params->{end_time} ),
		LAYER_DIR => template_env($params->{layer_dir}),
		IDB_DIR => template_env($params->{idb_dir}),
		OUT_FILE => template_env($task->{output_file}),
		OUT_PLUGIN => basename($params->{out_plugin}),
		STEP => $params->{step},
		LOG_DIR => template_env( dirname($task->{log_file}) ),
		INDEX_DIR => $params->{index_dir},
		node_type => $node_type
	);
	my $stupid_log = dirname($task->{log_file}).'/'.$task->{name}.'.log'; 
	touch( $stupid_log );
	create_link( $stupid_log, $task->{log_file});
	copy $ini_file, $srv_home.'/ini/'.$task->{name}.'.ini';
# 	return ( $out_file );

}

sub var_kks($$){
	my ($var, $kks_dir) = @_;
	if( defined( $var->{kks} ) && ref( $var->{kks} ) eq 'ARRAY' ){#Если уже вручную указан kks для переменной
		my @kks = map { $_ = { kks=> $_, index1 => 1, index2 => 1} } @{$var->{kks}};
		$var->{kks} = \@kks;
		return;
	}
	
	my $var_name = $var->{'name'};
	my @index1 = @{$var->{'index1'}};
	my @index2 = @{$var->{'index2'}};
	
	my @res;
	my $lc_name = lc $var_name;
	#finding kks file

	my $kks_file;
	my @names = ( $var->{name}, $var->{altname} );
	@names = (@names,  @{$var->{altnames}}) if( defined( $var->{altnames} ));
	@names = (@names,  @{$var->{synonims}}) if( defined( $var->{synonims} ));
	
	if( $remote_server ){
		$kks_file = remote_run_res($remote_server, 
		'$HOME/skif/'.$hortman_name.' kksfile '.str_cut($kks_dir, $arc_home).' '.join(' ', @names));
	}else{
		for my $syn ( @names ){
			next unless($syn);
				if( -f "$kks_dir/$syn.kks" ){
				$kks_file = "$syn.kks";
				last;
			}
		}			
	}

	abort("Kks file '$kks_dir/$kks_file' doesnot exists for $var_name") unless( $kks_file );

	my %check_hash = ();
	for my $i1 (@index1){
		for my $i2 (@index2){
			$check_hash{$i1.';'.$i2} = 1;
		}
	}
	
	my @lines;
	
	if( $remote_server ){
		my $t = remote_run_res( $remote_server, '$HOME/skif/'.$hortman_name.' kks '.
			str_cut($kks_dir, $arc_home).'/'.$kks_file );
		@lines = split(m/\n/, $t);
	}else{
		my $fd = open_read("$kks_dir/$kks_file");
		(@lines) = <$fd>;
		close $fd;
	}
	
	abort("Empty kks file $kks_dir/$kks_file") unless(@lines);
	
	while($_ = shift @lines){
		abort("Wrong line in kks file: $kks_file") unless( m/(\S+)\s(\S+)\s(\S+)/ );
		my($kks, $i1, $i2) = ($1, $2, $3);
#		blah "$kks, $i1, $i2 ".join ' ', @index2;
		if( ( array_contains($i1+1, @index1 ) || !defined($var->{'index1'})) &&
			( array_contains($i2+1, @index2 ) || !defined($var->{'index2'}))){
			push @res, { kks => $kks, index1 => $i1, index2 => $i2};
			delete $check_hash{($i1+1).';'.($i2+1)}
		}
	}
	
	
	if( my @keys = keys( %check_hash ) ){
		my $message = "kks for var $var->{name} does not exists for current indexses: ".join(', ', @keys);
		if( $arc_svrk_vars{suppress_not_found_index_warnings} ){
			warn( $message );
		}else{
			abort( $message );
		}
	}
	
	
	$var->{kks} = \@res;
	return;
}

sub vars_kks($$){
	my ($vars, $kks_dir) = @_;
	for my $var (@$vars){
  		var_kks($var, $kks_dir );
	}
}

sub kks_str($){
	my ($kks_hash) = @_;
	return sprintf( $input_idb_format, $kks_hash->{kks}, $kks_hash->{index1}, $kks_hash->{index2} );
}

sub validate_vars($$){
	my ($vars, $seg_id, ) = @_;
	my @res;
	my $c = 0;
	for( @$vars ){
		my $var;
		if( ref($_) eq 'HASH'){
			my %h = %{$_};
			$var = \%h;
			$var->{index1} = $var->{index} unless $var->{index1} ;
			my @i1 = index_str2array($var->{index1});
			my @i2 = index_str2array($var->{index2});

			$var->{index1} = \@i1;
			$var->{index2} = \@i2;
		}else{
			$var = parse_param_str( $_ );
		}
		
		if( $var->{kks} ){
			unless( ref( $var->{kks} ) ){
				$var->{kks} = [ $var->{kks} ];
			}
		}else{
			#узнать размерности переменной
			my ($layers, $cols, $rows, $var_id, $units, $dim) = var_info($var, $seg_id);
#			($var->{index1}, $var->{index2} ) = ($var->{index2}, $var->{index1}) if($dim == 2);
			( $cols, $rows ) = ( $rows, $cols ) if($dim == 1);
			
			my $max_index1 = max( @{$var->{index1}} );
			my $max_index2 = max( @{$var->{index2}} );
	
	
			my ($max_cols, $max_rows);
#			$max_rows = $max_index2 if(defined($max_index2));
#			$max_cols = $max_index1 if(defined($max_index1));
			$max_rows = $max_index1 if(defined($max_index1));
			$max_cols = $max_index2 if(defined($max_index2));

			my $var_name = "$var->{name}($rows x $cols)";
	
			#Проверить заданы ли все индексы
			abort("Не заданы все индексы для $var_name") if($cols > 1 && !defined($max_cols));
			abort("Не заданы все индексы для $var_name") if($rows > 1 && !defined($max_rows));
	
			#Проверить не обращаются ли к одномерному параметру как двумерному
			abort("Не правильно заданы индексы для $var_name") if(defined($max_cols) && $cols == 1);
	
			#Проверить не выходят ли индексы за границы
			abort("Выход за пределы массива для $var_name") if( $max_cols > $cols && !$arc_svrk_vars{suppress_not_found_index_warnings});
			abort("Выход за пределы массива для $var_name") if( $max_rows > $rows && !$arc_svrk_vars{suppress_not_found_index_warnings});
			#abort("max cols ($max_cols) > cols ($cols) in $var_name") if( $max_cols > $cols );
			#abort("max rows ($max_rows) > cols ($rows) in $var_name") if( $max_rows > $rows );
	
			$var->{index1} = [ 1 ] unless( defined($max_index1) ); 
			$var->{index2} = [ 1 ] unless( defined($max_index2) );
			
			$var->{units} = $units unless(defined( $var->{units} ) && !$units); 
			$var->{dim} = $dim;
		}
		push @res, $var;
	}
	return \@res;
}

sub write_kks($$){
	my ($vars, $idb_file) = @_;
	my $fd = open_write($idb_file);
	print $fd "FileTable,3,08.06.2006 15:49:15.000;\n";
  	print $fd "ShortName,char,$kks_field_length;Index1,number,14;Index2,number,14;\n";
#   print $idb_fd "ShortName,char,$kks_field_length;\n";
  	print $fd sprintf($input_idb_format, 'ShortName','Index1','Index2');
	for my $var (@$vars){
		for my $kks_hash (@{$var->{kks}}){
			print $fd kks_str( $kks_hash );
		}
	}
	close($fd);
}

sub parse_param_str($){
	my ($param_str) = @_;
	
	my $res = {};
	abort("Wrong param notation: '$param_str'") unless($param_str =~m/([\w\|]+)(?:\(([^\;]+)(?:\;([^\;]+)|)\)|)/);
#	$param_str =~ m/([\w\|]+)(?:\([^\)]+\)|)/;
#	my ($param_name, $index_strs) = ($1, $2);
#	my @synonims = split m/\|/, $param_name;
#	$param_name =~ s/\|\w+//;
#	
#	my (@inds1, @inds2);
#	
#	for my $index_str (split m/\s+/, $index_strs){
#		$index_str =~ m/([^\;]+)(?:\;([^\;]+)|)/;
#		my ($index_str1, $index_str2) = ($1, $2);
#	}
	my ($param_name, $index_str1, $index_str2) = ($1, $2, $3);
	
	my @synonims = split m/\|/, $param_name;
	$param_name =~ s/\|\w+//;
	abort("Wrong param notation") if( !defined($index_str1) && !defined($index_str2) && $param_str =~ m/\;/ );
	#abort( $index_str1.';'. $index_str2);
	$res->{name} = $param_name;
	
	my (@inds1, @inds2);
	
	if(defined($index_str1)){
		@inds1 = index_str2array($index_str1);
		$res->{index1} = \@inds1;
	}
 	
	
	if(defined($index_str2)){
		@inds2 = index_str2array($index_str2);
		$res->{index2} = \@inds2;
	}

	$res->{synonims} = \@synonims;
	return $res;
}

sub check_output_end($$){#return hole size or -1 if incorrect file
	my ($file, $task_end_time) = @_;
	my $line = read_last_line( $file );
	my @values = split m/\s+/, $line;
	
	my $file_end_time = shift @values;
	unless($file_end_time =~ m/\d+/){
		blah "  file_end_time : $file_end_time for file $file not a number";
		return -1;
	}
	unless($file_end_time){
		blah "  file_end_time : $file_end_time for file $file null";
		return -1;
	}
	if( $file_end_time = $task_end_time  + 1 ){
		warn "Output end time = task end time + 1";
		return 0;
	}
	if( $file_end_time > $task_end_time  + 1 ){
		warn "Output end time > task end time";
		return -1;
	}
#	print "$task_end_time\n";
#	return -1 unless( $file_end_time =~ m/\d+/ && $file_end_time);
	return $task_end_time - $file_end_time;
	
}

sub layer_intervals($$;$$){
	my ($layer_id, $max_hole_length, $begin_time, $end_time) = @_;
	my @res;
	my ($length, $hole_start, $hole_end);
	unless( defined($begin_time)){
		($begin_time, $end_time) = db_select_row("select begin_time, end_time from e_arc_layer where n_arc_layer = $layer_id");
	}
	$begin_time = convert_date_to_nornal_ymd($begin_time,'-');
	$end_time = convert_date_to_nornal_ymd($end_time,'-');
	
	return () if( $begin_time gt $end_time );
	
	my ($min_btime, $max_etime) = db_select_row("select min( begin_time ), max( end_time ) from e_arc_file where n_arc_layer = $layer_id 
			and end_time > '$begin_time' and begin_time < '$end_time'" );

	$begin_time = $min_btime if( $begin_time le $min_btime );
	$end_time = $max_etime if( $end_time gt $max_etime );
	
	my ($int_b, $int_e) = ($begin_time, undef);
	
		
	my $all_ref = db_select_all( "select begin_time, end_time from e_arc_file where n_arc_layer = $layer_id 
			and end_time > '$begin_time' and begin_time < '$end_time'
			order by begin_time" );
	my @all = @$all_ref;
	if( $#all ==- 1){
		return ();
	}
	my ($file_b, $file_e);
	for( my $i = 0; $i < $#all && $#all; $i++){
		my ($file_b, $file_e) = @{$all[$i]};
		my ($next_file_b) = @{$all[$i+1]};
		$int_b = $file_b unless( defined( $int_b ));
		if( ymd2time( $next_file_b ) - ymd2time( $file_e ) > $max_hole_length){
			$int_e = $file_e;
			if( str2time($int_e) - str2time($int_b) >= 1 ){
				push @res, { begin_time => convert_date_to_nornal_ymd($int_b),
							 end_time => convert_date_to_nornal_ymd($int_e)};
				
			}
			$int_b = undef;
		}
	}
	$int_b = $all[$#all]->[0] unless( defined( $int_b ) );
	$int_e = $end_time;#$all[$#all]->[1];
	push @res, { begin_time => convert_date_to_nornal_ymd($int_b),
				 end_time => convert_date_to_nornal_ymd($int_e)};
	
	return @res;
}

sub format_print_param_str($$){
	my ($out_fd, $var) = @_;
	my $res = '';
	if( $var->{dim} == 0){
		$res .= sprintf("%-12s\t",$var->{name});
        #$res .= sprintf("Dost_%-9s\t",$var->{name});# if($var->{dost});
	}else{
		for my $kks (@{$var->{kks}}){
			my ($i1, $i2) = ($kks->{index1} + 1, $kks->{index2} + 1 );
			if( $var->{dim} == 1 ){
				$res .= sprintf("%-12s\t","$var->{name}($i1)");
			}else{
				$res .= sprintf("%-12s\t","$var->{name}($i1;$i2)");
			}
		}
	}
	return $res;
}

sub format_output_print_header_param_names($@){
	my ($out_fd, @params_descr) = @_;
	print $out_fd sprintf("%-19s\t","# Time");
	for my $param (@params_descr){
		#for my $kks (@{$params->{kks}}){
		#	print $out_fd format_print_param_str($out_fd, $param);
		#}
#		if( $param->{length} > 1){
#			for( my $c = 1; $c<= $param->{length}; $c++){
#			print $out_fd sprintf("%-19s\t","$param->{name}($c)");
#			}
#		}else{
			print $out_fd format_print_param_str($out_fd, $param);
#		}
	}
	print $out_fd "\n";
}

sub format_output_print_header_param_names_dost($@){
    my ($out_fd, @vars ) = @_;
    my $res = sprintf("%-19s\t","Time");
    for my $var (@vars){
        $res .= sprintf("%-12s\t",$var->{name});
        #$res .= sprintf("%-12s\t",'Dost_'.$var->{name});
    }
    print $out_fd $res."\n";
}

sub format_output_print_header_param_names_avg($$@){
	my ($out_fd, $graph_format, @params_descr) = @_;
	my ($prev_line, $first_line, $kks_line, $units_line, $d);
	
	if( $graph_format ){
			$d = '-';
	}else{
		$d = '';
	}
	#30YQR02FX902XQ01
	$units_line = sprintf("%-19s\t%-19s\t%s\t","", "", "");
	$kks_line = sprintf("%-19s\t%-19s\t%s\t","", "", "");
	$prev_line = sprintf("%-19s\t%-19s\t%s\t","", "", "");
	$first_line = sprintf("%-19s\t%-19s\t%s\t","Start Time", "End time", "Hole");
	my $space_distance = sprintf('%-8s','');

	for my $param (@params_descr){
#		if( $param->{length} > 1){
		for( my $c = 1; $c<= $param->{length}; $c++){
			my $kks = $param->{kks}->[$c-1]->{kks};
			my $name = $param->{name} || $d;
			my $avg_type = $param->{avg_type} || $d;
			my $units = $param->{units} || $d;
			$units =~ s/\s/_/g if( $graph_format );
			( $kks ) = split m/\s*\|/, $kks;
			unless(  $param->{avg_type} ){
				$first_line .= "Avg_$name\tDost_$name\t";
			}else{
				$first_line .= "Min_$name\tMax_$name\tAvg_$name\tDost_$name\t";
			}
#			$first_line .=  "Min_$param->{name}($c)\tMax_$param->{name}($c)\tAvg_$param->{name}($c)\tDost_$param->{name}($c)\t";
			if( $param->{avg_type} ){
				$prev_line .= sprintf("%-8s\t%-8s\t%-8s\t%-8s\t",$d, $d, $kks ,$d);
				$units_line .= sprintf("%-8s\t%-8s\t%-8s\t%-8s\t",$avg_type, $avg_type, $units ,$d);
			}else{
				$prev_line .= sprintf("%-8s\t%-8s\t",$kks,$d);
				$units_line.= sprintf("%-8s\t%-8s\t",$units,$d);
			}
			
		}
	}


	print $out_fd $prev_line."\n";
	print $out_fd $units_line."\n";
	print $out_fd $first_line."\n";
}

sub format_output_print_header($%){
	my ($out_fd, %task_params) = @_;
	

	my $begin_time = $task_params{begin_time};

	$begin_time = convert_date_to_nornal_ymd($begin_time);
	my $end_time = $task_params{end_time};
	$end_time = convert_date_to_nornal_ymd($end_time);

		
	$begin_time = make_time_correction_ymd($begin_time, $task_params{time_correction});
	$end_time = make_time_correction_ymd($end_time, $task_params{time_correction});
	my $step = $task_params{step} / 1000;
	my $camp = $task_params{campaign};
	$camp = $task_params{camp} unless $camp;

	#Пишем заголовок
	print $out_fd "# Block     : $task_params{block}\n";
	print $out_fd "# Campaign  : $camp\n";
	print $out_fd "# Step      : $step sec.\n";
	print $out_fd "# Step out  : $task_params{step_out} sec.\n" if($task_params{step_out});
	print $out_fd "# Begin time: $begin_time\n";
	print $out_fd "# End time  : $end_time\n";
	print $out_fd "# Format    : $task_params{output_format}\n" if($task_params{print_default_format});
	
	
	if($task_params{vars}){
		my @vars_str;
		my @units_str;
		for my $var (@{$task_params{vars}}){
			push @vars_str,$var->{name};
			push @units_str, $var->{units};
		}
		my $vars_str = join ', ', @vars_str;
		my $units_str =join ', ', @units_str;
		print $out_fd "# Params    : $vars_str\n";
		print $out_fd "# Units     : $units_str\n";
	}

	print $out_fd "# Locale    : $task_params{time_correction}\n" if($task_params{time_correction});
	print $out_fd "\n";
	
	if( $task_params{legends} ){
		for my $legend (@{$task_params{legends}}){
			print $out_fd sprintf("# %-10s: %s\n", $legend->{name}, $legend->{descr});
		}
		print $out_fd "\n";
	}
}

sub vars_length($){
	my $vars = shift;
	for my $var (@$vars){
		$var->{length} = @{$var->{kks}};
	}
}

sub format_output_def($%){
	return format_output(@_);
}

#sub merge_duplicate_vars($){
#	my ($vars_ref) = @_;
#	my @res = ();
#	for my $var (@{$vars_ref}){
#		unless(@res){
#			push @res, $var;
#			next;
#		}
#		
#		for my $rvar (@res){
#			if ($var->{name} eq $rvar->{name}){
#				my @kks = ( @{$rvar->{kks}}, 
#				$rvar->{kks} = \@kks;
#			}
#		}	
#	}
#	
#	return @res; 
#}

sub format_output($$){
	#TODO: на проверке ошибок на выводе, учитывать локаль времени
	#TODO: Иправить ошибки с размерностями параметров (m_unit)
	my ($out_file, $params) = @_;
	
	my $in_fd = open_read($out_file);
	my $block_id = db_get_id_by_name('e_block', $params->{block})
	 or abort("Wrong block name '$params->{block}'");

	my $posDir = $params->{idb_dir}.'/../pos/';
	#abort( dump_hash_ref( shift @params_descr ) );
	my $locale = $params->{time_correction};
	my $format = $params->{output_format};
	vars_length( $params->{vars});
	replace_indexes( $params->{vars}, $posDir);
#	merge_duplicate_vars( $params->{vars});
	my @out_fds;
	my @out_format_files;
	my $graph_format = $params->{graph_format};
	my $print_reliability = $params->{print_reliability};
	
	if( $params->{merge_params} ){
			my $file_name = $out_file;
			$file_name =~ s/[\d\w]+\.txt$/output.merge.txt/;
			my $out_fd = open_write($file_name,1);
			format_output_print_header($out_fd, %{$params}) unless($params->{no_header});
			if(  $params->{average_mode} ){
				format_output_print_header_param_names_avg($out_fd, $graph_format, @{$params->{vars}}) unless($params->{no_header});
			}elsif( $params->{time_progress} ){
				format_output_print_header_param_names($out_fd, @{$params->{vars}} ) unless($params->{no_header});
			}
			push @out_fds, $out_fd; 
			push @out_format_files, $file_name;
	}else{
		#Создаем файлы для каждого из заданых параметров
		for my $param (@{$params->{vars}}){
			my $file_name = $out_file;
			$file_name =~ s/[\d\w]+\.txt$/$param->{name}.txt/;

			my $out_fd = open_write($file_name,1);
	
			format_output_print_header($out_fd, %{$params}) unless($params->{no_header});
			if( $params->{time_progress} ){
#				format_output_print_header_param_names($out_fd, @{$params->{vars}} ) unless($params->{no_header});
				format_output_print_header_param_names($out_fd, $param) unless($params->{no_header});
			}else{
				print $out_fd "# Param: $param->{name}\n";
			}
			

			#print $out_fd "\n";
			push @out_fds, $out_fd; 
			push @out_format_files, $file_name;
		
		}
	}
	
	
	my ($time_point, $end_point, @values);
	#	$end_point ;#= dmy2time($task_params{begin_time});
	my $prev_time;
	while(<$in_fd>){
		if( m/err\:\s*(.*)/ ){
			warn "Error line in output file: $1";
			next;
		}

		($time_point, @values) = split m/\s+/;
		my $tp = $time_point;
		$time_point -= $params->{time_correction} * 60 * 60;
		$time_point = time2str("%Y-%m-%d %H:%M:%S", $time_point);

		
		if( $params->{average_mode} ){
			my $time_point2 = shift @values;
			my $tp2 = $time_point2;
			next if( is_array_filled_nulls(@values ));

			$time_point2 -= $params->{time_correction} * 60 * 60;
			$time_point2 = time2str("%Y-%m-%d %H:%M:%S", $time_point2);

			#holes

			$prev_time = $tp2 - 1 unless( defined($prev_time )) ;			
			my $hole = $tp2 - $prev_time;

			
			$prev_time = $tp;
			if( $params->{field} ){
				$time_point = "$time_point2\t$time_point\tHole($hole)\n";	
				format_print_values_average_mode_field( \@out_fds, $params->{vars}, $time_point, $format, @values);
			}else{
				$time_point = "$time_point2\t$time_point\t$hole";
				format_print_values_average_mode( \@out_fds, $params->{vars}, $time_point, $format, @values);
			}
			
		}elsif( $params->{kks_mode} ){
			format_print_values_kks( \@out_fds, $params->{vars}, $time_point, $format,$print_reliability, @values);
		}elsif( $params->{time_progress} ){
			format_print_values_time_progress( \@out_fds, $params->{vars}, $time_point, $format,$print_reliability, @values);
		}else{
			format_print_values( \@out_fds, $params->{vars}, $time_point, $format, $print_reliability, @values);
		}
	}
	
	map {close $_ } @out_fds;
	close $in_fd;
	
	if( wantarray ){
		return @out_format_files;
	}else{
		return shift @out_format_files;
	}
}


sub format_print_values($$$$$@){
	my ($out_fds_ref, $vars, $time_point, $format, $print_reliability, @values) = @_;
	
	my @out_fds = @$out_fds_ref;
	
	my $c = 0;
	my $fc = 0;
	my $out_fd;
	for my $var (@$vars){
		$out_fd = $#out_fds ? $out_fds[$fc++] : $out_fds[0];
		#print $out_fd "# Param: $var->{name}";
		print $out_fd "# Time point: $time_point\n";
		
		for(my $i = 0; $i < $var->{length}; $i++){
#			abort("Error in reading input idb") unless($line);
			my $k = $var->{kks}->[$i];
			my ($kks, $i1, $i2) = ( $k->{kks}, $k->{index1}, $k->{index2} );
			
			$i1++; $i2++;
			my $value = $values[$c++];
			my $dost = $values[$c++];
			$dost = sprintf("%-8s", $dost);
			
			$value =~ s/\,/./g;
			$value = sprintf($format, $value);
			
			$value = "$value\t$dost" if( $print_reliability );
			$k->{value} = $value;
		}
		
		my @kks =  @{$var->{kks}};
		
		if( ArcVar('sort_indexes') ){
			@kks = sort { $a->{index2} <=> $b->{index2} or $a->{index1} <=> $b->{index1} } @{$var->{kks}};
		}
		
		$var->{kks} = \@kks;
		for(my $i = 0; $i < $var->{length}; $i++){
            my $k = $var->{kks}->[$i];
			my ($kks, $i1, $i2, $value) = ( $k->{kks}, $k->{index1}, $k->{index2}, $k->{value} );
			$i1++; $i2++;
			if($var->{dim} == 0){
				print $out_fd "\t$value\n";
			}elsif($var->{dim} == 1){
				print $out_fd "$i1\t$value\n";
			}elsif($var->{dim} == 2){
				print $out_fd "$i2\t$i1\t$value\n";
			}
		}
	}
}



sub format_print_values_time_progress($$$$$@){
	my ($out_fds_ref, $vars, $time_point, $format, $print_reliability, @values) = @_;
	
	my @out_fds = @$out_fds_ref;
	
	my $c = 0;
	my $fc = 0;
	my $out_fd;
	
	for my $out_fd (@out_fds ){
		print $out_fd "$time_point\t";
	}
	
	for my $var (@$vars){
		$out_fd = $#out_fds ? $out_fds[$fc++] : $out_fds[0];
		
		for(my $i = 0; $i < $var->{length}; $i++){
			my $k = $var->{kks}->[$i];
			my ($kks, $i1, $i2) = ( $k->{kks}, $k->{index1}, $k->{index2});
			$i1++; $i2++;
			my $value = $values[$c++];
			my $dost = $values[$c++];
			$dost = sprintf("%-8s", $dost);
			$value =~ s/\,/./g;
			$value = sprintf($format, $value);
			$value = "$value\t$dost" if( $print_reliability );
			print $out_fd "$value\t";
			#print $out_fd "$dost\t";
		}
	}
	for my $out_fd (@out_fds ){
		print $out_fd "\n";
	}
}

sub format_print_values_kks($$$$$@){
	my ($out_fds_ref, $vars, $time_point, $format, $print_reliability, @values) = @_;
	
	my @out_fds = @$out_fds_ref;
	
	my $c = 0;
	my $fc = 0;
	my $out_fd;
	
	for my $out_fd (@out_fds ){
		print $out_fd "$time_point\n";
	}
	
	for my $var (@$vars){
		$out_fd = $#out_fds ? $out_fds[$fc++] : $out_fds[0];
		
		my $kks = $var->{kks}->[0];
		$kks = sprintf('%-10s', $kks->{kks});
		my $value = $values[$c++];
		my $dost = $values[$c++];
		$dost = sprintf("%-8s", $dost);
		$value =~ s/\,/./g;
		$value = sprintf($format, $value);
		$value = "$value\t$dost" if( $print_reliability );
		print $out_fd "$kks\t$value\n";
	}
	
	for my $out_fd (@out_fds ){
		print $out_fd "\n";
	}
}

sub format_print_values_average_mode_field($$$$@){
	my ($out_fds_ref, $vars, $time_point, $format_def, @values) = @_;
	
	my @out_fds = @$out_fds_ref;
	my $c = 0;
	my $fdc = 0;
	my $out_fd;
	my $buff;
#	my $first = 1;	
	for my $var (@$vars){
		$out_fd = @out_fds ? $out_fds[$fdc++] : $out_fds[0];
		$buff = '';
		my @kks = @{$var->{kks}};
		my ($format, $kind) = ($var->{format},$var->{avg_type});
		$format = $format_def unless( $format );
#		$buff .= "# Param: $var->{name}\n";
		my $name = sprintf("%-9s", $var->{name});
		unless(  $var->{avg_type} ){
			$buff .= "i1\ti2\tAvg_$name\tDost_$name\n";
		}else{
			$buff .= "i1\ti2\tMin_$name\tMax_$name\tAvg_$name\tDost_$name\n";
		}
		for my $i2 (@{$var->{index2}}){
			for my $i1 (@{$var->{index1}}){
				my $min = $values[$c++];#sprintf( "%-8s", sprintf($format, $values[$c++]));
				my $max = $values[$c++];#sprintf( "%-8s", sprintf($format, $values[$c++]));
				my $avg = $values[$c++];
				my $kks = shift @kks;
				my ($k, $n1, $n2 ) = split ($kks->{kks}, $kks->{index1}, $kks->{index2});
				$min =~ s/\,/./g;
				$max =~ s/\,/./g;
				$avg =~ s/\,/./g;
			
				($min, $max) = (0,0) unless( $avg != 0);
				if( $kind eq 'n' ){
					$min = sprintf( "%-8s", sprintf($format, $min));
					$max = sprintf( "%-8s", sprintf($format, $max));
				}elsif( $kind eq 'd' ){
					if($avg != 0){
						$min = ($avg - $min) / $avg;
						$max = ($max - $avg) / $avg;
					}
					$min = sprintf( "%-8s", sprintf($format, $min ));
					$max = sprintf( "%-8s", sprintf($format, $max ));

				}elsif( $kind eq 'd%' ){
					if($avg != 0){
						$min = ($avg - $min) / $avg * 100;
						$max = ($max - $avg) / $avg * 100;
					}
					$min = sprintf( "%-8s", sprintf("%04.1f", $min ));
					$max = sprintf( "%-8s", sprintf("%04.1f", $max ));
				}
			
				$avg = sprintf( "%-8s", sprintf($format, $avg));
				my $dostovernost = $values[$c++];
	#			$atLeastOneGood = 1 if( $dostovernost != 0 );
				$dostovernost = sprintf('%-12.2f', $dostovernost);
				$buff .= "$i1\t$i2\t";
#				$buff .= "$n1\t$n2\t";
				unless( $kind ){
					$buff.="$avg\t$dostovernost\t";
				}else{
					$buff.="$min\t$max\t$avg\t$dostovernost\t";
				}
			$buff .= "\n";
			}
			
		}
		print $out_fd "$time_point\n".$buff."\n";
	}

}

sub format_print_values_average_mode($$$$@){
	my ($out_fds_ref, $vars, $time_point, $format_def, @values) = @_;
	
	my @out_fds = @$out_fds_ref;
	
	my $c = 0;
	my $out_fd = shift @out_fds;
	my $buff;
#	my $first = 1;	
	for my $var (@$vars){

		my ($format, $kind) = ($var->{format},$var->{avg_type});
		$format = $format_def unless( $format );
		
		for(my $i = 0; $i < $var->{length}; $i++){
		
			my $min = $values[$c++];#sprintf( "%-8s", sprintf($format, $values[$c++]));
			my $max = $values[$c++];#sprintf( "%-8s", sprintf($format, $values[$c++]));
			my $avg = $values[$c++];
			$min =~ s/\,/./g;
			$max =~ s/\,/./g;
			$avg =~ s/\,/./g;
			
			($min, $max) = (0,0) unless( $avg != 0);
			if( $kind eq 'n' ){
				$min = sprintf( "%-8s", sprintf($format, $min));
				$max = sprintf( "%-8s", sprintf($format, $max));
			}elsif( $kind eq 'd' ){
				if($avg != 0){
					$min = ($avg - $min) / $avg;
					$max = ($max - $avg) / $avg;
				}
				$min = sprintf( "%-8s", sprintf($format, $min ));
				$max = sprintf( "%-8s", sprintf($format, $max ));

			}elsif( $kind eq 'd%' ){
				if($avg != 0){
					$min = ($avg - $min) / $avg * 100;
					$max = ($max - $avg) / $avg * 100;
				}
				$min = sprintf( "%-8s", sprintf("%04.1f", $min ));
				$max = sprintf( "%-8s", sprintf("%04.1f", $max ));
			}
			
			$avg = sprintf( "%-8s", sprintf($format, $avg));
			my $dostovernost = $values[$c++];
#			$atLeastOneGood = 1 if( $dostovernost != 0 );
			$dostovernost = sprintf('%-12.2f', $dostovernost);
			
			unless( $kind ){
				$buff.="$avg\t$dostovernost\t";
			}else{
				$buff.="$min\t$max\t$avg\t$dostovernost\t";
			}
		}
	}
	print $out_fd "$time_point\t".$buff."\n";
}

sub replace_indexes($$){
	my ($vars, $posDir) = @_;
	for my $var (@$vars){
		if( $var->{pos_var} ){
			my $indexes = read_pos_var("$posDir/$var->{pos_var}.pos");
			replace_index($var, $indexes);
		}
	}
}

sub read_pos_var($){
	my $pos_file = shift;
	my $res = {};
	my $fd = open_read( $pos_file );
	while(<$fd>){
		my ($tvs, $i) = split m/\s+/;
		$res->{$i} = $tvs if($i);
	}
	close $fd;
	return $res;
}

sub replace_index($$){
	my ($var, $indexes) = @_;
	my $index_name = $var->{dim} != 2 ? 'index1' : 'index2';
	for my $kks (@{$var->{kks}}){
		my $i = $kks->{$index_name} + 1;
		$kks->{$index_name} = $indexes->{$i} - 1 if( defined( $indexes->{$i} ) );	
	}

	return $var;
}

return 1;

END {}




