package TaskMan;
use strict;
    
BEGIN {
 	use File::Basename;
	use Time::HiRes qw( usleep );
	use File::Temp qw/ tempfile tempdir /;
	use File::Path;
	use Exporter();
	use FindBin;
	use lib "$FindBin::Bin/";
	use ErrorHandler;
	use Loggy;
	use Misc;

	
    @TaskMan::ISA = qw(Exporter);
    @TaskMan::EXPORT = qw( &taskman_init &run_task &get_tasks &kill_task &kill_task_by_name &task_alive &create_task
    $task_dir $pid_dir $check_interval &CreateTask &RunTask &AppendOutput &LinkTask &MakeTask AppendLog ReadTask);
    					
	our $script_path = $FindBin::Bin;
		
}

my $pid_dir = '/tmp/pid';
my $task_dir = '/tmp/';
my $check_interval = 500_000; #microseconds
my $running_tasks_dir = $task_dir.'/running_tasks/';

sub taskman_init(%){
# 	info "taskman_init ".join ' ', @_;
	my (%params) = @_;
	$task_dir = $params{task_dir};
	$running_tasks_dir = $params{running_tasks_dir};
	mkdir( $running_tasks_dir ) unless( -d $running_tasks_dir );
	
	$pid_dir = $params{pid_dir};
	$check_interval = $params{check_interval};
}

sub task_alive($){
	my ($name) = @_;
	return -f "$pid_dir/$name.pid";
}

sub create_task($){
	my $name_template = shift;
	my $tempdir = tempdir ( $name_template, DIR => $task_dir );
	chmod( 0755,$tempdir );
	return  $tempdir ;
}

sub MakeTask($$;$){
	my ( $dir, $params, $func ) = @_;
	return {
		name => basename($dir),
		dir => $dir,
		params => $params,
		func => $func,
		output_file => $dir.'/output.txt',
		log_file => $dir.'/log.txt',
		tasks_dir => $dir.'/tasks/'
	};
}

sub CreateTask($$&;$){
	my ($task_name_base, $params, $func, $base_dir) = @_;
	$base_dir ||= $task_dir;
	my $tempdir = tempdir ( $task_name_base.'XXXX', DIR => $base_dir );
	chmod( 0755,$tempdir );
	mkpath( $tempdir.'/tasks/' );
	return MakeTask($tempdir, $params, $func );
}

sub AppendLog($$){
	my ($task, $sub_task) = @_;
	if( -f $sub_task->{log_file} ){
		cat_to( $sub_task->{log_file}, $task->{log_file} );
	}
}

sub ReadTask($){
	my $task_dir = shift;
	my $params = read_json( $task_dir.'/task.json' );
	return MakeTask( $task_dir, $params );
}

sub LinkTask($$){#Добавляет сиволическую ссылку на директорию задачи $sub_task в директорию задачи $task
	my ($task, $sub_task) = @_;
	my $count = files_count( $task->{tasks_dir} );
	$count++;
	create_link($sub_task->{dir}, "$task->{tasks_dir}/$count.$sub_task->{name}");
}

sub AppendOutput($$){#Добавляет вывод задачи $sub_task к основному выводу задачи $task
	my ($task, $sub_task) = @_;
	if( -f $sub_task->{output_file} ){
		cat_to( $sub_task->{output_file},  $task->{output_file} );
	}
}

sub RunTask($){
	my ($task) = @_; 

	my $task_dir = $task->{dir};
	my $pid_file = "$task_dir/running";
	my $stdout_file = "$task_dir/stdout";
	my $stderr_file = "$task_dir/stderr";
	
		
	$task->{params}->{stdout} = $stdout_file;
	$task->{params}->{stderr} = $stderr_file;
		
	write_json( "$task->{dir}/task.json", $task->{params} );
	my $fd = open_write($task->{output_file});
	close $fd;
	my $options = $task->{params};
#	run_task( sub{
	info "Running task $task->{name} ( $task->{dir} )";
#		$task->{func}->($task);
	my $task_name = $task->{name};
				
	if( $task->{params}->{close_stdout} ){
	
		close STDOUT;
		close STDERR;
		
		open STDOUT, '>', $stdout_file or die("Cannot write '$stdout_file'");
		open STDERR, '>', $stderr_file or die("Cannot write '$stderr_file'");
		$task->{params}->{close_stdout} = undef; 
	}
	my $pidfd = open_write( $pid_file );
	print $pidfd $$;
	close $pid_file;		
	
	create_link( $task->{dir}, $running_tasks_dir.'/'.$task->{name} );
			
	eval{ $task->{func}->( $task ) };

	
	unlink( $pid_file ) or abort('cannot unlink $pid_file');
	unlink( $running_tasks_dir.'/'.$task->{name} );
#		unless( $@ ){# if good eval
#			unlink( $stdout_file ) if( -f $stdout_file );
#			unlink( $stderr_file ) if( -f $stderr_file );
#		}
		
#		if( $task->{sub_tasks} && ref( $task->{sub_tasks} ) eq 'ARRAY'){
#			for my $sub_task (@{$task->{sub_tasks}}){
#				RunTask($sub_task, $options);
#			}
#		}
#	}, $task->{name}, $options->{detach}, $options->{close_stdout} );
	return $task;
}


sub run_task(&$;$$){
	my ($task_sub, $task_name, $detach, $close_stdout) = @_;

	$detach = 0 unless( defined($detach) );
	unless(defined( $close_stdout )){
		if( $detach ){
			$close_stdout = 1;
		}else{
			$close_stdout = 0;
		}
	}
	
	my $pid_file = "$pid_dir/$task_name.pid";
	my $stdout_file = "$pid_dir/$task_name.stdout";
	my $stderr_file = "$pid_dir/$task_name.stderr";

	
	#my $pid = fork;
#	abort("Error runnig process") unless( defined($pid));
#	if($pid != 0){#parent
#		my $fd = open_write($pid_file);
#		print $fd $pid;
#		close $fd;
#		my $exit_status ;
#		unless($detach){
#			#wait untill finished
#			abort("wait error, no child process") if( waitpid($pid, 0) == -1 );
#			$exit_status = $?;
#			abort( "Error in running task '$task_name'\nexit code $?\nSTDOUT: ".cat($stdout_file)."\nSTDERR: ".cat($stderr_file) ) if($exit_status);
#			return ( $exit_status, $stdout_file, $stderr_file);
#		}
#	}else{#child
#		close STDOUT;
#		close STDERR;
#		unless($close_stdout){
#			open STDOUT, '>', $stdout_file or die("Cannot write '$stdout_file'");
#			open STDERR, '>', $stderr_file or die("Cannot write '$stderr_file'");
#		}
#		eval{ $task_sub->() };
#		unlink( $pid_file ) or abort("Cannot remove pid file '$pid_file'");
#		unlink( $stdout_file ) or abort("Cannot remove '$stdout_file'");
#		unlink( $stderr_file ) or abort("Cannot remove '$stderr_file'");
#
#		exit $?;
#	}
	
	return;

	
}

sub get_tasks(;$){
	my ($task_class) = @_;
	
	$task_class = '*' unless( defined( $task_class ) );
	my @pid_list = glob("$task_dir/$task_class*.pid");
	my @res;
	for my $pid_file (@pid_list){
		my $pid = cat( $pid_file );
		my  $name = $pid_file; $name  =~ s/\.pid$//; $name  =~ m/([^\/]+)$/; $name = $1;
		push @res, { name => $name, pid => $pid};
	}
	return @res;
}

sub kill_task($){
	my ($pid) = @_;
	system_run("kill -9 $pid");
}
sub kill_task_by_name($){
	my ($name) = @_;
	my $pid_file = "$pid_dir/$name.pid";
	my $pid = cat( $pid_file );
	kill_task($pid);
}

return 1;

END {}

