#!/usr/bin/perl -s
#Скрип для перевода местного времени в гринвическое для файла паспортизации
use strict;
use FindBin;
use lib "$FindBin::Bin/";
#use Misc;
#use Loggy;

my @files = @ARGV;
for my $file (@files){
	eval{
		toGmTime(  $file );
	};
	
	warn "Error: ".$@ if $@ ;
}


sub toGmTime($){
	my $file = shift;
	
	print "Processing file $file...\n";
	
	my $fd = open_read($file);
	my $fd_out = open_write($file.".gm");
	
	my $header_ended = 0;
	while(<$fd>){
		if( m/^\d/ ){#Dates start
			my ($start_time, $end_time, @values) = split m/\t/;
			my $s = ymd2time( $start_time, 1 ) or die("Wrong time format: $start_time");
			my $e = ymd2time( $end_time, 1 ) or die("Wrong time format: $end_time");
			$start_time = time2ymd( $s );
			$end_time = time2ymd( $e );
			print $fd_out join "\t", ($start_time, $end_time, @values);
			
		}else{#header
			if( m/begin time\s*:\s*(.*)/i ){
				my $start_time = $1;
				my $s = ymd2time( $start_time, 1 ) or die("Wrong time format: $start_time");
				$start_time = time2ymd( $s );
				print $fd_out "# Begin time: $start_time\n" ;
			}elsif( m/end time\s*:\s*(.*)/i ){
				my $end_time = $1;
				my $s = ymd2time( $end_time, 1 ) or die("Wrong time format: $end_time");
				$end_time = time2ymd( $s );
				print $fd_out "# End time  : $end_time\n" ;
			}elsif( m/Greenwich/i ){
				die "Time already in greenwich format in $file";
			}elsif( m/Dost/i ){
				print $fd_out $_;
				print $fd_out "# Time      : All times in Greenwich" unless( $header_ended );
				$header_ended = 1;
			}else{
				print $fd_out $_;
			}
			
		}
	}
	
	close $fd_out;
	close $fd;
}

