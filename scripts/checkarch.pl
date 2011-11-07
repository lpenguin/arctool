#!/usr/bin/perl

use strict;
use Time::Local;
use FindBin;

my $script_path = $FindBin::Bin;
my $wd = `pwd`;
chomp $wd;
my $table_file = "$wd/table.csv";

`echo 'File:Block:Campaign:Step:Begin time:End time:Min Teff:Max Teff:Avg Teff:Min Nakz:Max Nakz:Avg Nakz:Data Density:Params:Units:Errors' > $table_file`;
		
# Perl trim function to remove whitespace from the start and end of the string
sub trim($)
{
	my $string = shift;
	$string =~ s/^\s+//;
	$string =~ s/\s+$//;
	return $string;
}

sub ymd2time($){
	return 0 unless(shift =~ m/(\d\d\d\d)\D(\d\d)\D(\d\d)\D(\d\d)\D(\d\d)\D(\d\d)/ );
	return timegm($6,$5,$4,$3,$2-1,$1);
}

for (@ARGV){
	check_file($_);
}


my $no_errors = 1;
my @errors = ();
sub error($){
	my $msg = shift;
	push @errors, $msg;
	print "$msg\n";
	$no_errors = 0;
}

sub reset_proc{
	$no_errors = 1;
	@errors = ();
}

sub read_header($){
	my $fd = shift;
	my $res = {};
	while(<$fd>){
		chomp;
		return $res if( m/^\s/ || m/^Start/ ); 
		next unless $_;
		unless( m/^#([^\:]+)\:(.*)/ ){
			error " Bad header line: $_";
			next;
		}
		
		my $key = trim( lc $1 );
		my $val = trim( $2 );
		
     	$key =~ s/\s+/_/g; 
		
		$res->{$key} = $val unless exists $res->{$key};
	}
	error " End of file";
	return $res;
}

sub print_header($$){
	my ($header, $fd) = @_;
	open my $tfd, "$script_path/pst.header" or error("Cannot find header file");
	while(<$tfd>){
		for my $key (keys %{$header} ){
			s/\$$key/$header->{$key}/g;
		}
		print $fd $_;
	}
	close $tfd;
}
sub check_file($){
	my ($file) = @_;
	print "Processing $file ...\n";
	
	reset_proc;
	my $file_type = 'human';
	$file_type = 'graph' if $file =~ m/\.graph\.pst$/i;
	
#	error " Warning: possible wrong file: $file (not *.graph.pst)"	unless( $file =~ m/\.graph\.pst$/i );
	 
	open my $fd, $file;
	
	my ($head_begin_time, $head_end_time, $begin_time, $end_time);
	
	my ( $min_teff_header, $max_teff_header, $min_nakz_header, $max_nakz_header, $avg_nakz_header, $n_nakz_header, $avg_teff_header, $n_teff_header )=
		(undef, undef, undef, undef, 0, 0);

	my $hole_count = 0;
	my $avg_step = '60 sec.';
	
	my $prev_time = undef;
	my $line = 0;
	my $data_start_line;
	my $min_dost = 0.9;
	
	my $header = read_header( $fd );
	$head_begin_time = $header->{begin_time};
	$head_end_time = $header->{end_time};
	error " Begin time and end time must be in header" 
		if( !defined($head_begin_time) || !defined($head_end_time) );
		   	
	while(<$fd>){
		$line++;
	
		next unless($_);

		
		next if( m/^Start/i );
		next if( m/^\s/i );
#		if( !defined($head_begin_time) || !defined($head_end_time) ){
#		    error " Error: Begin time and end time must be in header";
#		    last;
#		}
		
		my ( $time1_str, $time2_str, $hole, $min_teff, $max_teff, $avg_teff, $dost_teff, $min_nakz, $max_nakz, $avg_nakz, $dost_nakz );		
		
		error "Nan at $line" if( m/nan\s+[\d\.]+\s+/ );
		my @fields = split m/\s*\t\s*/;
		if( $file_type eq 'graph'){
			( $time1_str, $time2_str, $hole,
			  $min_teff, $max_teff, $avg_teff, $dost_teff, 
			  $min_nakz, $max_nakz, $avg_nakz, $dost_nakz ) = @fields ;
		}else{
			( $time1_str, $time2_str, $hole,
			  $avg_teff, $dost_teff, 
			  $min_nakz, $max_nakz, $avg_nakz, $dost_nakz ) = @fields ;	
		}
		 
		$hole_count += $hole - 1 if( $hole > 0);		
#		( $time1_str, $time2_str, $hole, $min_teff, $max_teff, $avg_teff, $dost_teff, $min_nakz, $max_nakz, $avg_nakz, $dost_nakz ) = split m/\t/;
		
		if( $dost_teff > $min_dost && $dost_nakz > $min_dost && $avg_nakz > 0 && ! ($avg_teff =~ m/nan/) ){
			$min_teff_header = $avg_teff if( $avg_teff < $min_teff_header || !defined($min_teff_header));
			$max_teff_header = $avg_teff if( $avg_teff > $max_teff_header || !defined($max_teff_header) );
			$avg_teff_header += $avg_teff;
			$n_teff_header++;
		}
		
		if( $dost_nakz > $min_dost ){
			$min_nakz_header = $avg_nakz if( $avg_nakz < $min_nakz_header || !defined($min_nakz_header) );
			$max_nakz_header = $avg_nakz if( $avg_nakz > $max_nakz_header || !defined($min_nakz_header) );	
			$avg_nakz_header += $avg_nakz ;
			$n_nakz_header++;		
		}
		
		my $time1 = ymd2time( $time1_str );
		my $time2 = ymd2time( $time2_str );
		
#		$min_time = $time1 if( !defined($min_time) || $time1 < $min_time );
#		$max_time = $time1 if( !defined($max_time) || $time1 < $min_time );
		
		$begin_time = $time1_str unless( defined( $begin_time ) );
		$end_time = $time2_str;
			
		if( defined( $prev_time ) ){
			my $chk_hole = $time1 - $prev_time;
			if( $chk_hole != $hole){
				error " Invalid hole $hole, instead of: $chk_hole ($line: $time1_str, $time2_str)";
			}
			if( $chk_hole < 0 ){
				error " Negative hole $hole ($line: $time1_str, $time2_str)";
			}
		}else{
			$data_start_line = $line - 1;
		}
		$prev_time = $time2;
	}
	close $fd;
	if( abs( ymd2time($begin_time) - ymd2time($head_begin_time) ) > 1 ){
	    error " Begin Time: $begin_time != Header Begin Time: $head_begin_time";
	}
	if( abs( ymd2time($end_time) - ymd2time($head_end_time) ) > 1 ){
	   error " End Time: $end_time != Header End Time: $head_end_time";
	}
	
	$avg_nakz_header = $n_nakz_header ? sprintf("%.2f", $avg_nakz_header / $n_nakz_header ) : "";
	$avg_teff_header = $n_teff_header ? sprintf("%.2f", $avg_teff_header / $n_teff_header ) : "" ;
	
	my $ln = ymd2time($end_time) - ymd2time($begin_time);
	my $density = $ln ? sprintf( "%.2f %%", (1 - $hole_count / $ln ) * 100 ) : "";
	
	$header->{min_teff} = $min_teff_header;
	$header->{max_teff} = $max_teff_header;
	$header->{avg_teff} = $avg_teff_header;
	$header->{min_nakz} = $min_nakz_header;
	$header->{max_nakz} = $max_nakz_header;
	$header->{avg_nakz} = $avg_nakz_header;
	$header->{data_dens} = $density;
	$header->{avg_step} = $avg_step;
	
	unless( $header->{params}){
		$header->{params} = 'Teff, Nakz, Hgr_suz, Ofset, G1K_Vol, A_Thn, Cbor, KQ_Max, QL_Min';
	}	
	unless( $header->{units} ){
		$header->{units} = ', MWt, %, %, tonn/hour, grad C, , , Wt/cm';
	}
	my $errors = join " ; ", @errors;
	$errors = "ok" unless( $errors );
	if( $no_errors ){
		print "Ok\n";
	}
	open $fd, $file;
	my $h = read_header($fd);
	my $kksl = $_;
	open my $tfd, '>'.$file.'.tmp';
	
	print_header $header, $tfd;
	print $tfd "\n";
	print $tfd $kksl."\n";
	while(<$fd>){
		my $null_dost = sprintf("%-8s\t%-8s\t", '0.00', '0.00');			
		s/nan\s+[\d\.]+\s+/$null_dost/g;

		print $tfd $_;
	}
	
	close $tfd; 
	close $fd;
	
	unlink $file;
	rename $file.'.tmp', $file;
	

	#}
	#write to table
	if( $file =~ m/graph\.pst$/ ){
	    open my $tabfd, ">>", $table_file;
    	    print $tabfd 
    	    	"\"$file\":\"$header->{block}\":\"$header->{campaign}\":\"$header->{step}\":\"$header->{begin_time}\":\"$header->{end_time}\":\"$header->{min_teff}\":\"$header->{max_teff}\":\"$header->{avg_teff}\":\"$header->{min_nakz}\":\"$header->{max_nakz}\":\"$header->{avg_nakz}\":\"$header->{data_dens}\":\"$header->{params}\":\"$header->{units}\":\"$errors\"\n";
	    close $tabfd;
	}
	print "\n";
	
	
}
