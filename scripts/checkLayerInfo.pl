#!/usr/bin/perl

use strict;
use Time::Local;
sub dmy2time($){
	return 0 unless(shift =~ m/(\d\d)\D(\d\d)\D(\d\d\d\d)\D(\d\d)\D(\d\d)\D(\d\d)/ );
	return timegm($6,$5,$4,$1,$2-1,$3);
}

my $file = shift @ARGV;
open my $fd, $file;
my $fileStrings = 0;

my $prevTime;
my $line = 0;
while(<$fd>){
    $line++;
    
    if( m/files/ ){
        $fileStrings = 1;
    }else{
        next unless $fileStrings;
    }
    
    my ($fileName, $begin, $end) = split m/\s+/;
    $begin =~ s/_/\s/g;
    $end =~ s/_/\s/g;
    $begin =~ s/\.\d+$//;
    $end =~ s/\.\d+$//; 
    
    my $beginTime = dmy2time( $begin );
    my $endTime = dmy2time( $end );
    
    unless( defined( $prevTime )){
        $prevTime = $endTime;
        next;
    }
    
    if( $prevTime > $beginTime ){
        print "Error: prev time > begin time\n $prevTime > $beginTime \nline $line\n";
    }
    $prevTime = $endTime;
}

print "closing\n";
close $fd;
