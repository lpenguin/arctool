package Loggy;
use strict;

BEGIN {
	use Exporter();

	use FindBin;
	use lib "$FindBin::Bin/";
# 	use Misc;
	use ErrorHandler;
    @Loggy::ISA = qw(Exporter);
    @Loggy::EXPORT = qw( &log_indent &log_unindent &mylog &set_log_file &info &out
    &log_file &blah &warn &log_to_file &unset_log_file &set_log_level &log_level
    &set_warn_handler
    LOG_ALL LOG_WARN LOG_ERRORS LOG_NOTHING LOG_INFO);
	
	use constant LOG_ALL => 0;
	use constant LOG_INFO => 1;
	use constant LOG_WARN => 2;
	use constant LOG_ERRORS => 3;
	use constant LOG_NOTHING => 4;
}

our $warn_handler = \&def_warn;
my $log_indent_level = 0;#Количество отступов перед сообщениями
my $log_level = 0; #Уровень важности выводимых сообщений: 0 - все, 1 - предупреждения и ошибки, 2 - только ошибки, 3 -ничего
my $log_file; #Файл для вывода сообщений
my $log_to_file = 0; #Выводить ли лог в файл
# set_log_level( LOG_NOTHING);
sub log_indent(){
	$log_indent_level++;
}

sub log_unindent(){
	$log_indent_level--;
	$log_indent_level = 0 if($log_indent_level < 0);

}

sub mylog($;$$$){
	my ($message, $level, $indent, $log_to_file) = @_;
	
# 	my ($package, $filename, $line, $subroutine ) = caller(1);
#     $filename =~ s/.*\///;
#     print "mylog at ${filename}:$line\n";
    $level = $log_level unless(defined($level));
	
	$indent = $log_indent_level unless(defined($indent));;
# 	$log_to_file = 1;
	
	unless($level < $log_level){
		my $indent_space = "";
		
		for(1 .. $indent){
			$indent_space.=' ';
		}
		$message =~ s/\n/\n$indent_space/g;
		$message = $indent_space.$message;
		
		$message .= "\n" unless( $message =~ m/\n$/ );
		
		print $message;
	}
	log_to_file($message) if( $log_file );
}

sub out($){
	print shift;
	print "\n";
}

sub blah($;$){
	my ($message, $indent) = @_;
	#print "<<<< $log_indent_level\n";
	$indent |= $log_indent_level;
	mylog($message, LOG_ALL, $indent);
}

sub set_warn_handler($){
	$warn_handler = $_[0]
}
sub warn($;$){
	our $error_handler;
	my ($message, $indent) = @_;

	&$warn_handler($message, $indent);
}

sub set_warn_handler($){
	$warn_handler = $_[0]
}

sub def_warn($;$){
}

sub def_warn($;$){
	my ($message, $indent) = @_;
	$indent |= $log_indent_level;
	mylog($message, LOG_WARN, $indent);
}

sub info($;$){
	my ($message, $indent) = @_;
	$indent |= $log_indent_level;
	mylog($message, LOG_INFO, $indent);
}
# sub abort($;$){
# 	my ($message, $indent) = @_;
# 	$indent |= $log_indent_level;
# 	mylog($message, 2, $indent);
# 	#ErrorHandler::abort($message);
# }

sub log_to_file($){
	return unless( $log_file );
	my $message = shift;
	chomp($message);
	$message = time2ymd(time).' '.$message;
	
	open my $fd, ">>$log_file";
	print $fd $message."\n";
	close $fd;
}

sub unset_log_file(){
	$log_file = "";
	$log_to_file = 0;
}

sub set_log_level($){
	$log_level = shift @_;
}

sub log_level(){
	return $log_level;
}

sub set_log_file($){
	$log_file = shift;
	$log_to_file = 1;
#	log_to_file("------------------------------");
}

sub log_file(){
	return $log_file;
}

sub time2ymd($;$){
	my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) =
                                                localtime(shift);
 	my $dl = shift;
	$dl=' ' unless(defined($dl)); 	
    $year += 1900;
    $mon++;
    return sprintf("%04d.%02d.%02d$dl%02d:%02d:%02d", $year,$mon,$mday,$hour,$min,$sec);
}

return 1;
END{}
