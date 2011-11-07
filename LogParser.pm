package LogParser;
use strict;
BEGIN {

		use FindBin;
	use lib "$FindBin::Bin/";
	use Misc;
	use Loggy;
	use ErrorHandler;
    @LogParser::ISA = qw(Exporter);
    @LogParser::EXPORT = qw( parse_log )
}

sub parse_log($){#status=>'ok' - good log, status=>'ok' error - bad log
	my $file = shift;
	my $fd = open_read( $file );
	
	my $error_id = '31';
	my $error = {
		status => 'ok'
	};
	<$fd>;<$fd>;<$fd>;
	while( <$fd> ){
		my ($id, $name, $time, $text) = split m/\s*\|/;
#		info "Id: $id";
		if( $id eq $error_id){
			$error = {
				status => 'error',
				message => $text
			};
			if( $text =~ m/violated\s(\w+)\.(\w+)/g ){
				$error->{type} = 'file';
				$error->{file} = $1.'.'.$2;
			}
			return $error;
		}
	}
	
	close $fd;
	return $error;
}
return 1;

END {}
