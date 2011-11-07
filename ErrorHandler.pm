package ErrorHandler;
BEGIN
{
	use Carp qw(croak confess);
    use Exporter();
    use strict;
    use FindBin;
    
    use lib "$FindBin::Bin/";
	
    @ErrorHandler::ISA = qw(Exporter);
    @ErrorHandler::EXPORT = qw(&abort &set_error_handler);

    #\&die;
}

our $error_handler = \&def_abort;

sub def_abort(;$){
	my $message = shift;
    ($package, $filename, $line, $subroutine ) = caller(1);
    $filename =~ s/.*\///;
    print "$message at ${filename}:$line\n";
    die $message;
#	confess $message;
#	die shift, "\n";
	#die;
}
sub set_error_handler($){
	$error_handler = $_[0]
}
sub abort(;$){
	our $error_handler;
	my ($message) = @_;
	$message||='';
	&$error_handler($_[0]);
}
return 1;
END {}
