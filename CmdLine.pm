package CmdLine;
use Term::ANSIColor;
use strict;

BEGIN {
	use Exporter;
	use FindBin;
	use lib "$FindBin::Bin/";
	use Misc;
	@CmdLine::ISA = qw(Exporter);
    @CmdLine::EXPORT = qw( &newCmdLine readCmdLine addAction addActions addOption addOptions printUsage setMainOptions processCmdLine setDefaultAction);
}


sub newCmdLine(;$){
    my ($props) = @_;
    $props = {} unless($props);
    
	return expand({
	        name => '',
	        description => '',
	        actions => {},
	        options => {}},
	        $props);
}

sub setDefaultAction($$){
    my ($cmdLine, $action) = @_;
    $cmdLine->{defaultAction} = $action;
    return $cmdLine;
}

sub addAction($$){
	my ($cmdLine, $props ) = @_;
	my $name = $props->{name};
	return $cmdLine unless( $name );
	
	unless( ref($props->{subr}) ){
		my $functionName = $props->{subr};
		$props->{subr} = sub($@){   my ($options, @args) = @_;
									eval("main::$functionName(\$options, \@args) ;"); 
									print $@ if$@;
								};
	}
	
	$cmdLine->{actions}->{$name} = expand({
		name => $name,
		shortName => '',
		description => "",
		subr => undef,
		options => []
	}, $props);
	
	return $cmdLine;
}

sub addActions($@){
	my ($cmdLine, @actions) = @_;
    for my $action (@actions){
        addAction($cmdLine, $action);
    }
    return $cmdLine;
}

sub setMainOptions($@){
    my ($cmdLine, @optionNames) = @_;
    addAction( $cmdLine, { 
        name => '__main__',
        options => \@optionNames
    });
    return $cmdLine;
}

sub addOption($$){
    my ($cmdLine, $props) = @_;
    if( ref($props) != "HASH" ){
        $props = {
            name => $props
        };
    }
    
    $props->{name} = $props->{shortName} unless( $props->{name} );
    
    return $cmdLine unless( $props->{name} );

    my $keyName = $props->{name};
    $keyName =~ s/-/_/g;
    
    my $option = expand({
        name => $props->{name},
        altNames => [],
        shortName => '',
        description => '',
        bool => 0,
        array => 0,
        value => undef,
        keyName => $keyName
    }, $props);
    
    $cmdLine->{options}->{$option->{name}} = $option;

    return $cmdLine;
}

sub addOptions($@){
    my ($cmdLine, @options) = @_;
    for my $option (@options){
        addOption($cmdLine, $option);
    }
    return $cmdLine;
}


sub getOptions($@){
    my ($options, @optionNames) = @_;
    my $res = {};
    for my $optionName (@optionNames){
        my $option = getOption($options, $optionName);
        $res->{$option->{name}} = $option if($option); 
    }
    return $res;
}

sub getOption($$){
    my ($options, $optionName) = @_;
    for my $key (keys( %{$options})){
        my $option = $options->{$key};
        if( $option->{name} eq $optionName or $option->{shortName} eq $optionName or defined(array_find($optionName, @{$option->{altNames}}))){
            return $option;
        }
    }
    return undef;
}

sub getAction($$){
    my ( $actions, $actionName ) = @_;
    for my $key (keys( %{$actions})){
        my $action = $actions->{$key};
        if( $action->{name} eq $actionName or $action->{shortName} eq $actionName ){
            return $action;
        }
    }
    return undef;
}

sub setOptions($$){
    my ($optionContainer, $optionDescrs) = @_;
    for my $name (keys(%{$optionDescrs})){
        setOption( $optionContainer, $optionDescrs->{$name}, 1) if( $optionDescrs->{$name}->{bool} ) ;
    }
    return $optionContainer;
}

sub setOption($$$){
    my ($optionContainer, $optionDescr, $value) = @_;
    my $keyName = $optionDescr->{keyName} ;
    if( $optionDescr->{array} ){
         $optionContainer->{ $keyName } = [] if( !exists( $optionContainer->{ $keyName } ) );
         push @{$optionContainer->{ $keyName }}, $value
    }else{
        $optionContainer->{ $keyName } = $value;
    }
    return $optionContainer;
}



sub printOptions($){
    my ($options) = @_;
    for my $key ( sort(keys( %{$options} ))){
        my $option = $options->{$key};
        if( $option->{name} ne $option->{shortName} ){
            my $name;
            $name = "--$option->{name}";
            $name .= "[]" if( $option->{array} );
            $name .= '=value' unless( $option->{bool} );

            print sprintf("\t%-22s\t%s", $name, $option->{description});
            print "\n"; 
            if( $option->{shortName} ){
                print "\t -$option->{shortName}\n";
            }
            if( $option->{altNames} ){
            	for my $altName (@{$option->{altNames}}){
            	    print "\t--$altName\n";
            	}

            }
        }else{
            my $name;
            $name = " -$option->{shortName}";
            $name .= "[]" if( $option->{array} );
            $name .= '=value' unless( $option->{bool} );
            print sprintf("\t%-22s\t%s", $name, $option->{description});
            print "\n"; 
        }
        print "\n";
    }
}

sub printUsage($){
    my ($cmdLine) = @_;
    
    print "$cmdLine->{name}: ";
    print "$cmdLine->{description}\n\n";
    my $actions = $cmdLine->{actions};
    
    if( $actions->{'__main__'} ){
        print "Main Options:\n\n";
        printOptions(getOptions( $cmdLine->{options}, @{$actions->{'__main__'}->{options}}) );
    }
    
    print "Actions:\n\n";
    for my $key (keys(%{$actions})){
        next if( $key eq '__main__');
        
        my $action = $actions->{$key};
        #print color 'bold';
        print sprintf("%-31s", $action->{name});
        #print color 'reset';
        print $action->{description};
        #print color 'reset';
        #next;
        print "\n";
        #print sprintf("%-31s %s\n", $action->{name}, $action->{description});
        #print color 'bold';
        print $action->{shortName}."\n" if( $action->{shortName} );
        #print color 'reset';
        my $options = getOptions( $cmdLine->{options}, @{$action->{options}} );
        if( $options ){
            print "\n";
            printOptions($options);
        }
        print "\n";
    }
}
sub readOptions($$){
    my ($cmdLine, $args) = @_;
    my $optionContainer = {};
    my $s;
    do{
	    $s = shift @$args;
	    if( $s =~ m/^-(\w+)$/ ){
	        my $r = getOptions( $cmdLine->{options}, split m//, $1 ); 
	        setOptions($optionContainer, $r) if( keys(%{$r}) );
	    }elsif( $s =~ m/^-(\w+)=*(.*)$/){
    	    my $val = defined( $2 ) && $2 ne '' ?  $2  : 1;  
	        my $o = getOption( $cmdLine->{options}, $1 );
	        setOption( $optionContainer, $o, $val) if( $o )
	    }elsif($s =~ m/^--([\w-]+)=*(.*)$/){
	        my $val = defined( $2 ) && $2 ne '' ? $2 : 1;
   	        my $o = getOption( $cmdLine->{options}, $1 );
            setOption( $optionContainer, $o, $val)  if( $o )
        }else{
             @$args = ($s, @$args);
             $s = '';
        }
	}while( $s );
	
   
	return $optionContainer;
}

sub readCmdLine($){
    my ($cmdLine) = @_;
    my $action = shift @ARGV;
    my $options = readOptions($cmdLine, \@ARGV);
    my @args = @ARGV;
    $cmdLine->{readed} = {
        action => $action,
        options => $options,
        args => \@args
    };
    return $cmdLine;
}

sub processCmdLine($){
    my ($cmdLine) = @_;
    my $readed = $cmdLine->{readed};
    my $action;
    unless( $readed->{action} ){
        $action = $cmdLine->{defaultAction} ;
    }else{
        $action = getAction($cmdLine->{actions}, $readed->{action});
    }
    
    unless( $action) {
        print "Wrong action: $readed->{action}\n";
        return;
    }
    
    my $sub = $action->{subr};
    $sub->($readed->{options}, @{$readed->{args}});
    
}
return 1;

END {}
