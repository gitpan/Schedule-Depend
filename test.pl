#!/usr/bin/perl

# Before `make install' is performed this script should be runnable with
# `make test'. After `make install' it should work as `perl test.pl'

######################### We start with some black magic to print on failure.

# Change 1..1 below to 1..last_test_to_print .
# (It may become useful if the test is moved to ./t subdirectory.)

BEGIN { $| = 1; print "1..1\n"; }
END {print "not ok 1\n" unless $loaded;}
use Schedule::Depend;
$loaded = 1;
print "ok 1\n";

######################### End of black magic.

# Insert your test code below (better if it prints "ok 13"
# (correspondingly "not ok 13") depending on the success of chunk 13
# of the test code):

use Cwd qw( &abs_path );

my $dir = abs_path '.';

my $rundir = "$dir/tmp";

-d $rundir || mkdir $rundir, 02777
	or die "$rundir: $!";

unlink <$rundir/*>;

local $/;
my $defsched =
	<DATA> . "\nrundir = $rundir\nlogdir = $rundir\n\ncleanup = ls -l $rundir";

my $ok = 1;

my $pid = $$;

sub test_debug
{
	print STDERR "Testing debug (seq $ok)\n";

	# also tests where the single-argument form of the
	# constructor works properly.

	my @argz = ( shift );

	Schedule::Depend->prepare( @argz )->debug
}

sub test_execute
{
	print STDERR "$$: Testing execute (seq $ok)\n";

	my @argz = ( sched => shift, verbose => 2 );

	if( my $que = Schedule::Depend->prepare( @argz ) )
	{
		$que->execute
	}
	else
	{
		undef
	}
}

sub test_debug_execute
{
	print STDERR "Testing debug w/ execute (seq $ok)\n";

	# this should run through all of the stages 
	# quietly.

	my @argz = ( sched => shift, verbose => 0 );

	if( my $q = Schedule::Depend->prepare( @argz )->debug )
	{
		$q->execute;
	}
	else
	{
		undef
	}
}

sub test_restart
{
	print STDERR "Testing execute w/ restart (seq $ok)\n";

	my @argz = ( sched => shift, restart => 1, verbose => 1 );

	Schedule::Depend->prepare( @argz )->execute
}


sub testify
{
	my $badnews = 0;

	my @subz =

	(
		\&test_debug,
		\&test_execute,
		\&test_debug_execute,
		\&test_restart,
	);

	open STDOUT, '> test.log'
		or die "test.log: $!";

	for my $maxjob ( qw(1 2 0) )
	{
		eval { unlink <$rundir/*.???> };

		for my $debug ( '', 'debug = 1' )
		{
			my $sched = $defsched . "\n$debug" . "\nmaxjob = $maxjob";

			for( @subz )
			{
				++$ok;

				print "\nTesting Sequence: $ok\n";

				&$_( $sched );

				die "$$: Forkatosis in Depend" if $$ != $pid;

				if( $@ )
				{
					print "Bad news, boss: $@";

					print STDERR "\nnot ok $ok\t$@\n";

					++$badnews if $@;
					
				}
				else
				{
					print STDERR "\nok $ok\n";
				}
			}
		}
	}

	eval 
	{
		unlink <$rundir/*>;
		rmdir $rundir;
	};

	$badnews
}



# exit non-zero if we hit any snags.

testify

__DATA__

# aliases for the targets and depdencies

startup = /bin/pwd

a = echo "a"
b = echo "b"
c = echo "c"
d = echo "d"
e = echo "e"
f = echo "f"

# the names "startup" and "cleanup" are arbitrary. at least one
# job must exist without dependencies (explicitly via "foo:"
# or implicitly as dependency that has no dependencies of its own).
# in this case "startup" has no further dependencies and will
# be the first job run. 

# the normal jobs depend on our "startup" job being complete.

a b c d e f : startup

cleanup : a b c d e f
