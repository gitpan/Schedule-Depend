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

select STDERR;

my $ok = 1;

sub test_debug
{
	print "Testing debug ($ok)\n";

	my @argz = ( shift );

	Schedule::Depend->prepare( @argz )->debug
}

sub test_execute
{
	print "Testing execute ($ok)\n";

	my @argz = ( sched => shift, verbose => 1 );

	Schedule::Depend->prepare( @argz )->execute
}

sub test_debug_execute
{
	print "Testing debug w/ execute ($ok)\n";

	my @argz = ( sched => shift, verbose => 1 );

	Schedule::Depend->prepare( @argz )->debug->execute;
}

sub test_restart
{
	print "Testing execute w/ restart ($ok)\n";

	my @argz = ( sched => shift, restart => 1 );

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

	for my $maxjob ( qw(1 2 0) )
	{
		eval { unlink <$rundir/*pid> };

		for my $debug ( '', 'debug = 1' )
		{
			my $sched = $defsched . "\n$debug" . "\nmaxjob = $maxjob";

			for( @subz )
			{
				++$ok;

				eval { &$_( $sched ) };

				if( $@ )
				{
					print "Bad news, boss: $@";

					print STDOUT "\nnot ok $ok\t$@\n";

					++$badnews if $@;
					
				}
				else
				{
					print STDOUT "\nok $ok\n";
				}
			}
		}
	}

	eval
	{
		unlink <$rundir/*pid>;
		rmdir $rundir;
	};

	print STDERR "Failed cleanup: $@" if $@;

	$badnews
}



# exit non-zero if we hit any snags.

testify

__DATA__

# aliases for the targets and depdencies

startup = pwd

a = echo "a" 1>&2
b = echo "b" 1>&2
c = echo "c" 1>&2
d = echo "d" 1>&2
e = echo "e" 1>&2
f = echo "f" 1>&2

# the names "startup" and "cleanup" are arbitrary. at least one
# job must exist without dependencies (explicitly via "foo:"
# or implicitly as dependency that has no dependencies of its own).
# in this case "startup" has no further dependencies and will
# be the first job run. 

# the normal jobs depend on our "startup" job being complete.

a b c d e f : startup

cleanup : a b c d e f
