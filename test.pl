#!/usr/bin/perl

package Schedule::Depend::Test;

# Before `make install' is performed this script should be runnable with
# `make test'. After `make install' it should work as `perl test.pl'

######################### We start with some black magic to print on failure.

# Change 1..1 below to 1..last_test_to_print .
# (It may become useful if the test is moved to ./t subdirectory.)
#
# test::simple has some problems due to forks, etc. Until i can 
# work them out this is easier to hard-code.

BEGIN { $| = 1; print "1..37\n"; }
END {print "not ok 1\n" unless $loaded;}
use Schedule::Depend;
$loaded = 1;
print "ok 1\n";

######################### End of black magic.

# Insert your test code below (better if it prints "ok 13"
# (correspondingly "not ok 13") depending on the success of chunk 13
# of the test code):

########################################################################
# housekeeping & package variables
########################################################################

use Cwd qw( &cwd &abs_path );

use Data::Dumper;
	$Data::Dumper::Purity		= 1;
	$Data::Dumper::Terse		= 1;
	$Data::Dumper::Indent		= 1;
	$Data::Dumper::Deepcopy		= 0;
	$Data::Dumper::Quotekeys	= 0;

local $/;

my $pid	= $$;

my $ok	= 1;

my $dir		= abs_path '.';
my $tmpdir	= "$dir/tmp";

-d $tmpdir || mkdir $tmpdir, 02777
	or die "$tmpdir: $!";

my $defsched = <DATA> . 
qq{
	# alias

	cleanup = ls -l $tmpdir/*

	# attributes

	rundir	% $tmpdir
	logdir	% $tmpdir
	force	% 1
};

my @defargz =
(
	rundir => $tmpdir,
	logdir => $tmpdir,
);

########################################################################
# subroutines
########################################################################

{
	package Testify;

	use base qw( Schedule::Depend );

	# basically, forces everything in the schedule to 
	# be PHONY.

	sub unalias
	{
		my $que = shift;

		my $job = shift;

		my $sub = sub { $job };

		$sub
	}
}


sub test_debug
{
	print STDERR "Testing debug (seq $ok)\n";

	# tests single-argument constructor.
	# w/o schedule override, should run with
	# verbose == 1.

	Schedule::Depend->prepare( shift )->validate
}

sub test_execute
{
	print STDERR "$$: Testing execute (seq $ok)\n";

	my @argz = ( @defargz, sched => shift, verbose => 2 );

	# nice trick here it that the line numbers tell
	# you if the que failed inside of prepare or debug.

	if( my $que = Schedule::Depend->prepare( @argz ) )
	{
		$que->execute;
	}
	else
	{
		die "$$: Schedule::Depend failed to prepare: $@";
	}
}

sub test_debug_execute
{
	print STDERR "Testing debug w/ execute (seq $ok)\n";

	# this should run through all of the stages
	# quietly.

	my @argz = ( @defargs, sched => shift, verbose => 0 );

	eval
	{
		my $que = Schedule::Depend->prepare( @argz )->validate
			or die "$$: Schedule::Depend failed to prepare: $@";

		$que->execute;
	};

	die $@ if $@;
}

sub test_restart
{
	print STDERR "Testing execute w/ restart (seq $ok)\n";

	my @argz = ( @defargz, sched => shift, restart => 1, verbose => 1 );

	eval { Schedule::Depend->prepare( @argz )->execute };

	die $@ if $@;
}

sub test_derived
{
	print STDERR "Testing override w/ debug (seq $ok)\n";

	my $sched = shift;

	my @argz = ( @defargz, sched => $sched, verbose => 1 );

	eval { Testify->prepare( @argz )->validate->execute };

	die $@ if $@;
}

sub test_derived_restart
{
	print STDERR "Testing subcall w/ restart (seq $ok)\n";

	my @argz = ( @defargz, sched => shift, restart => 1, verbose => 1 );

	eval { Testify->prepare( @argz )->execute };

	die $@ if $@;
}

sub testify
{
	my $badnews = 0;

	my %subz =
	(
		test_debug				=> \&test_debug,
		test_execute			=> \&test_execute,
		test_debug_execute		=> \&test_debug_execute,
		test_restart			=> \&test_restart,
		test_derived			=> \&test_derived,
		test_derived_restart	=> \&test_derived_restart,
	);

	open STDOUT, '> test.log' or die "test.log: $!";

	for my $maxjob ( qw(1 2 0) )
	{
		for my $debug ( '', 'debug % 1' )
		{
			my $sched = $defsched . "\n$debug" . "\nmaxjob % $maxjob";

			for $sub ( keys %subz )
			{
				++$ok;

				print "\nTest $ok:  $sub / $debug / $maxjob\n";

				eval
				{
					unlink <$tmpdir/*.???>;

					$subz{$sub}->( $sched );

					die "$$: Forkatosis in Depend" if $$ != $pid;
				};

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

	{
		print "\nTesting alias syntax: $ok\n";

		unlink <./tmp/*>;

		package Findit;

		use base qw( Schedule::Depend );

		sub getscalled { print "I got called!"; 0 };

		# getscalled should end up as a sub call in unalas.

		my $sched =
		q{
			# should be loacated 
			bar = getscalled
			foo = File::Basename::basename
			baz = { print "Hello, world!"; 0 }
			bam = /bin/pwd


			foo : bar
			baz : foo
			bam : baz
			/bin/ls : bam
		};

		my %argz = 
		(
			rundir	=> $tmpdir,
			logdir	=> $tmpdir,
			sched	=> $sched, 
			verbose	=> 0,
		);

		eval { Findit->prepare( %argz )->execute };

		++$ok;
		print STDERR $@ ? "\nnot ok $ok\t:$@\n" : "\nok $ok\n";
	}

	{
		print STDERR "Testing group execution (seq $ok)\n";

		unlink <./tmp/*>;

		my $sched = 
		qq(
			foo : before
			after : foo

			before = ls -lt ./tmp
			after  = ls -lt ./tmp

			# two files should be 5 sec apart.

			foo < bar : bletch blort >
			foo < bar    = /bin/date > $ENV{PWD}/tmp/after >
			foo < bletch = /bin/date > $ENV{PWD}/tmp/before >
			foo < blort  = /bin/sleep 3 >
		);

		my %argz =
		(
			sched	=> $sched,

			maxjob	=> 1,
			verbose	=> 2,

			rundir	=> $tmpdir,
			logdir	=> $tmpdir,
		);

		my $que = Schedule::Depend->prepare(%argz);
		
		$que->validate->execute;
	}

	eval
	{
		unlink <$tmpdir/*pid>;
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

# the normal jobs depend on "startup" exiting zero.
# the final exit status will be that of cleanup.

a b c d e f : startup

cleanup : a b c d e f
