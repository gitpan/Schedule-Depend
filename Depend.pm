# see DATA for pod.

########################################################################
# housekeeping
########################################################################

package Schedule::Depend;

use strict;

# make messages output during the use phase prettier.

local $\ = "\n";
local $, = "\n\t";
local $| = 1;

# values for $que->{skip}{$job}:
#
#	CLEAN is is used in restart mode to flag jobs that have
#	completed cleanly and don't need to be rerun.
#
#	ABORT flags jobs with a failed dependency that are being
#	skipped because they cannot be run. their waitfor entries
#	will be flagged with ABORT also.

use constant CLEAN	=>  1;
use constant ABORT	=> -1;

use Carp;

use File::Basename;

use Text::Balanced qw( &extract_codeblock );

# pretty-print the object in debug mode.

use Data::Dumper;
	$Data::Dumper::Purity		= 1;
	$Data::Dumper::Terse		= 1;
	$Data::Dumper::Indent		= 1;
	$Data::Dumper::Deepcopy		= 0;
	$Data::Dumper::Quotekeys	= 0;

########################################################################
# package variables
########################################################################

our $VERSION = 0.15;

# hard-coded defaults used in the constructor (prepare). 
# these can be overridden in the config file via aliases,
# e.g., "rundir = /tmp".
#
# using the PROJECT environment variable makes rooting
# the execution a bit simpler in test environments.

local $ENV{PROJECT} ||= '';

my %defaultz =
(
	# i.e., don't choke the number of parallel jobs.
	# change this to 1 for serial behavior.

	maxjob	=> 0,

	# don't run in debug mode by default. allows 
	# safety check for testing schedules.

	debug	=> 0,
);

########################################################################
# subroutines
########################################################################

# $que evalues to true if there is anything left to be run

use overload q{bool} => sub{ scalar %{ $_[0]->{queued} } };

# jobs that are ready to be run, these are any job for
# which keys %{ $que->{queued}{$job} } is false.
#
# the sort delivers jobs in lexical order -- makes 
# debugging a bit easier and allows higher-priroty 
# jobs to get run sooner if maxjobs is set. derived
# classes might update the sort to deliver jobs with
# more waiting jobs or which were available soonest.

sub ready
{
	my $queued = $_[0]->{queued};

	sort grep { ! keys %{ $queued->{$_} } }  keys %$queued
}

# list of all jobs queued.

sub queued { sort keys %{ $_[0]->{queued} } }

# syntatic sugar. 
# mainly here as an example of what the que entry does.

sub depend
{
	my $que = shift;
	my $job = shift;

	sort keys %{ $que->{depend}{$job} };
}

# take a job out of the queue. this is not the same as
# listing it as complete: jobs must be removed from the
# queue immediately when they are forked since the que
# may be examined many times while the job is running.

sub dequeue
{
	my $que = shift;
	my $job = shift;

	delete $que->{queued}{$job};
}

# handle job completion:
#
#	remove this job from the list of depend list of jobs
#	that depend on it.
#
#	if there is a cleanup method defined for the $que 
# 	then call it for the job.

sub complete
{
	my $que = shift;
	my $job = shift;

	# syntatic sugar, also a minor speedup w/ $queued..

	my $queued = $que->{queued};
	my $depend = $que->{depend};

	delete $queued->{$_}{$job} for @{ $depend->{$job} };

	if( my $cleanup = $que->can('cleanup') )
	{
		print "$$: Cleaning up after: $job" if $que->{verbose};

		&$cleanup( $que, $job )
	}
}

# the result here gets passed to runjob for
# execution. idea here is to hand off whatever
# seems most useful to run.
#
# Changes to unalias should be carefully checked
# againsed what runjob expects to handle.
#
# checking for the package is 
# generally overkill but allows this to be
# called as a normal subroutine and still
# return something useful.
#
# Note: need to make sure that:
#
#	foo = Package::Name->blah
#
# ends up as Package::Name->blah( 'foo' );
#
# the $packge, $subname trick will break on 
# this because "Name->blah" won't be any kind
# of valid subroutine w/in the package.
#
# question is whether the $que->can will handle
# this properly or another test is required in 
# order to deal with these.

sub unalias
{
	$DB::single = 1;

	# $que might be a blessed object or it might be
	# a package name. Either way, we will be able to
	# use it in "$que->can( $job )".

	my $que = shift;
	my $job = shift;

	# if $que is a referent then access its alias sub-hash
	# and grab out the entry for this job. If it doesn't
	# exist then return the job.

	my $run = ref $que && defined $que->{alias}{$job} ?
		$que->{alias}{$job} : $job;

	# the job may be a fully qualified subroutine
	# with a package name (e.g., Foo::Bar::bletch).
	# breaking this down up here makes the elsif
	# test below cleaner.

	my ( $package, $subname ) = $run =~ /^(.+)::(.+?$)$/;

	# after looking for placeholders, check for a 
	# fully-qualified sub, then a method then 
	# anything lying around in the current package 
	# finally anything that can be compiled into 
	# runnable perl..
	#
	# if none of these is found the caller gets back
	# a string.
	#
	# reminder to anyone overloading this: the $package->can
	# is necessary to avoid $que->can( 'Foo::bar' ) returning
	# a package-qualified sub reference. 
	#
	# the "sub" in the eval converts whatever block
	# was found in the alias to an anon subroutine.
	# any "sub" string in the original alias would
	# have been stripped by extract_codeblock.

	if( $run eq '' or $run eq 'PHONY' )
	{
		$run = sub { 0 };
	}
	elsif( $package && $subname && ( my $sub = $package->can($subname)) )
	{
		$run = sub { $sub->( $job ) };
	}
	elsif( $sub = $que->can( $run ) )
	{
		$run = sub { $que->$sub( $job ) };
	}
	elsif( $sub = __PACKAGE__->can($run) )
	{
		$run = sub { __PACKAGE->$sub( $job ) };
	}
	elsif( my ($block,$junk) = extract_codeblock($run,'{}') )
	{
		$run = eval "sub $block"
			or croak "$$: Bogus code block: $block";
	}

	# at this $run is either the expanded alias
	# as a string or a closure that will call 
	# $run with the original job tag as its argument.

	$run
}

# execute the job after the process has forked.
# overloading this is heavily tied to changes 
# in unalias.
#
# the default behavior is to handle code references
# by exiting with the exit status, anything else
# gets pushed into the shell. this should handle
# nearly all cases, since the unalias method can 
# deliver sub ref's or closures to handle nearly
# anything.
#
# returning the exit status here makes avoiding
# phorkatosis simpler and ensures that the exit
# status gets written to the pidfile even if
# our parent dies while we are running.

sub runjob
{
	my $que = shift;
	my $run = shift;

	print "$$: $run" if $que->{verbose} > 1;

	# caller gets back the result of running the
	# code or of the system call.

	if( ref $run eq 'CODE' )
	{
		# the caller gets back whatever the code
		# reference returns. anything not false
		# gets printed (e.g., warning messages).
		#
		# exectution only aborts if the NUMERIC
		# value of the return is true -- returning
		# a text message will not abort execution.

		&$run;
	}
	else
	{
		# alwyas hands back a number, so the 
		# normal exit/signal extraction applies.

		system( $run );
	}
}

# called prior to job execution in all modes to determine if the 
# job seems runnable. test for existing pidfiles w/o exit status,
# etc.
#
# this can be overloaded on various O/S to use /proc/blah to 
# get more detailed in its checks. default is to croak on a
# pidfile w/o exit status.

sub precheck
{
	my $que = shift;
	my $job = shift;

	# this stuff only goes out of the verbosity is
	# at the "detail" level.

	my $verbose = $que->{verbose} > 1;

	my $pidfile =
		$que->{pidz}{$job} = $que->{alias}{rundir} . "/$job.pid";

	my $outfile =
		$que->{outz}{$job} = $que->{alias}{logdir} . "/$job.out";

	my $errfile =
		$que->{errz}{$job} = $que->{alias}{logdir} . "/$job.err";

	print "$$: Precheck: $job" if $verbose;

	# gets set to true in the if-block if the job is running.

	my $running = 0;

	# this gets set to true in the block below if we
	# are running in restart mode and the jobs exited
	# zero on the previous pass. setting it to zero here
	# avodis uninit variable warnings.

	$que->{skip}{$job} = 0;

	# any pidfile without an exit status implies a running job
	# and prevents further execution. in restart mode any file
	# with a zero exit status will be skipped.

	if( -s $pidfile )
	{
		open my $fh, '<', $pidfile or croak "$$: < $pidfile: $!";

		chomp( my @linz = <$fh> );

		close $fh;

		print "$$:	Pidfile:  $pidfile", @linz if $verbose;

		if( @linz == 3 )
		{
			# the job exited, check the status in case we
			# are running in restart mode. either way, the
			# caller gets back false since the jobs isn't
			# running any longer.

			# here we can decide not to run the job if it exited
			# zero.
			#
			# in restart mode we skip anything that exited zero.
			# otherwise we can just zero out the pidfile and 
			# keep going.

			print "$$: Completed: $job" if $verbose;

			if( $que->{restart} )
			{
				my( $pid, $cmd, $exit ) = @linz;

				# avoid numeric comparision, exit status of "dog"
				# would work then also...

				if( $exit eq '0' )
				{
					# no reason to re-run this job.
					# note: this is always reprinted.
					
					print "$$: Marking job for skip on restart: $job";

					$que->{skip}{$job} = CLEAN;
				}
				else
				{
					print "$$:	$job previous non-zero exit, will be re-run"
						if $verbose;
				}
			}
			else
			{
				print "$$: Not Running:  $job" if $verbose;
			}
		}
		elsif( @linz )
		{
			# preparing the queue when one of the scheduled
			# jobs is running "error".
			#
			# here's where the fun part starts. for now i'll
			# punt: anything without an exit status is assumed
			# to be running.

			print STDERR "\n$$:	Pidfile without exit: $job";

			$running = 1;
		}
	}
	elsif( -e $pidfile )
	{
		# assume the job is running. this may require some 
		# manual cleanup but avoids the logic race of a 
		# file being checked while the pid and run lines
		# are buffered.
		#
		# Note: on Solaris or Linux this could check things
		# via /proc or Unix::Process. Occams Razor tells me
		# to leave this alone until it proves to be a problem.

		print STDERR "\n$$:	Still running: empty $pidfile";

		$running = 1;
	}
	else
	{
		print "\n$$:	No pidfile: $job is not running" if $verbose;
	}

	# zero out the pid/log/err files if the job isn't running
	# at this point. leaving this down here makes it simpler
	# to update the block above if we have more than one
	# way to decide if things are still running.

	if( $que->{skip}{$job} || $running )
	{
		print "\n$$: Leaving existing pidfile untouched"
			if $verbose;
	}
	else
	{
		# after this point it seems likely that we can open the
		# the necessary files for write at runtime. leaving
		# them open here doesn't save much time and is a headache
		# if we are running in debug mode anyway.

		for my $path ( $pidfile, $outfile, $errfile )
		{
			open my $fh, '>', $path
				or croak "Failed writing empty $path: $!";
		}

	}

	# caller gets back true if we suspect that the job is 
	# still running, false otherwise.

	$running
}

# constructor.

sub prepare
{
	local $\ = "\n";
	local $, = "\n";
	local $/ = "\n";

	my $item = shift;

	# we either need one or an even number of arguments.

	croak "\nOdd number of arguments"
		if @_ > 1 && @_ % 2;

	my %argz = @_ > 1 ? @_ : ( sched => $_[0] );

	# writing the pidfiles out will overwrite any history
	# of the previous execution and make the queue un-
	# restartable for execution.

	croak "prepare called with both deubg and restart"
		if( $argz{debug} && $argz{restart} );

	# avoid processing a false schedule. this can happen via 
	# things like prepare( verbose => 1 ) or prepare();

	croak "Missing schedule list" unless $argz{sched};

	# convert a string schedule to an array referent.
	# at this point we assume that anyone passing an object
	# has a proper stringify overload for it.

	$argz{sched} = [ split /\n/, $argz{sched} ] unless ref $argz{sched};

	# the queue begins life empty. much of this won't be used until
	# execute is called but it's simpler to at least label the things
	# here.

	my $que =
		bless
		{
			queued	=> {},	# jobs pending execution.
			depend	=> {},	# inter-job dependencies.

			skip	=> {},	# see constants ABORT, CLEAN.
			jobz	=> {},	# $jobz{pid} = $job

			alias	=> {},	# $q->{alias}{tag} = value

			pidz	=> {},	# $que->{pidz}{$job} = path
			outz	=> {},	# stdout of forked jobs.
			errz	=> {},	# stderr of forked jobs.

		},
		ref $item || $item;

	# syntatic sugar, minor speedup.

	my $depend	= $que->{depend};
	my $queued	= $que->{queued};
	my $alias	= $que->{alias};

	# defaults avoid undef warnings.
	#
	# verbose displays decision information, non-verobse only
	# displays error messages.
	#
	# debug mode doesn't fork or run the commands. it's useful
	# for debugging dependency lists.
	#
	# perl debugger always runs in debug mode, debug mode
	# always runs verbose.
	#
	# $argz{verbose} overrides all other levels during
	# preparation, without an argument it's either 
	# 2 (set via $que->{debug}) or defaults to 0 (not much
	# output).
	#
	# verbose > 0 will display the input lines.
	# verobse > 1 additionally displays each alias/dependency
	# as it is processed.
	#
	# if nothing "verbose" is set in the schedule or arg's 
	# then debug mode runs in "progress" mode for verbose.
	#
	# restart sets a "skip this" flag for any jobs whose
	# pidfiles show a zero exit. this allows any dependencies
	# to be maintained w/o having to re-run the entire que
	# if it aborts.

	$que->{debug}   =	$alias->{debug} ? 1 :
						$argz{debug}	? 1 :
						$^P;

	$que->{verbose} =	$alias->{verbose}	? $alias->{verbose} :
						$argz{verbose}		? $argz{verbose} :
						$que->{debug}		? 1	:
						0;

	$que->{restart} = $argz{restart} ? 1 : 0;

	# we only generate output here if the que's verbosity
	# is above 1.

	my $verbose = $que->{verbose} > 1;

	# handle the dependency list, first step is to strip 
	# out comments and blank lines, we are then left with
	# valid dependencies.
	#
	# The only lines that really matter will have a ':' or '=' 
	# in them for "job : target" or "alias = assignment" entries.

	for( @{ $argz{sched} } )
	{
		s/#.*//;
		s/^\s+//;
		s/\s+$//;
	}

	my @linz = grep /[:=]/, @{ $argz{sched} }
		or croak "build_queue called with empty depend list.";

	print "$$: Preparing Schedule From:", @linz, ''
		if $verbose;

	# step 1: deal with information in the aliases.
	#
	# info lines are key = value pairs, assigned to a 
	# hash they are stored as-is in the object. these
	# are used for thing like run directories, restart
	# switches, job aliases.
	#
	# assigning the default values first allows anything
	# in the aliases to overwrite the defaults.
	#
	# dependency lines cannot have '=' in them, so a grep
	# on this character works.
	#
	# one case this does not handle gracefully is a "phony"
	# alias (e.g., "foo ="). these can be dealt with
	# by assigning a minimum-time executable (e.g., "foo = /bin/true").
	#
	# the dir's have to be present and have reasonable mods
	# or the rest of this is a waste, the maxjobs has to be
	# >= zero (with zero being unconstrained).
	#
	# the dirname trick allows soft-linking an executable into
	# multiple locations with the path delivering enough 
	# information to run the jobs properly for that verison 
	# of the code (e.g., via config file co-located or 
	# derived from the basename).

	%$alias =
	(
		%defaultz,
		map { ( split /\s*=\s*/, $_, 2 ) } grep /=/, @linz
	);

	$alias->{rundir} ||= $argz{rundir}	|| $ENV{RUNDIR} || dirname $0;
	$alias->{logdir} ||= $argz{logdir}	|| $ENV{LOGDIR} || dirname $0;

	print "$$: Aliases:", Dumper $alias
		if $verbose;

	croak "$$: Negative maxjob: $alias->{maxjob}" 
		if $alias->{maxjob} < 0;

	for( $alias->{rundir}, $alias->{logdir} )
	{
		print "$$: Checking: $_" if $verbose;

		-e		|| croak "Non-existant:  $_";
		-w _	|| croak "Un-writable:   $_";
		-r _	|| croak "Un-readable:   $_";
		-x _	|| croak "Un-executable: $_";
	}

	# if we are alive at this point then the required values
	# seem sane.

	# items without '=' are the sequence information, items
	# with '=' in them have already been dealt with above.

	print "$$: Starting rule processing" if $verbose;

	for( grep { ! /=/ } @linz )
	{
		my( $a, $b ) = map { [ split ] } split /[^:]:[^:]/, $_, 2;

		croak "$$: Bogus rule '$_' has no targets"
			unless $a;

		print "$$: Processing rule: '$_'\n"
			if $verbose;

		# step 1: validate the job status. this includes
		# checking if any are already running or if we
		# are in restart mode and the jobs don't need to
		# be re-run. 
		#
		# overloading to validate external influences (e.g.,
		# existing system resources or data files) should
		# be done here.
		
		# catch: it's a pain to add all of the dependencies
		# for a job as separate jobs (e.g., a: b c d requiring
		# separate stub entries b:, c: and d:).
		#
		# catch: some jobs don't depened on anything but still need
		# to be in the queue.
		#
		# fix for both: assign an emtpy hash to everything initially.
		# this keeps the grep { ! keys ... } happy and means that
		# runnable will find all of them.

		# Note: after this loop is done running precheck we should
		# have an empty file for each job. tracking the empty files
		# is a good way to know what's left in the queue if we
		# aren't running in verbose mode.

		for my $job ( @$a, @$b )
		{
			# skip files we've already checked.

			next if defined $queued->{$job};

			croak "$$: Unrunnable: $job" if $que->precheck( $job );

			$queued->{$job} = {};
		}

		# at this point every job in the rule has been
		# put where we can find it again. now to deal 
		# with the targets.

		for my $job ( @$a )
		{
			# sanity check:
			#
			#	does the job deadlock on itself?

			croak "$$: Deadlock dependency: $job depends on itself"
				if grep { $job eq $_ } @$b;

			# insert this job into the queue with a list of what
			# it depends on.
			#
			# the keys of %$queued are those jobs
			# still queued [hey!]. They are runnable when all of
			# the dependencies have been complated, i.e., the
			# hash %{ $queued->{job} } is empty.
			#
			# $depend->{$job} references an array of the other jobs
			# that will depend on $job completing. This is used to
			# quickly remove entries from $queued->{$anotherjob}
			# when $job completes.
			#
			# for all pratical purposes, keys %$queued is the
			# "queue" here.

			@{$queued->{$job}}{@$b} = ( (1) x @$b );

			push @{ $depend->{$_} }, $job for @$b;
		}

	}

	if( $verbose )
	{
		print join "$$: Jobs:", sort keys %$queued;
		print join "$$: Waiting for:", sort keys %$depend;
	}

	# quick sanity checks: is everything listed as a dependency
	# also a job and is there at least one job that has no 
	# dependencies (i.e., can be run to begin with)?

	# $queued->{$job} is defined if $job gets run.
	# $depend->{$job} is defined if something depends on $job.
	#
	# at this point $queued{$job} should be defined for 
	# keys %$depend or there is a dependency which never
	# will get run.

	if( my @unrun = grep { ! defined $queued->{$_} } sort keys %$depend )
	{
		croak join ' ', "\nSome dependencies do not get run:", @unrun;
	}

	# if there are no jobs ready for the first iteration then 
	# we won't get very far...

	if( my @initial =  ready $que )
	{
		print join "\t", "$$: Initial Job(s): ", @initial, "\n"
			if $verbose;
	}
	else
	{
		croak "Deadlocked schedule: No jobs are initially runnable.";
	}

	print "$$: Resuling queue:\n", Dumper $que, "\n"
		if $verbose;

	# if we are still alive at this point the queue looks 
	# sane enough to try.

	$que
}

# generate a deep copy of the original que object and 
# run it in debug mode. if the debug succeeds then 
# pass back the que object for daisy-chaining into
# execute, if not then return undef.
#
# Note: the deep copy is necessary to avoid dequeue
# and complete from consuming the queued and depend
# hashes w/in the que object.
#
# Side effect of this will be creating pidfiles for 
# all jobs showing a "debugging" line and non-zero 
# exit. This allows debug in and restart mode without 
# having to clean up the pidfiles by hand.

sub debug
{
	my $que = shift;

	eval
	{
		my $tmp = Dumper $que
			or die "Failed to generate tmp que for debug: $!";

		$tmp = eval $tmp
			or die "Failed eval of Dumped queue: $!";

		$tmp->{debug}	= 1;
		$tmp->{maxjob}	= 1;

		$tmp->execute
	};

	print STDERR "\n$$: Debug Failure: $@" if $@;

	# caller gets back original object for daisy-chaining or
	# undef (which will abort further execution).

	$@ ? undef : $que;
}

# do the deed.

sub execute
{
	local $\ = "\n";
	local $, = "";
	local $/ = "\n";
	local $| = 1;

	my $que = shift;

	# set the verbosity level. rather than test 
	# for $verbose > X a zillion times this sets
	# a few, hopefully more descriptive, var's.
	#
	# if nothing is set then skips due to abort, 
	# non-zero exits, forkaphobia, exec fails
	# and croaks are displayed.
	#
	# print_progress	=>	show jobs being forked, reaped, 
	#						jobs skipped on restart.
	# print_detail 		=>	show unalias results, exit status, 
	#						runnable jobs, job slot limitations.
	#
	# debug mode always runs w/ verbose == 2 (i.e., sets
	# $print_detail).

	my $print_progress	= $que->{verbose} > 0;
	my $print_detail	= $que->{verbose} > 1;

	# this intentionally goes to STDERR so that the
	# logs have a start/completion message in them
	# at least.

	if( $que->{debug} )
	{
		print STDERR "$$: Beginning Debugging";
		print "$$: Debugging:\n", Dumper $que
			if $print_detail;
	}
	else
	{
		print STDERR "$$: Beginning Execution";
	}

	# housekeeping: set run-specific variables to 
	# reasonable values.

	$que->{abort} = 0;

	# may have been tickled externally, localizing the value
	# here avoids screwing up the caller's settings.

	local $SIG{CHLD} = 'DEFAULT';

	# ignore HUP's while we are forking.

	local $SIG{HUP}	 = 'IGNORE';

	# use for logging messages to make sure that the jobs are
	# really run in parallel.

	my $t0 = time;

	# associate pids returned by wait with the jobs we forked.

	my $jobz = {};

	# nothing started, yet.
	# $curjob gets compared to $maxjob to decide how many
	# runnable jobs can be forked each time we compute the
	# runnable jobs list.

	my $maxjob	= $que->{alias}{maxjob};
	my $slots	= $maxjob;

	# things are runnable when the hash stored for them in the queue
	# has no dependencies remaining.
	#
	# sorting the list makes it a bit easier to track the execution
	# but shouldn't effect the outcome at all.
	#
	# notice that this forks all the jobs it can before testing
	# for another exit.
	#
	# note that since the execution of jobs is async. there may be
	# times when we have a queue with no runnable jobs. in this
	# case runnable returns nothing and we fall through to the
	# wait.
	#
	# the || %$jobz test handles the last few jobs, where the
	# queue is empty but we need to cycle through the wait
	# loop.

	while( $que || %$jobz )
	{
		if( my @runnable = $que->ready )
		{
			print join "\t", "$$: Runnable:", @runnable, "\n"
				if $print_detail;

			RUNNABLE:
			for my $job ( @runnable )
			{
				# if maxjob is set then throttle back the number of
				# runnable jobs to the number of available slots.
				# forking decrements $slots; exits increments it.

				if( $maxjob && ! $slots )
				{
					print "$$: No slots available: Unable to start runnable jobs\n"
						if @runnable && $print_detail;

					last RUNNABLE;
				}

				# expand the job entry from the dependency list 
				# into whatever actually gets passed to the shell.
				#
				# the second argument is the "firstpass" indicator,
				# which is zero now that we aren't calling from
				# the prepare method.

				my $run = $que->unalias( $job );

				print "$$: Unalias: $job => $run\n"
					if $print_detail;

				# open the pidfile first, better to croak here than
				# leave a job running w/o a pidfile.

				open my $fh, '>', $que->{pidz}{$job}
					or croak "$$: $job: $que->{pidz}{$job}: $!"
						unless $que->{skip}{$job} == CLEAN;

				# deal with starting up the job:
				#	don't fork in debug mode.
				# 	put an abort message into the pidifle in abort mode.
				#	otherwise fork-exec the thing.

				if( $que->{debug} )
				{
					# skip forking in the debugger, I have enough
					# problems already...

					print "Debugging: $job ($run)\n"
						if $print_progress;

					# make sure anyone who follows us knows about
					# the debug pass, don't store zero to avoid 
					# problems with restart mode.

					print $fh "$$\ndebug $job ($run)\n1";

					$que->dequeue( $job );
					$que->complete( $job );
				}
				elsif( my $reason = $que->{skip}{$job} )
				{
					# $que->{skip}{$job} is only set if $que->{restart}
					# or $que->{noabort} are set.
					#
					# process jobs that are runnable but marked for
					# skipping.  these include ones that completed
					# zero on the previous pass in restart mode or
					# depend on failed jobs in noabort mode. either way,
					# they don't effect the number of running jobs and
					# need to be purged from the queue before we decide
					# how many jobs can be run.

					if( $reason == CLEAN )
					{
						# job is being skipped due to re-execution in restart
						# mode and the job exiting zero on the preivous pass.
						#
						# the pidfile can be ignored since the previous exit
						# was clean.

						print "$$: Skipping $job ($run) on restart."
							if $print_progress;
					}
					elsif( $reason == ABORT )
					{
						# this means that a job this one depends on failed.
						# update the pidfile with an "aborted" message and
						# non-zero exit status then find the jobs that depend
						# on this one and update their skip values to ABORT
						# also. 
						#
						# %jobz doesn't get updated here: since nothing is forked
						# there isn't a pid to store anywhere.

						print "$$: Skipping $job ($run) on aborted prerequisite."
							if $print_progress;

						print $fh "Failed prerequisite in noabort mode\n-1";

						# first mark the jobs which depend on this one 
						# for abort, then dequeue and complete this one.
						# dequeueing this job will push 

						$que->{skip}{$_} = ABORT for $que->depend( $job );
					}
					else
					{
						die "$$: Bogus skip setting for $job: $reason";
					}

					# either way, we are done with this job.
					# any jobs left runnable here will be picked up on 
					# the next pass.

					$que->dequeue( $job );
					$que->complete( $job );
				}
				elsif( $que->{abort} )
				{
					# jobz doesn't get updated here: since nothing is forked
					# there isn't a pid to store anywhere. 
					#
					# we do have to update the pidfile, however, to show
					# that the job was aborted.

					print "$$: Skipping $job ($run) due to que abort.";

					print $fh "$$\nabort $job ($run)\n-1";

					$que->dequeue( $job );
					$que->complete( $job );
				}
				else
				{
					# we are actually going to run the job.

					# we have to remove the job from $que->{queued} immediately
					# since we may call runnable any number of times during
					# the job's execution.
					#
					# %jobz allows us to map the pid of an exited job onto
					# its key in the queue in order to clean up the 
					# dependencies on it.
					#
					# once the job is forked the pidfile contains two lines:
					# process id and command being run. 
					#
					# the file handle for this job gets a pid written to it
					# when the job is forked and an exit status appended at
					# the end. checking for zero exit status of existing 
					# pidfiles in the prepare would be a nice way to allow
					# for automatic restarts.

					if( my $pid = fork )
					{
						print $fh "$pid\n$run";

						$jobz->{$pid} = $job;

						$que->dequeue( $job );

						--$slots;

						print "$$: Forked $pid: $job\n"
							if( $print_detail );
					}
					elsif( defined $pid )
					{
						# remember to do this before closing stdout.

						print "$$: Executing: $job ($run)\n"
							if $print_detail;

						# child never reaches the point where this is
						# closed in the main loop.

						close $fh;

						# Note: make sure to exit w/in this block to
						# avoid forkatosis. the exec is effectively
						# an exit since it stops running this code.
						#
						# single-argument exec allows the shell
						# to expand any meta-char's in $run.
						#
						# braces avoid compiler warning about 
						# unreachable code on the croak.

						my $outpath = $que->{outz}{$job};
						my $errpath = $que->{errz}{$job};

						print "$$: $job: Output in $outpath"
							if $print_detail;

						print "$$: $job: Errors in $errpath"
							if $print_detail;

						open STDOUT, '>', $outpath or croak "$outpath: $!";
						open STDERR, '>', $errpath or croak "$errpath: $!";

						# do the deed, record the result and exit.
						# forcing the numeric context allows code
						# to return a warning or mesage without the
						# que aborting.
						
						my $exit = $que->runjob( $run );

						warn "$$: $job: $exit" if $exit;
						
						# the status and signal tests are guaranteed to
						# work for system (stringy $job) calls. if a sub
						# returns a string the += 0 will convert it to a
						# numeric zero.

						$exit += 0;

						if( my $stat = $exit >> 8 )
						{
							carp "$$: Non-zero return for $job: $stat";
						}
						elsif( my $signal = $exit & 0xFF )
						{
							carp "$$: $job stopped by signal: $signal";
						}
						else
						{
							print "$$: Completed $job" if $print_progress;
						}

						print $fh $exit;

						exit $exit;
					}
					else
					{
						# give up if we cannot fork a job. all jobs
						# after this will have pidfiles with a non-zero
						# exit appended to them and an abort message.

						print $fh -1;

						print STDERR "\nn$$: phorkafobia on $job: $!";

						$que->{abort} = 1;
					}
				}

				# parent closes the file handle here, regardless
				# of how the file was processed.
				#
				# test avoids problems if the file handle wasn't
				# opened (e.g., if the job was skipped).

				close $fh if $fh;
			}

		}
		elsif( %$jobz eq '0' )
		{
			# if nothing is available for execution then we'd
			# better have some jobs outstanding in the background
			# or the queue is deadlocked.

			print STDERR "\n$$: Deadlocked schedule: neither runnable nor pending jobs.\n";

			$que->{abort} = 1;
		}

		print STDERR "\n$$: Aborting queue due to errors.\n"
			if $que->{abort};

		# block for something to exit, convert the pid back into a 
		# job key remove this job from the dependency lists of 
		# whatever remains queued. 
		#
		# this should not be a while-loop, since that could
		# leave runnable jobs waiting for unrelated items
		# to start. since any one job might leave multiple
		# jobs runnable, we have to deal with the exits 
		# one at a time. the if-block also deals more 
		# gracefully with multiple jobs pending due to 
		# job slot limits, since any one job will always
		# leave one more job immediately runnable.
		#
		# note: since nothing gets run until all the dependencies 
		# have been removed this should never hit an undefined 
		# sub-hash in %queued.
		#
		# if there are no outstanding jobs then this will immediately
		# return -1 and we won't block.

		if( (my $pid = wait) > 0 )
		{
			my $status = $?;

			my $job = $jobz->{$pid}
				or die "$$: unknown pid $pid";

			print "$$: Exit: $job ($pid) $status\t$que->{pidz}{$job}\n"
				if $print_detail;

			open my $fh, '>>',  $que->{pidz}{$job}
				or croak "$que->{pidz}{$job}: $!";

			print $fh $status;

			close $fh;

			unless( $que->{noabort} )
			{
				$que->{abort} ||= $status;
			}
			else
			{
				# this will cascade to other jobs that depend on 
				# this one, forcing them to be skipped in the que.

				$que->{skip}{$job} = ABORT;
			}

			if( my $exit = $status >> 8 )
			{
				print STDERR "\n$$: $job Non-zero exit: $exit from $job\n";
				print STDERR "\n$$: Aborting further job startup.\n";
			}
			elsif( my $signal = $status & 0xF )
			{
				print STDERR "\n$$: $job Stopped by a signal: $signal\n";
				print STDERR "\n$$: Aborting further job startup.\n";
			}
			else
			{
				print "$$: Successful: $job ($pid).\n"
					if $print_progress;
			}

			$que->complete( $job );

			delete $jobz->{$pid};

			++$slots;

			print "$$: Pending Jobs: ", scalar values %$jobz, "\n"
				if( $print_detail );
		}
	}

	print STDERR $que->{debug} ?
		"$$: Debugging Completed." :
		"$$: Execution Completed.";

	0
}

# keep the use pragma happy

1

__END__

=head1 Name

Schedule::Depend

=head1 Synopsis

Single argument is assumed to be a schedule, either newline 
delimited text or array referent:

	my $q = Scheduler->prepare( "newline delimited schedule" );

	my $q = Scheduler->prepare( [ qw(array ref of schedule lines) ] );

Multiple items are assumed to be a hash, which much include the
"sched" argument.

	my $q = Scheduler->prepare( sched => "foo:bar", verbose => 1 );

Object can be saved and used to execute the schedule or the schedule
can be executed (or debugged) directly:

	$q->debug;
	$q->execute;

	Scheduler->prepare( sched => $depend)->debug;
	Scheduler->prepare( sched => $depend, verbose => 1  )->execute;

Since the deubgger returns undef on a bogus queue:

	Scheduler->prepare( sched => $depend)->debug->execute;

The "unalias" method can be safely overloaded for specialized
command construction at runtime; precheck can be overloaded in
cases where the status of a job can be determined easily (e.g.,
via /proc). A "cleanup" method may be provided, and will be
called after the job is complete.

See notes under "unalias" and "runjob" for how jobs are
dispatched. The default methods will handle shell code
sub names automatically.

=head1 Arguments

=over 4

=item sched

The dependencies are described much like a Makefile, with targets
waiting for other jobs to complete on the left and the dependencies
on the right. Schedule lines can have single dependencies like:

	waits_for : depends_on

or multiple dependencies:

	wait1 wait2 : dep1 dep2 dep3

or no dependencies:

	runs_immediately :

Which are unnecessary but can help document the code.

Dependencies without a wait_for argument are an error (e.g.,
": foo" will croak during prepare).

The schedule can be passed as a single argument (string or
referent) or with the "depend" key as a hash value:

	sched => [ schedule as seprate lines in an array ]

	or

	sched => "newline delimited schedule, one item per line";

It is also possible to alias job strings:

	foo = /usr/bin/find -type f -name 'core' | xargs rm -f

	...

	foo : bar

	...

will wait until bar has finished, unalias foo to the 
command string and pass the expanded version wholesale
to the system command.

See the "Schedules" section for more details.

=item verbose

Turns on verbose execution for preparation and execution.

All output controlled by verbosity is output to STDOUT;
errors, roadkill, etc, are written to STDERR.

verbose == 0 only displays a few fixed preparation and 
execution messages. This is mainly intended for production
system with large numbers of jobs where searching a large
output would be troublesome.

verbose == 1 displays the input schedule contents during
preparation and fork/reap messages as jobs are started.

verbose == 2 is intended for monitoring automatically
generated queues and debugging new schedules. It displays
the input lines as they are processed, forks/reaps, 
exit status and results of unalias calls before the jobs
are exec-ed.

verbose can also be specified in the schedule, with 
schedule settings overriding the args. If no verbose
setting is made then debug runs w/ verobse == 1,
execution with 0.

=item debug

Runs the full prepare but does not fork any jobs, pidfiles
get a "Debugging $job" entry in them and an exit of 1. This
can be used to test the schedule or debug side-effects of
overloaded methods. See also: verbose, above.

=item rundir & logdir

These are where the pidfiles and stdout/stderr of forked
jobs are placed, along with stdout (i.e., verbose) messages
from the que object itself.

These can be supplied via the schedule using aliases 
"rundir" and "logdir". Lacking any input from the schedule
or arguments all output goes into the #! file's directory
(i.e., dirname $0).

Note: The last option is handy for running code via soft link 
w/o having to provide the arguments each time. The RBTMU.pm
module in examples can be used in a single #! file, soft linked
in to any number of directories with various .tmu files and 
then run to load the varoius groups of files.


=head1 Description

Parallel scheduler with simplified make syntax for job 
dependencies and substitutions.  Like make, targets have
dependencies that must be completed before the can be run. 
Unlike make there are no statements for the targets, the targets
are themselves executables.

The use of pidfiles with status information allows running
the queue in "restart" mode. This skips any jobs with zero
exit status in their pidfiles, stops and re-runs or waits for
any running jobs and launches anything that wasn't started.
This should allow a schedule to be re-run with a minimum of
overhead.

The pidfile serves three purposes:

=over 4

=item Restarts

 	On restart any leftover pidfiles with
	a zero exit status in them can be skipped.

=item Waiting

 	Any process used to monitor the result of
	a job can simply perform a blocking I/O to
	for the exit status to know when the job
	has completed. This avoids the monitoring
	system having to poll the status.

=item Tracking

 	Tracking the empty pidfiles gives a list of
	the pending jobs. This is mainly useful with
	large queues where running in verbose mode 
	would generate execesive output.

=back

Each job is executed via fork/exec (or sub call, see notes
for unalias and runjob). The parent writes out a 
pidfile with initially two lines: pid and command line. It
then closes the pidfile. The child keeps the file open and
writes its exit status to the file if the job completes;
the parent writes the returned status to the file also. This
makes it rather hard to "loose" the completion and force an
abort on restart.

=head2 Schedules

The configuration syntax is make-like. The two sections
give aliases and the schedule itself. Aliases and targets
look like make rules:

	target = expands_to

	target : dependency

example:

	a = /somedir/abjob.ksh
	b = /somedir/another.ksh
	c = /somedir/loader

	a : /somedir/startup.ksh 
	b : /somedir/startup.ksh 

	c : a b

	/somedir/validate : a b c


Will use the various path expansions for "a", "b" and "c"
in the targets and rules, running /somedir/abjob.ksh only
after /somedir/startup.ksh has exited zero, the same for
/somedir/another.ksh. The file /somedir/loader
gets run only after both abjob.ksh and another.ksh are 
done with and the validate program gets run only after all
of the other three are done with.

The main uses of aliases would be to simplify re-use of 
scripts. One example is the case where the same code gets
run multiple times with different arguments:

	# comments are introduced by '#', as usual.
	# blank lines are also ignored.

	a = /somedir/process 1 
	b = /somedir/process 2 
	c = /somedir/process 3 
	d = /somedir/process 4 
	e = /somedir/process 5 
	f = /somedir/process 6 

	a : /otherdir/startup	# startup.ksh isn't aliased
	b : /otherdir/startup
	c : /otherdir/startup

	d : a b
	e : b c 
	f : d e

	cleanup : a b c d e f

Would allow any variety of arguments to be run for the 
a-f code simply by changing the aliases, the dependencies
remain the same.

Another example is a case of loading fact tables after the
dimensions complete:

	fact1 fact2 fact3 : dim1 dim2 dim3

Would load all of the dimensions at once and the facts
afterward. Note that stub entries are not required
for the dimensions, they are added as runnable jobs 
when the rule is read.

If the jobs unalias to the names of the que object's
methods then the code will be called instead of sending
the string through system. For example:

	job = /path/to/runscript
	foo = cleanup
	bar = cleanup
	xyz = cleanup

	job : ./startup
	foo bar xyz : job

Will run ./startup via system in the local directory,
run the job via system also then call $que->cleanup('foo'),
$que->cleanup('bar'), and $que->cleanup('xyz') in parallel
then finish (assuming they all exist, of course).

This allows the schedule to easily mix subroutine and 
shell code as necessary or convienent.

The final useful alais is an empty one, or the string
"PHONY". This is used for placeholers, mainly for 
breaking up long lines or assembling schedules 
automatically:

	waitfor =

	waitfor : job1
	waitfor : job2
	waitfor : job3
	waitfor : job4

	job5 job6 job7 : waitfor

	
will generate a stub that immediately returns
zero for the "waitfor" job. This allows the 
remaining jobs to be hard coded -- or the 
job1-4 strings to be long file paths -- without
having to generate huge lines or dynamicaly 
build the job5-7 line.

=head2 Overloading unalias for special job expansion.

Up to this point all of the schedule processing has been
handled automatically. There may be cases where specialized
processing of the jobs may be simpler. One example is where
the "jobs" are known to be data files being loaded into a 
database, another is there the subroutine calls must come
from an object other than the que itself.

In this case the unalias or runjob methods can be overloaded.
Because runjob will automatically handle calling subroutines
within perl vs. passing strings to the shell, most of the
overloading can be done in unalias.

If unalias returns a code referent then it will be used to
execute the code. One way to handle file processing for,
say, rb_tmu loading dimension files before facts would be
a schedule like:

	dim1 = tmu_loader
	dim2 = tmu_loader
	dim3 = tmu_loader
	fact1 = tmu_loader
	fact2 = tmu_loader

	fact2 fact1 : dim1 dim2 dim3

This would call $que->tmu_loader( 'dim1' ), etc, allowing
the jobs to be paths to files that need to be loaded.

The problem with this approach is that the file names can
change for each run, requiring more complicated code.

In this case it may be easier to overload the unalias 
method to process file names for itself. This might
lead to the schedule:

	fact2 fact1 : dim1 dim2 dim3

and nothing more with unalias deciding what to do 
with the files at runtime:

	sub unalias
	{
		my $que = shift;
		my $datapath = shift;

		my $tmudir  = dirname $0;

		my $filename = basename $datapath;

		my $tmufile = dirname($0) . '/' . basename($datapath) . '.tmu';

		-e $tmufile or croak "$$: Missing: $tmufile";

		# unzip zipped files, otherwise just redrect them 

		if( $datapath =~ /.gz$/ )
		{
			"gzip -dc $datapath | rb_ptmu $tmufile \$RB_USER"
		}
		else
		{
			"rb_tmu $tmufile \$RB_USER < $datapath"
		}

		# caller gets back one of the two command
		# strings
	}


In this case all the schedule needs to contain are 
paths to the data files being loaded. The unalias
method deals with all of the rest at runtime.

Adding a method to the derived class for more complicated
processing of the files (say moving the completed files 
to an archive area and zipping them if necessary) could
be handled by passing a closure:

	sub unalias
	{
		my $que = shift;
		my $datapath = shift;

		-e $datapath or croak "$$: Nonexistint data file: $datapath";

		# process the files, all further logic
		# is dealt with in the loader sub.

		sub { tmuload_method $datapath }
	}

Since code references are processed within perl this
will not be passed to the shell. It will be run in the
forked process, with the return value of tmuload_method
being passed back to the parent process.

Using an if-ladder various subroutines can be chosen
from when the job is unaliased (in the parent) or in 
the subroutine called (in the child).

=head2 Aliases can pass shell variables. 

Since the executed code is fork-execed it can contain any
useful environment variables also:

	a = process --seq 1 --foo=$BAR

will interpolate $BAR at fork-time in the child process (i.e..
by the shell handling the exec portion).

The scheduling module exports modules for managing the 
preparation, validation and execution of schedule objects.
Since these are separated they can be manipulated by the
caller as necessary.

One example would be to read in a set of schedules, run
the first one to completion, modify the second one based
on the output of the first. This might happen when jobs are
used to load data that is not always present.  The first
schedule would run the data extract/import/tally graphs.
Code could then check if the tally shows any work for the
intermittant data and stub out the processing of it by
aliasing the job to "/bin/true":

	/somedir/somejob.ksh = /bin/true

	prepare = /somedir/extract.ksh

	load = /somedir/batchload.ksh


	/somedir/somejob.ksh : prepare 
	/somedir/ajob.ksh : prepare 
	/somedir/bjob.ksh : prepare 

	load : /somedir/somejob.ksh /somedir/ajob.ksh /somedir/bjob.ksh


In this case /somedir/somejob.ksh will be stubbed to exit
zero immediately. This will not interfere with any of the
scheduling patterns, just reduce any dealays in the schedule.

=head2 Note on calling convention for closures from unalias.

The closures generated in unalias vary on their parameter
passing:

	$run = sub { $sub->( $job ) };				$package->can( $subname )

	$run = sub { $que->$sub( $job ) };			$que->can( $run )

	$run = sub { __PACKAGE__->$sub( $job ) };	__PACKAGE__->can( $run )

	$run = eval "sub $block";					allows perl block code.

The first case comes up because Foo::bar in a schedule
is unlikey to successfully process any package arguments.
The __PACKAGE__ situation is only going to show up in 
cases where execute has been overloaded, and the
subroutines may need to know which package context
they were unaliased.

The first case can be configured to pass the package
in by changing it to:

	$run = sub { $packge->$sub( $job ) };

This will pass the package as $_[0].

The first test is necessary because:

	$object->can( 'Foo::bar' )

alwyas returns \&Foo::bar, which called as $que->$sub
puts a stringified version of the object into $_[0],
and getting something like "2/8" is unlikely to be
useful as an argument.

The last is mainly designed to handle subroutines that
have multiple arguments which need to be computed at
runtime:

	foo = { do_this( $dir, $blah); do_that }

or when scheduling legacy code that might not exit 
zero on its own:

	foo = { some_old_sub(@argz); 0 }

The exit from the block will be used for the non-zero
exit status test in the parent when the job is run.


=head1 Notes on methods

Summary by subroutine call, with notes on overloading and
general use.

=head2 boolean overload

Simplifies the test for remaining jobs in execute's while
loop; also helps hide the guts of the queue object from
execute since the test reduces to while( $que ).

=head2 ready

Return a list of what is runnable in the queue. these
will be any queued jobs which have no keys in their 
queued subhash. e.g., the schedule entry

	"foo : bar"
	
leaves

	$queued->{foo}{bar} = 1.
	
foo will not be ready to excute until keys
%{$queued->{foo}} is false (i.e., $queued->{foo}{bar} 
is deleted in the completed module).

This is used in two places: as a sanity check of
the schedule after the input is complete and in
the main scheduling loop.

If this is not true when we are done reading the
configuration then the schedule is bogus. 

Overloading this might allow some extra control over
priority where maxjobs is set by modifying the sort
to include a priority (e.g., number of waiting jobs).

=head2 queued, depend

queued hands back the keys of the que's "queued" hash.
This is the list of jobs which are waiting to run. The
keys are sorted lexically togive a consistent return
value.

depend hands back the keys of que's "depend" hash for a
particular job. This is a list of the jobs that depend
on the job.

Only reason to overload these would be in a multi-stage
system where one queue depends on another. It may be useful
to prune the second queue if something abnormal happens
in the first (sort of like make -k continuing to compile).

Trick would be for the caller to use something like:

	$q1->dequeue( $_ ) for $q0->depend( $job_that_failed );

	croak "Nothing left to run" unless $q1;

note that the sort allows for priority among tags when
the number of jobs is limited via maxjob. Jobs can be
given tags like "00_", "01_" or "aa_", with hotter jobs
getting lexically lower tag values.

=head2 dequeue

Once a job has been started it needs to be removed from the
queue immediately. This is necessary because the queue may
be checked any number of times while the job is still running.

For the golf-inclined this reduces to

	delete $_[0]->{queued}{$_[1]}
	
for now this looks prettier.

Compare this to the complete method which is run after the
job completes and deals with pidfile and cleanup issues.

=head2 complete

Deal with job completion. Internal tasks are to update
the dependencies, external cleanups (e.g., zipping files)
can be handled by adding a "cleanup" method to the queue.

Thing here is to find all the jobs that depend on whatever
just got done and remove their dependency on this job.

$depend->{$job} was built in the constructor via:

		push @{ $depend->{$_} }, $job for @dependz;

Which assembles an array of what depeneds on this job.
Here we just delete from the queued entries anything
that depends on this job. After this is done the runnable
jobs will have no dependencies (i.e., keys %{$q{queued}{$job}
will be an empty list).

A "cleanup" can be added for post-processing (e.g., gzip-ing
processed data files or unlinking scratch files). It will
be called with the que and job string being cleaned up after.

=head2 unalias, runjob

Expand an alias used in a rule, execute the
unaliased job. Default case is to look the tag up in
$que->{alias} and return either an alias or the 
original tag and exec the expanded string via the
current shell.

One useful alternative is to use dynamic expansion
of the tag being unaliased (e.g., the TMU example in
the main notes, above). Another is to expand the 
tag into a code reference via:

	sub unalias
	{
		my ($que,$job) = (shift,shift);

		no strict 'refs';
		my $sub = \&$job;
	}

or 

	my $sub = sub { handler $job };

to use a closure instead of various subroutine references.

This allows queueing subroutines rather than shell code.

runjob accepts a scalar to be executed, either via 
exec in the shell or a subroutine call. The default
is to exit with the return status of a subroutine
call or exec the shell code or die.

=head2 precheck

Isolate the steps of managing the pidfiles and 
checking for a running job.

This varies enough between operating systems that
it'll make for less hacking if this is in one
place or can be overridden.

This returns true if the pidfile contains the pid
for a running job. depending on the operating 
system this can also check if the pid is a copy 
of this job running.

If the pid's have simply wrapped then someone will
have to clean this up by hand. Problem is that on
Solaris (at least through 2.7) there isn't any good
way to check the command line in /proc.

On HP it's worse, since there isn't any /proc/pid.
there we need to use a process module or parse ps.

On solaris the /proc directory helps:

	croak "$$: job $job is already running: /proc/$dir"
		if( -e "/proc/$pid" );}

but all we can really check is that the pid is running,
not that it is our job.

On linux we can also check the command line to be sure
the pid hasn't wrapped and been re-used (not all that
far fetched on a system with 30K blast searches a day
for example).

Catch: If we zero the pidfile here then $q->debug->execute
fails because the file is open for append during the
execution and we get two sets of pid entries. The empty
pidfiles are useful however, and are a good check for 
writability.

Fix: deal with it via if block in execute.

=head2 prepare

Read the schedule and generate a queue from it.

Lines arrive as:

	job = alias expansion of job

or

	job : depend on other jobs

any '#' and all text after it on a line are stripped, regardless
of quotes or backslashes and blank lines are ignored.

Basic sanity checks are that none of the jobs is currently running,
no job depends on istelf to start and there is at least one job
which is inidially runnable (i.e., has no dependencies).

Caller gets back a blessed object w/ sufficient info to actually
run the scheduled jobs.

The only reason for overloading this would be to add some boilerplate
to the parser. The one here is sufficient for the default grammar,
with only aliases and dependencies of single-word tags.

Note: the "ref $item || $item" trick allows this to be used as
a method in some derived class. in that case the caller will get
back an object blessed into the same class as the calling
object. This simplifies daisy-chaining the construction and saves
the deriving class from having to duplicate all of this code in
most cases.

=head2 debug

Stub out the execution, used to check if the queue
will complete. Basic trick is to make a copy of the
object and then run the que with "norun" set.

This uses Dumper to get a deep copy of the object so that
the original queue isn't consumed by the debug process,
which saves having to prepare the schedule twice to debug
then execute it.

two simplest uses are:

	if( my $que = S::D->prepare( @blah )->debug ) {...}

or

	eval { S::D->prepare( @blah )->debug->execute }

depending on your taste in error handling.

=head2 execute

Actually do the deed. There is no reason to overload 
this that I can think of. 



=head1 Known Bugs

The block-eval of code can yield all sorts of oddities
if the block has side effects (e.g., exit()). This 
probably needs to be better wrapped. In any case, caveat
scriptor...

The eval also needs to be better tested in test.pl.

=head1 Author

Steven Lembark, Knightsbridge Solutions
slembark@knightsbridge.com

=head1 Copyright

(C) 2001-2002 Steven Lembark, Knightsbridge Solutions

This code is released under the same terms as Perl istelf. Please
see the Perl-5.6.1 distribution (or later) for a full description.

In any case, this code is release as-is, with no implied warranty
of fitness for a particular purpose or warranty of merchantability.

=head1 See Also

perl(1)

perlobj(1) perlfork(1) perlreftut(1) 

Other scheduling modules:

Schedule::Parallel(1) Schedule::Cron(1)

=cut
