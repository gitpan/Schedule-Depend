########################################################################
# housekeeping
########################################################################

package Schedule::Depend;

our $VERSION = 0.04;

# make messages output during the use phase prettier.

local $\ = "\n";
local $, = "\n\t";

use Carp;

use File::Basename;

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

# hard-coded defaults used in the constructor (prepare). 
# these can be overridden in the config file via aliases,
# e.g., "rundir = /tmp".
#
# using the PROJECT environment variable makes rooting
# the execution a bit simpler in test environments.

local $ENV{PROJECT} ||= '';

my %defaultz =
(
	rundir	=> $ENV{PROJECT} . '/var/run',
	logdir	=> $ENV{PROJECT} . '/var/log',

	# i.e., don't choke the number of parallel jobs.
	# change this to 1 for serial behavior.

	maxjob	=> 0,

	# don't run in debug mode by default. allows 
	# safety check for testing schedules.

	debug	=> 0,
);

########################################################################
# methods
########################################################################

# simplifies the test for remaining jobs in execute's while
# loop. also helps hide the guts of the queue object from
# execute since the test reduces to while( $que ).

use overload q{bool} => sub{ scalar %{ $_[0]->{queued} } };

# return a list of what is runnable in the queue. these
# will be any queued jobs which have no keys in their 
# queued subhash. e.g., "foo : bar" leaves
# $queued->{foo}{bar} = 1. foo will not be ready to
# excute until keys %{$queued->{foo}} is false.
#
# this is used in two places: as a sanity check of
# the schedule after the input is complete and in
# the main scheduling loop.
#
# if this is not true when we are done reading the
# configuration then the schedule is bogus. 

sub ready
{
	my $queued = $_[0]->{queued};

	sort grep { ! keys %{ $queued->{$_} } }  keys %$queued
}

# only reason to overload these would be in a multi-stage
# system where one queue depends on another. it may be useful
# to prune the second queue if something abnormal happens
# in the first (sort of like make -k continuing to compile).
#
# trick would be for the caller to use something like:
#
#	$q1->dequeue( $_ ) for $q0->depend( $job_that_failed );
#
#	croak "Nothing left to run" unless $q1;
#
# note that the sort allows for priority among tags when
# the number of jobs is limited via maxjob. jobs can be
# given tags like "00_", "01_" or "aa_", with hotter jobs
# getting lexically lower tag values.

sub queued { sort keys %{ $_[0]->{queued} } }

sub depend
{
	my $que = shift;
	my $job = shift;

	sort keys %{ $que->{depend}{$job} };
}

# once a job has been started it needs to be removed from the
# queue immediately. this is necessary because the queue may
# be checked any number of times while the job is still running.
# this is different than cleaning up after the job has completed.
#
# this reduces to delete $_[0]->{queued}{$_[1]} but for now 
# this looks prettier.

sub dequeue
{
	my $que = shift;
	my $job = shift;

	delete $que->{queued}{$job};
}

# deal with job completion. internal tasks are to update
# the dependencies, external cleanups (e.g., zipping files)
# can be handled by adding a "cleanup" method to the queue.
#
# thing here is to find all the jobs that depend on whatever
# just got done and remove their dependency on this job.
#
# $depend->{$job} was built in the constructor via:
#
#		push @{ $depend->{$_} }, $job for @dependz;
# 
# which assembles an array of what depeneds on this job.
# here we just delete from the queued entries anything
# that depends on this job. after this is done the runnable
# jobs will have no dependencies (i.e., keys %{$q{queued}{$job}
# will be an empty list).
#
# a "cleanup" can be added for post-processing (e.g., gzip-ing
# processed data files or unlinking scratch files). it will
# be called with the que and job string being cleaned up after.

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

# expand an alias used in a rule.
#
# this is done as a seprate method to simplify overriding
# it in derived classes -- where a specialized expansion
# rule could simplify the dependencies or allow dynamic
# expansion. the default case is to simply look in the 
# info hash for the alias and return whatever's there.
# a translator with more brains might want to stub out
# some execution or use variable paths for daily vs. 
# weekly code.

sub unalias
{
	my $que = shift;
	my $job = shift;

	$que->{alias}{$job} || $job;
}

# isolate the steps of managing the pidfiles and 
# checking for a running job.
#
# this varies enough between operating systems that
# it'll make for less hacking if this is in one
# place or can be overridden.
#
# this returns true if the pidfile contains the pid
# for a running job. depending on the operating 
# system this can also check if the pid is a copy 
# of this job running.
# 
# if the pid's have simply wrapped then someone will
# have to clean this up by hand. problem is that on
# Solaris (at least through 2.7) there isn't any good
# way to check the command line in /proc.
#
# on HP it's worse, since there isn't any /proc/pid.
# there we need to 
#
# on solaris:
#
#	croak "$$: job $job is already running: /proc/$dir"
#		if( -e "/proc/$pid" );}
#
# on linux we can also check the command line.
#
# on hp there isn't much we can do without using a 
# process module or parsing ps -elf.
#
# catch: if we zero the pidfile here then $q->debug->execute
# fails because the file is open for append during the
# execution and we get two sets of pid entries. the empty
# pidfiles are useful however, and are a good check for 
# writability.
# 
# fix: deal with it via if block in execute.

sub precheck
{
	my $que = shift;
	my $job = shift;

	my $verbose = $que->{verbose};

	my $pidfile =
		$que->{filz}{$job} = $que->{alias}{rundir} . "/$job.pid";

	print "\n$$: Precheck: $job" if $verbose;

	# gets set to true in the if-block if the job is running.

	my $running = 0;

	# this gets set to true in the block below if we
	# are running in restart mode and the jobs exited
	# zero on the previous pass. setting it to zero here
	# avodis uninit variable warnings.

	$que->{skip}{$job} ||= 0;

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

			print "$$: Completed: $job" if $que->{verbose};

			if( $que->{restart} )
			{
				my( $pid, $cmd, $exit ) = @linz;

				# avoid numeric comparision, exit status of "dog"
				# would work then also...

				if( $exit eq '0' )
				{
					# no reason to re-run this job.
					
					print "$$: Marking job for skip on restart: $job";

					$que->{skip}{$job} = 1;
				}
				else
				{
					print "$$:	$job previous non-zero exit, will be re-run";
				}
			}
			else
			{
				print "$$: Not Running:  $job" if $que->{verbose};
			}
		}
		elsif( @linz )
		{
			# here's where the fun part starts. for now i'll
			# punt: anything without an exit status is assumed
			# to be running.

			print STDERR "$$:	Pidfile without exit: $job";

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

		print STDERR "$$:	Still running: empty $pidfile";

		$running = 1;
	}
	else
	{
		print "$$:	No pidfile: $job is not running" if $verbose;
	}

	# zero out the pidfile if the job isn't running at 
	# this point. leaving this down here makes it simpler
	# to update the block above if we have more than one
	# way to decide if things are still running.

	if( $que->{skip}{$job} || $running )
	{
		print STDERR "\n$$: Leaving existing pidfile untouched\n";
	}
	else
	{
		open my $fh, '>', $pidfile
			or croak "Failed writing empty $pidfile: $!";
	}

	# caller gets back true if we suspect that the job is 
	# still running, false otherwise.

	$running
}

# read the schedule.
#
# lines arrive as:
#
#	job = alias expansion of job
#
# or
#
#	job : depend on other jobs
#
# any '#' and all text after it on a line are stripped, regardless
# of quotes or backslashes.
#
# blank lines are ignored.
#
# basic sanity checks are that none of the jobs is currently running,
# no job depends on istelf to start and there is at least one job
# which is inidially runnable (i.e., has no dependencies).
#
# caller gets back a blessed object w/ sufficient info to actually
# run the scheduled jobs.
#
# obviously the parser needs a bit more boilerplate, for now it'll
# have to do.
#
# Note: the "ref $item || $item" trick allows this to be used as
# a method in some derived class. in that case the caller will get
# an object back which is bless into the same class as the calling
# object. this simplifies daisy-chaining the construction and saves
# the deriving class from having to duplicate all of this code in
# most cases.

sub prepare
{
	local $\ = "\n";
	local $, = "\n";
	local $/ = "\n";

	my $item = shift;

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
			skip	=> {},	# used to skip jobs on restart.
			jobz	=> {},	# current list of running jobs.
			filz	=> {},	# pidfile paths, see pidfile sub.

			alias	=> {},	# default alias values

		},
		ref $item || $item;

	# syntatic sugar.

	my $depend	= $que->{depend};
	my $queued	= $que->{queued};
	my $alias	= $que->{alias};
	my $filz	= $que->{filz};

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
	# restart sets a "skip this" flag for any jobs whose
	# pidfiles show a zero exit. this allows any dependencies
	# to be maintained w/o having to re-run the entire que
	# if it aborts.

	$que->{debug}   =	$alias->{debug} ? 1 :
						$argz{debug}	? 1 :
						$^P;

	$que->{verbose} =	$argz{verbose}	? 1 : $que->{debug};

	$que->{restart} = $argz{restart} ? 1 : 0;

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

	print "\nPreparing Schedule From:", @linz, ''
		if $que->{verbose};

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
	# dependency (e.g., "foo ="). these can be dealt with
	# by assigning a minimum-time executable (e.g., "foo = /bin/true").
	#
	# the dir's have to be present and have reasonable mods
	# or the rest of this is a waste, the maxjobs has to be
	# >= zero (with zero being unconstrained).

	%$alias =
	(
		%defaultz,
		map { ( split /\s*=\s*/, $_, 2 ) } grep /=/, @linz
	);

	print STDERR "\n$$: Aliases:", Dumper $alias;

	croak "$$: Negative maxjob: $alias->{maxjob}" 
		if $alias->{maxjob} < 0;

	for( $alias->{rundir}, $alias->{logdir} )
	{
		print "$$: Checking: $_" if $que->{verbose};

		-e		|| croak "Non-existant:  $_";
		-w _	|| croak "Un-writable:   $_";
		-r _	|| croak "Un-readable:   $_";
		-x _	|| croak "Un-executable: $_";
	}

	# if we are alive at this point then the required values
	# seem sane.

	# items without '=' are the sequence information, items
	# with '=' in them have already been dealt with above.

	print "\n$$: Starting rule processing" if $que->{verbose};

	for( grep { ! /=/ } @linz )
	{
		my( $a, $b ) = map { [ split ] } split /:/, $_, 2;

		croak "$$: Bogus rule '$_' has no targets"
			unless $a;

		print "\n$$: Processing rule: '$_'\n" if $que->{verbose};

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

	if( $que->{verbose} )
	{
		print join "\n\t", "\n\nJobs:\n", sort keys %$queued;
		print join "\n\t", "\n\nWaiting for:\n", sort keys %$depend;
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
		print join "\t", "\n\nInitial Job(s): ", @initial, "\n"
			if $que->{verbose};
	}
	else
	{
		croak "Deadlocked schedule: No jobs are initially runnable.";
	}

	print "\n\nResuling queue:\n", Dumper $que, "\n"
		if $que->{debug};

	# if we are still alive at this point the queue looks 
	# sane enough to try.

	$que
}

# stub out the execution, used to check if the queue
# will complete. basic trick is to make a copy of the
# object and then run the que with "norun" set.
#
# this uses Dumper to get a deep copy of the object so that
# the original queue isn't consumed by the debug process.
# saves having to prepare the schedule twice to debug then
# execute it.
#
# two simplest uses are:
#
#	if( my $que = S::D->prepare( @blah )->debug ) {...}
#
# or
#
#	eval { S::D->prepare( @blah )->debug->execute }
#
# depending on your taste in error handling.

sub debug
{
	my $que = shift;

	my $tmp = eval ( $tmp = Dumper $que )
		or croak "Failed to generate tmp que for debug: $!";

	$tmp->{debug}	= 1;
	$tmp->{verbose}	= 99;	# guaranteed to be the max :-)
	$tmp->{maxjob}	= 1;

	eval { $tmp->execute };

	# caller gets back original object for daisy-chaining or
	# undef (which will abort further execution).

	$@ ? undef : $que;
}

# execute the scheduled jobs.

sub execute
{
	local $\ = "";
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

	print STDERR "\n$$: Debugging:\n", Dumper $que, "\n"
		if $que->{debug};

	# housekeeping: set run-specific variables to 
	# reasonable values.

	local $que->{abort} = 0;

	# may have been tickled externally, localizing the value
	# here avoids screwing up the caller's settings.

	local $SIG{CHLD} = 'DEFAULT';

	# use for logging messages to make sure that the jobs are
	# really run in parallel.

	my $t0 = time;

	# associate pids returned by wait with the jobs we forked.
	#
	# note: this probably belongs in the object, would allow
	# the complete method to clean it up on the way through.

	my $jobz = $que->{jobz};

	# nothing started, yet.
	# this gets compared to $que->{maxjob} to decide how many
	# runnable jobs can be forked.

	my $maxjob = $que->{alias}{maxjob};
	my $curjob = 0;

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
			# throttle back the number of runnable jobs to 
			# $que->{info}{maxjob}. we have maxjob - the number
			# of running proc's slots available for new startups.
			# simplest fix is to slice the extra off the end of 
			# @runnable.

			print join "\t", "\n$$: Runnable:", @runnable, "\n"
				if $print_detail;

			if( $maxjob && (my $slots = $maxjob - $curjob) > 0 )
			{
				# i.e., at $slots offset into array, splice off
				# everything that's left, leaving 0..$slots-1
				# in @runnable.
				
				if( $slots < @runnable )
				{
					splice @runnable, $slots, @runnable;

					if( $print_detail )
					{
						print "\n$$: Startup slots:    $slots\n";
						print "\n$$: Queue limited to: ", join "\t", @runnable, "\n";
					}
				}
			}
			elsif( $maxjob )
			{
				# we can't run anything if there aren't slots
				# avilable. wait for something to exit.

				print "\n$$: No slots available: Unable to start runnable jobs\n"
					if $print_detail;

				@runnable = ();
			}

			for my $job ( @runnable )
			{
				# expand the job entry from the dependency list 
				# into whatever actually gets passed to the shell.
				#
				# the second argument is the "firstpass" indicator,
				# which is zero now that we aren't calling from
				# the prepare method.

				my $run = $que->unalias( $job );

				print STDERR "\n$$: Unalias: $job => $run\n"
					if $print_detail;

				# open the pidfile first, better to croak here than
				# leave a job running w/o a pidfile.

				open my $fh, '>', $que->{filz}{$job}
					or croak "$$: $job: $que->{filz}{$job}: $!"
						unless $que->{skip}{$job};

				if( $que->{skip}{$job} )
				{
					# job is being skipped due to the queue running in restart
					# mode and the job exiting zero on the preivous pass.
					#
					# jobz doesn't get updated here: since nothing is forked
					# there isn't a pid to store anywhere.
					#
					# the pidfile also doesn't get updated since it
					# already contains enough information.

					print STDERR "\n\t\tSkipping $job ($run) on restart."
						if $print_progress;

					$que->dequeue( $job );
					$que->complete( $job );
				}
				elsif( $que->{debug} )
				{
					# skip forking in the debugger, I have enough
					# problems already...

					print "\nDebugging: $job ($run)\n";

					# make sure anyone who follows us thinks
					# the thing ran cleanly. allows multiple
					# passes in debug mode w/o manual file
					# cleanup.

					print $fh "$$\ndebug $job ($run)\n1\n";

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

					print STDERR "\n\t\tSkipping $job ($run) due to schedule abort.";

					print $fh "$$\nabort $job ($run)\n-1\n";

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

						++$curjob;

						if( $print_detail )
						{
							print STDERR "\n$$: Forked $pid: $job\n";
							print STDERR "\n$$: Queued Jobs: $curjob\n";
						}
					}
					elsif( defined $pid )
					{
						# child never reaches the point where this is
						# closed in the main loop.

						close $fh;

						# Note: make sure to exit w/in this block to
						# avoid forkatosis. the exec is effectively
						# an exit since it stops running this code.

						print "\n$$: Executing: $job ($run)\n"
							if $print_detail;

						# single-argument form allows the shell
						# to expand any meta-char's in $run.
						#
						# braces avoid compiler warning about 
						# unreachable code on the croak.

						{ exec $run }

						# if the block above works the parent will
						# get back the exit status of the exec-ed 
						# code. if not then we'll end up here...

						print STDERR "\n$$: Failed exec for $job: $!\n";

						$que->{abort} = 1;
					}
					else
					{
						# give up if we cannot fork a job. all jobs
						# after this will have pidfiles with a non-zero
						# exit appended to them and an abort message.

						print $fh -1;

						print STDERR "\n$$: phorkafobia on $job: $!\n";

						$que->{abort} = 1;
					}
				}

				# parent closes the file handle here, regardless
				# of how the file was processed.

				close $fh;
			}
		}
		elsif( %$jobz == 0 )
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

			print STDERR "\n$$: Exit: $job ($pid) $status\t$que->{filz}{$job}\n"
				if $print_detail;

			open my $fh, '>>',  $que->{filz}{$job}
				or croak "$que->{filz}{$job}: $!";

			print $fh "\n$status\n";

			close $fh;

			if( my $exit = $status >> 8 )
			{
				print STDERR "\n$$: $job Non-zero exit: $exit from $job\n";
				print STDERR "\n$$: Aborting further job startup.\n";

				$que->{abort} = 1;
			}
			elsif( my $signal = $status & 0xF )
			{
				print STDERR "\n$$: $job Stopped by a signal: $signal\n";
				print STDERR "\n$$: Aborting further job startup.\n";

				$que->{abort} = 1;
			}
			else
			{
				print STDERR "\n$$: Successful: $job ($pid).\n"
					if $print_progress;
			}

			$que->complete( $job );

			delete $jobz->{$pid};

			--$curjob;

			if( $print_detail )
			{
				print "\n$$: Current Jobs: $curjob\n";
				print "\n$$: Pending Jobs: ",
					scalar values %$jobz, "\n";
			}

		}
	}
	print "\n\n$$: The queue has been debugged successfully"
		if $que->{debug};

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
"depend" argument.

	my $q = Scheduler->prepare( depend => "foo:bar", verbose => 1 );

Object can be saved and used to execute the schedule or the schedule
can be executed (or debugged) directly:

	$q->debug;
	$q->execute;

	Scheduler->prepare( depend => $depend)->debug;
	Scheduler->prepare( depend => $depend, verbose => 1  )->execute;

Since the deubgger exits nonzero on a bogus queue:

	Scheduler->prepare( depend => $depend)->debug->execute;


The "unalias" method can be safely overloaded for specialized
command construction at runtime; precheck can be overloaded in
cases where the status of a job can be determined easily (e.g.,
via /proc). A "cleanup" method may be provided, and will be
called after the job is complete.


=head1 Arguments

=over 4

=item depend

The dependencies are described much like a Makefile, with targets
waiting for other jobs to complete on the left and the dependencies
on the right. Schedule lines can have single dependencies like:

	waits_for : depends_on

or multiple dependencies:

	wait1 wait2 : dep1 dep2 dep3

or no dependencies:

	runs_immediately :

Dependencies without a wait_for argument are an error.

The schedule can be passed as a single argument (string or
referent) or with the "depend" key as a hash value:


	depend => [ schedule as seprate lines in an array ]

	or

	depend => "newline delimited schedule, one item per line";

=item verbose

Turns on verbose execution for preparation and execution.

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

Each job is executed via fork/exec. The parent writes out a 
pidfile with initially two lines: pid and command line. It
then closes the pidfile.  After the parent detectes the child
process exiting the exit status is written to the file and
the file closed.

The pidfile serves three purposes:

 -	On restart any leftover pidfiles with
  	a zero exit status in them can be skipped.

 -	Any process used to monitor the result of
 	a job can simply perform a blocking I/O to
	for the exit status to know when the job
	has completed. This avoids the monitoring
	system having to poll the status.

 -	Tracking the empty pidfiles gives a list of
 	the pending jobs. This is mainly useful with
	large queues where running in verbose mode 
	would generate execesive output.


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

Overloading the "unalias" method to properly select the
shell comand for loading the files would leave this as
the entire schedule. An example overloaded method would
look like:

	sub unalias
	{
		my $que = shift;
		my $diskfile = shift;

		my $tmufile = "$tmudir/$diskfile.tmu";

		-e $tmufile or croak "$$: Missing: $tmufile";

		my $logfile = "$logdir/$diskfile.log";

		# hand back the completed tmu command.
		
		"rb_tmu $tmufile \$RB_USER < $diskfile > $logfile 2>&1"
	}

A more flexable unalias might decide if the file should
be unzipped and piped or simply redirected and whether
to zip the logfile as it is processed.

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

=head1 Known Bugs

Running $q->debug then $q->execute( ... restart => 1 ) will 
result in nothing being executed. The restart option will
check, find that all of the 

=head1 Author

Steven Lembark, Knightsbridge Solutions
slembark@knightsbridge.com

=head1 Copyright

(C) 2001-2002 Steven Lembark, Knightsbridge Solutions

This code is released under the same terms as Perl istelf. Please
see the Perl-5.6.1 distribution (or later) for a full description.

In any case, this code is release as-is, with no implied warranty
of fitness for a particular purpose or warranty of merchantability.

=head1 SEE ALSO

perl(1).


=cut
