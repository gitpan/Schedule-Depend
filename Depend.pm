# see DATA for pod.
# code best viewd with tabs set to 4 spaces.

########################################################################
# housekeeping
########################################################################

package Schedule::Depend;

our $VERSION = 0.24;

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

########################################################################
# examine the state of the que
########################################################################

# $que evalues to true if there is anything left to be run,
# e.g.,: while( $que ) {...}

use overload q{bool} => sub{ scalar %{ $_[0]->{queued} } };

# jobs that are ready to be run, these are any job for
# which keys %{ $que->{queued}{$job} } is false.
#
# the sort delivers jobs in lexical order -- makes
# debugging a bit easier and allows higher-priroty
# jobs to get run sooner if axjobs is set. derived
# classes might update the sort to deliver jobs with
# more waiting jobs or which were available soonest.

sub ready
{
	my $queued = $_[0]->{queued};

	sort grep { ! keys %{ $queued->{$_} } }  keys %$queued
}

# list of all jobs queued.

sub queued { sort keys %{ $_[0]->{queued} } }

########################################################################
# handle specific jobs
########################################################################

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

	# jobs can be shell paths, strip the directory to get
	# something valid.

	my $base = basename $job;

	my $pidfile =
		$que->{pidz}{$job} = $que->{alias}{rundir} . "/$base.pid";

	my $outfile =
		$que->{outz}{$job} = $que->{alias}{logdir} . "/$base.out";

	my $errfile =
		$que->{errz}{$job} = $que->{alias}{logdir} . "/$base.err";

	print "$$: Precheck: $job" if $verbose;

	# gets set to true in the if-block if the job is running.

	my $running = 0;

	# this gets set to true in the block below if we
	# are running in restart mode and the jobs exited
	# zero on the previous pass. setting it to zero here
	# avodis uninit variable warnings.

	$que->{skip}{$job} = 0;

	# phony jobs never execute, they are placeholders.

	return 0
		if defined $que->{alias}{job} && $que->{alias}{$job} eq 'PHONY';

	# any pidfile without an exit status implies a running job
	# and prevents further execution. in restart mode any file
	# with a zero exit status will be skipped.

	if( -s $pidfile )
	{
		open my $fh, '<', $pidfile or croak "$$: < $pidfile: $!";

		chomp( my @linz = <$fh> );

		close $fh;

		print "$$:	Pidfile:  $pidfile", @linz if $verbose;

		if( @linz >= 3 )
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
			print "$$: Previous status:", @linz if $que->{debug};

			if( $que->{restart} )
			{
				# take the last exit from the file -- the child
				# and parent both write the same thing to the
				# file so whichever the last one is will be
				# sufficient.
				#
				# since a sub return can be any string that
				# evaluates numerically to zero we have to
				# use a numeric comparison here for a valid test.

				my( $pid, $cmd, $exit ) = @linz[0,1,-1];

				if( $exit == 0 )
				{
					# no reason to re-run this job.
					# note: this is always printed.

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
	elsif( -e $pidfile && ! $que->{restart} )
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

########################################################################
# convert the job tag from the schedule into what gets run and run it.
# these are general fodder for overloading.
########################################################################

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
# generate the closre called in the child process:
#
# process $run into a sub referent if we can find
# a way:
#
#	phony aliases return a numerically false exit
#	string immediately.
#
#	code blocks are evaled into a sub -- note that
#	this leaves any variables in the block expressed
#	here, in this package.
#
# Note: from here down the sub's are called with an
# argument of the original job tag from the
# schedule. see the POD for examples of where
# this can be useful for dispatching multiple
# jobs through the same code.
#
#	check for fully qualified subnames -- this has
# 	to be done AFTER checking for code blocks, which
# 	may contain legit "::". since S::D may not have
# 	already use-ed the module, require it here --
#	ignore any error since the package may be
#
#	check if the que object has a method to handle
# 	the call.
#
#	check if the current package has a subroutine
#	name.
#
# caller gets back the anonymous sub referent or
# the contents of $run.

sub unalias
{
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

	# this goes into the pidfile to identify what
	# is being run. it will be the 2nd line of the file,
	# before the child (stringy) exit and parent (fork
	# return) exit lines.

	my $idstring = $job eq $run ? $job : "$job ($run)";

	# generate the closure.

	my $sub = 0;

	if( $run eq 'PHONY' || $run eq 'STUB' )
	{
		$sub = sub { $idstring }
	}
	elsif( $run =~ /^({.+})$/ )
	{
		$sub = eval "sub $run"
			or croak "$$: Invalid block for $job: $run";
	}
	elsif( my ($p,$s) = $run =~ /^(.+)::(.+?$)$/ )
	{
		eval "require $p";

		my $fqsub = $p->can( $s )
			or croak "$$: Bogus $job ($run): $p cannot $s";

		$sub = sub { $fqsub->( $job ) };
	}
	elsif( my $method = $que->can( $run ) )
	{
		$sub = sub { $que->$method( $job ) };
	}
	elsif( my $pkgsub = __PACKAGE__->can($run) )
	{
		$sub = sub { __PACKAGE__->$pkgsub( $job ) };
	}
	else
	{
		$sub = sub { $que->shellexec($run) };
	}

	croak "$$: Bogus unalias: no subroutine for $idstring"
		unless $sub;

	print "$$: $idstring ($sub)" if $que->{verbose};

	( $idstring, $sub )
}

# we know a bit more about the speicfics of
# the system call. putting this into its
# own method allows better reporting and
# saves the caller from having to figure
# out what a shell vs. subroutine return
# means.

sub shellexec
{
	my $que = shift;
	my $run = shift;

	# if anything goes wrong put a message
	# into the logfile and pass the non-zero
	# exit status up the food chain.

	if( system($run) == -1 )
	{
		# we failed to run the program,

		carp "$$: Failed system($run): $!";

		-1
	}
	elsif( my $stat = $? )
	{
		# system succeeded in running the
		# program but it failed during
		# execution.

		if( my $exit = $stat >> 8 )
		{
			carp "$$: Non-zero return for $run: $exit";
		}
		elsif( $stat == 128 )
		{
			carp "$$: Coredump from $run";
		}
		elsif( my $signal = $stat & 0xFF )
		{
			carp "$$: $run stopped by signal: $signal";
		}

		$?
	}
	else
	{
		print "$$: system($run) succeeded"
			if $que->{verbose} > 1;

		0
	}
}

# execute the job after the process has forked.
# overloading this is heavily tied to changes
# in unalias.
#
# returning the exit status here makes avoiding
# phorkatosis simpler and ensures that the exit
# status gets written to the pidfile even if
# our parent dies while we are running.

sub runjob
{
	my $que = shift;
	my $run = shift;

	# the caller gets back whatever the code
	# reference returns. anything not false
	# gets printed (e.g., warning messages).
	#
	# exectution only aborts if the NUMERIC
	# value of the return is true -- returning
	# a text message will not abort execution.
	#
	# the system closures will return a numeric
	# value w/ exit + signal.

	&$run
}

########################################################################
# few bits of information, saves extra hard-coding of structure info.
# these are mainly useful for generating sub-queues with the config
# values intact.
########################################################################
{ # isolate @statkeyz w/o havig to re-init it each time around

	my @statkeyz = 
	qw(
		restart
		noabort
		abort
		verbose
		debug
		maxjob
	);

	sub status
	{
		my $que = shift;

		my %statz = ();

		@statz{@statkeyz} = @{$que}{@statkeyz};

		$statz{alias} = { $que->alias };

		wantarray ? %statz : \%statz
	}
}

# get a copy of the whole thing or just one entry.
# note that the %{...} mess avoids external code
# mucking around with the values -- none of the alias
# entries are references themselves.

sub alias
{
	my $que = shift;

	if( @_ )
	{
		@{ $que->{alias} }{@_}
	}
	else
	{
		%{ $que->{alias} }
	}
}

# single items.

sub restart	{ $_[0]->{restart} }
sub noabort	{ $_[0]->{noabort} }
sub abort	{ $_[0]->{abort} }

sub verbose	{ $_[0]->{verbose} }
sub debug	{ $_[0]->{debug} }

sub jobz	{ $_[0]->{jobz} }
sub pidz	{ $_[0]->{pidz} }

sub rundir	{ $_[0]->{alias}{rundir} }
sub logdir	{ $_[0]->{alias}{logdir} }

########################################################################
# constructor.
########################################################################

# this is meaningless unless the object used
# to access this method really is based on S::D.
# in which case, the only thing useful to bless
# the results into is ref $que.

sub subque
{
	my $que = shift;

	croak "$$: Bogus subque: argument not a que referent"
		unless $que->isa( __PACKAGE__ );

	croak "$$: Bogus subque: no schedule argument"
		unless @_;

	my %argz = @_ > 1 ? @_ : ( sched => shift );

	$argz{subque} = 1;

	# caller gets back a que

	$que->prepare( %argz )
}

sub prepare
{
	local $\ = "\n";
	local $, = "\n";
	local $/ = "\n";

	# this may be a package, or a blessed object.
	# object call may be used to overload the prepare 
	# symbol or to generate a subque.

	my $item = shift;

	# validate & sanitize the arguments
	#
	#	either need one or an even number of arguments.
	#
	#	won't do much good trying to create a sub-que if
	# 	$item isn't already a que.
	#
	#	writing the pidfiles out will overwrite any history
	#	of the previous execution and make the queue un-
	#	restartable for execution.
	#
	#	avoid processing a false schedule. this can happen via
	#	things like prepare( verbose => 1 ) or prepare();
	#
	#	convert a string schedule to an array referent.
	#	at this point we assume that anyone passing an object
	#	has a proper stringify overload for it.
	#
	# 	won't do much good trying to create a sub-que 

	croak "\nOdd number of arguments"
		if @_ > 1 && @_ % 2;

	my %argz = @_ > 1 ? @_ : ( sched => $_[0] );

	croak "$$: Bogus prepare: cannot prepare a subque without a que"
		unless ! $argz{subque} || $item->isa( __PACKAGE__ );

	croak "prepare called with both deubg and restart"
		if( $argz{debug} && $argz{restart} );

	croak "Missing schedule list" unless $argz{sched};

	$argz{sched} = [ split /\n/, $argz{sched} ] unless ref $argz{sched};

	# deal with subque by populating a hash with the things
	# that need to be carried over.

	my %oldque = ();

	if( $argz{subque} )
	{
		my @keyz =
		qw(
			verbose
			debug

			restart
			noabort

			alias

			skip
		);

		$oldque{$_} = $item->{$_}
			or croak "$$: Bogus que ($item): missing $_"
				for ( @keyz );
	}

	# much of this won't get used until runtime, having
	# it all in one place simplifies life, however.

	my %newque = 
	(
		# defined by the schedule.

		queued	=> {},	# jobs pending execution.
		depend	=> {},	# inter-job dependencies.
		phony	=> {},	# list of phony jobs

		alias	=> {},	# $q->{alias}{tag} = value

		skip	=> {},	# see constants ABORT, CLEAN.

		# bookkeeping of forked proc's.

		jobz	=> {},	# $jobz{pid} = $job
		pidz	=> {},	# $que->{pidz}{$job} = $job.pid path
		outz	=> {},	# $que->{pidz}{$job} = $job.out path
		errz	=> {},	# $que->{pidz}{$job} = $job.err path
	);

	# pull in any existing definitions into the que here.
	#
	# from this point onward the construction of a que and
	# a sub-que are the same.

	my $que = bless { %newque, %oldque }, ref $item || $item;

	# syntatic sugar, minor speedup.

	my $depend	= $que->{depend};
	my $queued	= $que->{queued};
	my $phony	= $que->{phony};
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

	unless( defined $que->{debug} )
	{

		$que->{debug}   =	$alias->{debug} ? 1 :
							$argz{debug}	? 1 :
							$^P;

		$que->{verbose} =	$alias->{verbose}	? $alias->{verbose} :
							$argz{verbose}		? $argz{verbose} :
							$que->{debug}		? 1	:
							0;

		$que->{restart} = $argz{restart} ? 1 : 0;
	}

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
	# or the rest of this is a waste, the maxjob has to be
	# >= zero (with zero being unconstrained).
	#
	# the dirname trick allows soft-linking an executable into
	# multiple locations with the path delivering enough
	# information to run the jobs properly for that verison
	# of the code (e.g., via config file co-located or
	# derived from the basename).
	#
	# setting a non-zero maxjob to zero could cause major
	# pain on most systems. hence the ||= and if. basically
	# this limits the arguments to adjusting zero maxjobs
	# to values that are nonzero.

	%$alias =
	(
		%defaultz,
		map { ( split /\s*=\s*/, $_, 2 ) } grep /=/, @linz
	);

	$alias->{rundir} ||= $argz{rundir}	|| $ENV{RUNDIR}
		or croak "$$: missing rundir";

	$alias->{logdir} ||= $argz{logdir}	|| $ENV{LOGDIR}
		or croak "$$: missing logdir";

	$alias->{maxjob} ||= $argz{maxjob};

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
		# avoids problems with empty rules ending in : not having
		# a [^:] to parse on the end.

		s/:$/: /;

		my( $a, $b ) = map { [ split ] } split /[^:]:[^:]/, $_, 2;

		croak "$$: Bogus rule '$_' has no targets"
			unless $a;

		print "$$: Processing rule: '$_'\n"
			if $verbose;

		croak "$$: Bogus parse, contains multiple ':': $_"
			if grep /:/, (@$a, @$b );

		# step 1: validate the job status. this includes
		# checking if any are already running or if we
		# are in restart mode and the jobs don't need to
		# be re-run.
		#
		# overloading to validate external influences (e.g.,
		# existing system resources or data files) should
		# be done here.
		#
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
		#
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
		#
		# sanity check: does the job deadlock on itself?
		#
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

		for my $job ( @$a )
		{
			croak "$$: Deadlock dependency: $job depends on itself"
				if grep { $job eq $_ } @$b;

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

########################################################################
# process the que.
########################################################################

# copy the que and run it once through to determine if there
# are any deadlock or unalis issues. the copy is necessary to
# avoid the debug operation "consuming" all of the queued
# entires.
#
# note that this may have subtle effects in cases where unalias
# has side-effects as it runs (e.g., updating global variables).
#
# if the copied object successfully empties itself then the
# original que object is returned. this allows for:
#
#	eval { S::D->prepare(%argz)->debug->execute };
#
# to debug and run the que in one pass since the debug will
# abort execution by returning undef if it fails.

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

	# after execute has been called once the que cannot be
	# re-run since the queued array has been consumed.

	croak "$$: Bogus execute: Nothing to run" unless $que;

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

	# use for logging messages to make sure that the jobs are
	# really run in parallel.

	my $t0 = time;

	# associate pids returned by wait with the jobs we forked.

	my $jobz = {};

	# housekeeping: set run-specific variables to
	# reasonable values.

	$que->{abort} = '';

	# may have been tickled externally, localizing the value
	# here avoids screwing up the caller's settings.

	local $SIG{HUP}		= 'IGNORE';
	local $SIG{CHLD}	= 'DEFAULT';

	local $SIG{INT}		= sub { $que->{abort} = "Scheduler aborted by SIGINT" };
	local $SIG{QUIT}	= sub { $que->{abort} = "Scheduler aborted by SIGQUIT" };

	# blow off anything listed as running and mark
	# the que as aborting. the normal process of
	# reaping child proc's will label the pidfiles
	# with a non-zero exit.

	local $SIG{TERM} =
	sub
	{
		print STDERR "$$: Killing running jobs on sigterm";

		$que->{abort} = "Parent process ($$) zapped by SIGTERM";

		kill TERM => keys %$jobz if %$jobz;
	};

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

				my ( $substring, $sub ) = $que->unalias( $job );

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

					print "Debugging: $substring\n"
						if $print_progress;

					# make sure anyone who follows us knows about
					# the debug pass, don't store zero to avoid
					# problems with restart mode.

					print $fh "$$\ndebug $substring\n1";

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

						print "$$: Skipping $substring on restart."
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

						print "$$: Skipping $substring on aborted prerequisite."
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
				elsif( $reason = $que->{abort} )
				{
					# jobz doesn't get updated here: since nothing is forked
					# there isn't a pid to store anywhere.
					#
					# we do have to update the pidfile, however, to show
					# that the job was aborted.

					print "$$: Skipping $substring due to: $reason";

					print $fh "$$\nAbort: $reason: $substring\n-1";

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
						print $fh "$pid\n$substring";

						$jobz->{$pid} = $job;

						$que->dequeue( $job );

						--$slots;

						print "$$: Forked $pid: $job\n"
							if( $print_detail );
					}
					elsif( defined $pid )
					{
						# parent will propagate SIGTERM, normal
						# reaping cycle will mark this with a
						# non-zero exit in the pidfile.

						local $SIG{TERM} = 'DEFAULT';

						# remember to do this before closing stdout.

						print "$$: Executing: $substring\n"
							if $print_detail;

						# Note: make sure to exit w/in this block to
						# avoid forkatosis. the exec is effectively
						# an exit since it stops running this code.
						#
						# single-argument exec allows the shell
						# to expand any meta-char's in $sub.
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
						#
						# printing the string to the file handle
						# allows subroutines to return more useful
						# completion messages -- so long as they
						# evaluate to zero.
						#
						# Note: child never reaches the point where $fh
						# is closed in the main loop.

						my $result = $que->runjob( $sub );

						print $fh $result;

						close $fh;

						# avoid logging nastygrams about non-numeric
						# values in the + 0 step.

						no warnings;

						my $exit = $result + 0;

						warn "$$: $job: $exit" if $exit;

						exit $exit;
					}
					else
					{
						# give up if we cannot fork a job. all jobs
						# after this will have pidfiles with a non-zero
						# exit appended to them and an abort message.

						print $fh -1;

						print STDERR "\nn$$: phorkafobia on $job: $!";

						$que->{abort} = "Phorkaphobia at $job";
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
			print STDERR "\n$$: Remaining jobs:", $que->queued;

			$que->{abort} = 'Deadlock';
		}

		print STDERR "\n$$: Aborting queue due to $que->{abort}.\n"
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
		# if there are no outstanding jobs then wait() immediately
		# return -1 and won't block.

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

			if( $status )
			{
				print STDERR "\n$$: $job Non-zero exit: $status\n";

				if( $que->{noabort} )
				{
					# this will cascade to other jobs that depend on
					# this one, forcing them to be skipped in the que.
					#
					# note that $que->{abort} doesn't get updated
					# here, since the que isn't aborting, just the
					# dependencies for this job.

					print STDERR "\n$$: Cascading abort skip to dependent jobs";

					$que->{skip}{$job} = ABORT;
				}
				else
				{
					print STDERR "\n$$: Aborting further job startup.\n";

					$que->{abort} = 'Nonzero exit';
				}
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

	# avoid running the que multiple times. simpler to
	# catch this at the beginning since it has fewer
	# side effects.

	$que->{executed} = 1;

	0
}

# keep the use pragma happy

1

__END__

=pod

=head1 Name

Schedule::Depend

=head1 Synopsis

Single argument is assumed to be a schedule, either newline
delimited text or array referent:

	my $que = Scheduler->prepare( "newline delimited schedule" );

	my $que = Scheduler->prepare( [ qw(array ref of schedule lines) ] );

Multiple items are assumed to be a hash, which much include the
"sched" argument.

	my $que = Scheduler->prepare( sched => "foo:bar", verbose => 1 );

Object can be saved and used to execute the schedule or the schedule
can be executed (or debugged) directly:

	$que->debug;
	$que->execute;

	Scheduler->prepare( sched => $depend)->debug;
	Scheduler->prepare( sched => $depend, verbose => 1  )->execute;

Since the deubgger returns undef on a bogus queue:

	Scheduler->prepare( sched => $depend)->debug->execute;

The "unalias" method can be safely overloaded for specialized
command construction at runtime; precheck can be overloaded in
cases where the status of a job can be determined easily (e.g.,
via /proc). A "cleanup" method may be provided, and will be
called after each job is complete if $que->can( "cleanup" ).

See notes under "unalias" and "runjob" for how jobs are
dispatched. The default methods will handle shell code
sub names automatically.

=head1 Arguments

=over 4

=item sched

The schedule can be passed as a single argument (string or
referent) or with the "depend" key as a hash value:

	sched => [ schedule as seprate lines in an array ]

	sched => "newline delimited schedule, one item per line";

Or can be passed a hash of configuration information with
the required key "sched" having a value of the schedule
scalar described above.


The dependencies are described much like a Makefile, with targets
waiting for other jobs to complete on the left and the dependencies
on the right. Schedule lines can have single dependencies like:

	waits_for : depends_on

or multiple dependencies:

	wait1 wait2 : dep1 dep2 dep3

or no dependencies:

	runs_immediately :

The last being unnecessary but can help document the code.

Dependencies without a wait_for argument are an error (e.g.,
": foo" will croak during prepare).

It is also possible to alias job strings:

	foo = /usr/bin/find -type f -name 'core' | xargs rm -f

	...

	foo : bar

	...

will wait until bar has finished, unalias foo to the
command string and pass the expanded version wholesale
to the system command. Aliases can include fully qualified
perl subroutines (e.g., " Foo::Bar::subname") or methods
accessable via the $que object (e.g., "subname"), code 
blocks (e.g., "{returns_nonzero; 0}". If no subroutine,
method or perl block can be extracted from the alias then
it is passed to the "shellexec" method as a string.

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
non-debug execution with  verbose == 0.

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

=item maxjob

This is the maximum number of concurrnet processs that
will be run at any one time during the que. If more jobs
are runnable than process slots then jobs will be started
in lexical order by their name until no slots are left.

=item restart, noabort

These control the execution by skipping jobs that have
completed or depend on those that have failed.

The restart option scans pidfiles for jobs which have
a zero exit in them, these are marked for skipping on
the next pass. It also ignores zero-sized pidfiles to
allow for restarts without having to remove the initail
pidfiles created automatically in prepare.

The noabort option causes execution to behave much like
"make -k": instead of aborting completely on a non-zero
exit the execution will complete any jobs that do not
depend on the failed job.

Combining noabort with restart can help debug new
schedules or handle balky ones that require multiple
restarts.

These can be given any true value; the default for
both is false.


=item Note on schedule arguments and aliases

verbose, debug, rundir, logdir, and maxjob can all be
supplied via arguments or within the scheule as aliases
(e.g., "maxjob = 2" as a scheule entry). Entries hard-
coded into the schedule override those supplied via the
arguments. This was done mainly so that maxjob could be
used in test schedules without risk of accidentally bringing
a system to its knees during testing. Setting debug in this
way can help during testing; setting verbose to 0 on
automatically generated queues with thousands of entries
can also be a big help.

Hard-coding "restart" would require either a new
directory for each new execution of the schedule or
explicit cleanup of the pidfiles (either by hand or
a final job in the schedule).

Hard-codding "noabort" is probably harmless.

Hard-coding "debug" will effectively disable any real
execution of the que.

=back

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

A job can be assigned a single alias, which must be on a 
single line of the input schedule (or a single row in 
schedleds passed in as arrays). The alias is expanded at
runtime to determine what gets dispatched for the job.

The main uses of aliases would be to simplify re-use of
scripts. One example is the case where the same code gets
run multiple times with different arguments:

	# comments are introduced by '#', as usual.
	# blank lines are also ignored.

	a = /somedir/process 1	# process is called with various arg's
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

If the alias for a job is a perl subroutine call then the
job tag is passed to it as the single argument. This 
simplifies the re-use above to:

	file1.gz = loadfile
	file1.gz = loadfile
	file1.gz = loadfile

	file1.gz file2.gz file3.gz : /some/dir/download_files


Will call $que->loadfile passing it "file1.gz" and so
on for each of the files listed -- afte the download_files
script exits cleanly.


Another example is a case of loading fact tables after the
dimensions complete:

	fact1	= loadfile
	fact2	= loadfile
	fact3	= loadfile
	dim1	= loadfile
	dim2	= loadfile
	dim3	= loadfile

	fact1 fact2 fact3 : dim1 dim2 dim3

Would load all of the dimensions at once and the facts
afterward. Note that stub entries are not required
for the dimensions, they are added as runnable jobs
when the rule is read.

Single-line code blocks can also be used as aliases.
One use of these is to wrap legacy code that returns
non-zero on success:

	a = { ! returns1; }

or

	a = { eval{returns1}; $@ ? 1 : 0 }

to reverse the return value or pass non-zero if the 
job died. The blocks can also be used for simple 
dispatch logic:

	a = { $::switchvar ? subone("a") : subtwo("a") }

allows the global $::switchvar to decide if subone
or subtwo is passed the argument. Note that the global
is required since the dispatch will be made within
the Schedule::Depend package.

Altering the package for subroutines that depend on
package lexicals can also be handled using a block:

	a = { package MyPackage; somesub }

Another alias is "PHONY", which is used for placeholder
jobs. These are unaliased to sub{0} and are indended 
to simplify grouping of jobs in the schedule:

	waitfor = PHONY

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

One example of phony jobs simplifying schedule generation
is loading of arbitrary files. A final step bringing the
database online for users could be coded as:

	online : loads

with lines for the loads added one by one as the files
are found:

	push @schedule, "loads : $path", "path = loadfile";

could call a subroutine "loadfile" for each of the paths
without the "online" operation needing to be udpated for
each path found.

The other standard alias is "STUB". This simply prints
out the job name and is intended for development where
tracking schedule execution is useful. Jobs aliased to
"STUB" return a closure "sub{print $job; 0}" and an id
string of the job tag.

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

and nothing more with

		-e $tmufile or croak "$$: Missing: $tmufile";

		# unzip zipped files, otherwise just redrect them

		my $cmd = $datapath =~ /.gz$/ ?
			"gzip -dc $datapath | rb_ptmu $tmufile \$RB_USER" :
			"rb_tmu $tmufile \$RB_USER < $datapath"
		;

		# caller gets back an id string of the file 
		# (could be the command but that can get a bit
		# long) and the closure that deals with the 
		# string itself.

		( $datapath, sub { shellexec $cmd } };
	}


In this case all the schedule needs to contain are
paths to the data files being loaded. The unalias
method deals with all of the rest at runtime.

Aside: This can be easily implemented by way of a simple
convention and one soft link. The tmu (or sqlldr) config.
files for each group of files can be placed in a single
directory, along with a soft link to the #! code that 
performs the load. The shell code can then use '.' for
locating new data files and "dirname $0" to locate the 
loader configuations. Given any reasonable naming convention
for the data and loader files this allows a single executable 
to handle mutiple data groups -- even multiple loaders --
realtively simply.




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

Remember that unalias returns two items, an id string and 
closure:

	my ( $substring, $sub ) = unalias $job;

The former is printed for error and log messages, the latter
is executed via &$sub in the child process.

The default closures vary somewhat in the arguments they
are passed for handling the job and how they are called:

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
priority where maxjob is set by modifying the sort
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

unalias is passed a single argument of a job tag and
returns two items: a string used to identify the job 
and a closure that executes it. The string is used for
all log and error messages; the closure executed via 
"&$sub" in the child process.

The default runjob accepts a scalar to be executed and 
dispatches it via "&$run". This is broken out as a 
separate method purely for overloading (e.g., for even
later binding due to mod's in unalias).

For the most part, closures should be capable of 
encapsulating any logic necessary so that changes to
this subroutine will not be necessary.


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

=head2 Alternate uses for S::D::unalias

This can be used as the basis for a general-purpose dispatcher.
For example, Schedule::Cron passes the command line directly
to the scheduler. Something like:

	package Foo;

	use Schedule::Cron;
	use Schedule::Depend;

	sub dispatcher
	{
		my $cmd = shift;

		if( my ( $name, $sub ) = Schedule::Depend->unalias($cmd) )
		{
			print "$$: Dispatching $name";

			&$sub;
		}
	}

permits cron lines to include shell paths, perl subs or
blocks:

	* * * * *	Some::Module::subname
	* * * * *	{ this block gets run  also }
	* * * * *	methodname

This works in part because unalias does a check for its
first argument being a refernce or not before attempting
to unalias it. If a blessed item has an "unalias" hash
within it then that will be used to unalias the job strings:

	use base qw( Schedule::Depend );

	my $blessificant = bless { alias => { foo => 'bar' } }, __PACKAGE__;

	my ( $string, $sub ) = $blessificant->unalias( $job );

will return a subroutine that uses the aliased strings
to find method names, etc.


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
if the block has side effects (e.g., exit()). The one-
line format also imposes some strict limits on blocks
for now.  In any case, caveat scriptor...

test.pl has a lot of catching up to do on the code. For
now it is best used to check for bugs in the handling of
basic schedule syntax than all of the unalias optins.

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
