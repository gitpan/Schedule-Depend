########################################################################
#
# execution engine for Schedule::Depend schedules.
#
# this acutally Does The Deed by calling S::D::prepare and
# S::D::execute. this is also where the command line gets
# processed -- overrides to Defaults are handled here.
#
# runsched bridges the caller's procedural access and the
# OO interface used for S::D's handling of perly targets.
#
# wierd as it may look, this simplifies things by having 
# all of S::D and the varous action-item modules as base
# classes. That way a single object can execute the schedule
# and all of the various sub's that handle the tasks. pushing
# this into a class simplifies the #! code since it does not
# have to duplicate all of this garbage for each test harness
# or variation on the execution.
#
########################################################################

########################################################################
# housekeeping
########################################################################

package Schedule::Depend::Execute;

our $VERSION=0.90;

use strict;
use warnings;

use FindBin::libs;

use Carp;
use Cwd qw( &abs_path );
use File::Basename qw( &dirname );

use Getopt::Long;
use Pod::Usage;

# these have to be on @INC for S::D to find
# them as methods of the que.
#
# Note: the que object ends up blessed into this class,
# not S::D.

use base
qw(
	Schedule::Depend
	Schedule::Depend::Config
	Schedule::Depend::Utilities
);

# push the sub's into this space, necessary for 
# $que->method calls to work.

BEGIN
{
	Schedule::Depend::Utilities->import;
}

########################################################################
# execute object ties the schedule and its methods togeher.
# the only entry point is "runsched".
#
# the global setting is used to determine which
# entries in the defaults hash are merged with 
# module-specific entries.

sub import
{
	my $caller = caller;

	no strict 'refs';

	*{ $caller . '::runsched' } = \&runsched;
}

########################################################################
# schedule deals with the command line for itself,
# along with actually running the jobs.
#
# runsched just wraps schedule in an eval so that the caller
# doesn't have to deal with exception handling.
########################################################################

my @optionz =
qw(
	verbose+
	debug+

	prefix=s

	restart!
	force!
	abort!

	nofork!

	ttylog!

	group!

	help|?!
);

my $cmdline = {};

########################################################################
# set default options in $cmdline

sub default
{
	$cmdline->{$_[0]} = $_[1] unless defined $cmdline->{$_[0]}
}

########################################################################
# generate and execute a que based on the input schedule.
#
# "runsched" wraps this in an eval for exception handling
# and notification.

sub runsched
{
	# avoid wrapping the caller in an eval.

	my $caller = caller;

	# deal with any command line options.
	# pod2usage dumps out a summary or verbose
	# usage message if getopt fails or the 
	# "help" option is used.

	unless( GetOptions($cmdline, @optionz) )
	{
		pod2usage "$0: invalid command line argument"
	}
	elsif( $cmdline->{help} )
	{
		$cmdline->{verbose} ||= 1;

		pod2usage
			'-verbose' => $cmdline->{verbose},
			'-exitval' =>  1,
			$0
	}

	%Schedule::Depend::Execute::defaults
		or die "Bogus gensched: %Schedule::Depend::Execute::defaults is false";

	my $defaults = \%Schedule::Depend::Execute::defaults;

	# won't get far without this...

	$cmdline->{sched} = shift || $defaults->{sched}
		or die "Bogus gensched: no schedule";

	# scratch areas used for S::D are under the 
	# Bin dir's sibling "var" directory. these
	# inclue $Bin/var/[tmp|run|log].

	my $prefix =
		$cmdline->{prefix} ||= abs_path "$FindBin::Bin/../var";

	@{$cmdline}{ qw(prefix tmpdir rundir logdir) } =
	(
		$prefix,

		$prefix . '/tmp',
		$prefix . '/run',
		$prefix . '/log',
	);

	checkdir @{$cmdline}{qw( prefix rundir logdir tmpdir )};

	# set defaults for cmdline values.
	#
	# if these are not in @optionz then they will 
	# always be set.

	default $_, 1 for qw( abort maxjob verbose );

	# the defaults for configuring by caller are either
	# the first part of the caller's pacakge space or 
	# a value specified in defaults as the "global_key".

	my $global =
		$defaults->{global_key} || ( split /::/, $caller )[0];

	log_message "Global key: $global ($caller)";

	my $result = 
	eval
	{
		# merge the command line settings into the default's 
		# global section.

		my $valuz = __PACKAGE__->configure( $global => $cmdline );

		log_message "Preparing schedule...";

		# last issue: the configuration includes the modules
		# that must be based into this one for S::D to derive
		# them as methods of the que object -- which may be none...

		if( my $base = $defaults->{use_base} )
		{
			ref $base
				or die "Bogus defaults{use_base}: not a referent";

			local $" = ' ';

			eval "use base qw( @$base )";
		}
		
		if( my $que = __PACKAGE__->prepare( $valuz->{$global} )->validate )
		{
			log_message "Schedule prepared and validated...";

			$que->{user} = $valuz;

			if( $cmdline->{debug} )
			{
				# debug mode has to pass in a list of methods to
				# run in order.

				croak "$$: bogus execute: debug mode missing method list"
					unless @_;

				log_message "Debugging jobs:", @_;

				for( @_ )
				{
					log_message "Unaliasing: $_";

					my $sub = $que->unalias( $_ );

					print "\n\a\n";

					$DB::single = 1;

					print $que->runjob( $sub ), "\n";
				}

				log_message "$0: method debug complete";
			}
			else
			{
				# do the deed by executing the schedule.
				# caller gets back the que's return value.

				log_message "Executing schedule.";

				$que->execute
			}
		}
		else
		{
			# this should never happen in production...

			log_error "$0: Failed prepare + validate";
		}
	};

	if( $result + 0 )
	{
		die "$$: Execution Exited with: $result";
	}
	elsif( $@ )
	{
		die "$$: Roadkill: execution aborted";
	}
	else
	{
		log_message "$$: Runsched Complete", $result;
	}

	0
}

# keep the use pragma happy

1

__END__

=head1 NAME

Schedule::Depend::Execute

Execution front end for schedules. Main use is tying together
the various modules used with S::D for running a set of jobs.
Exports "runsched" to pass in the schedule and optional sequence
of schedule items to debug for #! code.

=head1 SYNOPSIS

	# run the default schedule in
	# $Schedule::Depend::Execute::defalts{sched}.
	#
	# this is normal for production jobs.
	#
	# a local "Defaults" module is used to 
	# set up %Schedule::Depend::Execute::defaults.

	use Schedule::Depend::Execute;

	use Defaults;

	runsched;

	# use a specific schedule -- mainly useful
	# for debugging schedule components or 
	# running portions of a larger schedule as
	# utility functions.

	my $schedule = 
	q{
		dothis : dothat
		another : dothis
	};

	runsched $schedule;

	# extra arguments are run without forking in the
	# order given if "--debug" is passed on the command
	# line.

	runsched $schedule, qw( dothat dothis another );

	# run only a subset of the jobs.

	runsched $schedule, qw( dothat dothis );

=head1 NOTES

The first two examples are a reasonable production job. If
"--debug" is passed in on the command line then the last 
schedule is prepared and validated, after which the sequence
of jobs is run single-stream, without forking in the order
given.

During development the schedule sequence can be debugged by 
adding new items to the execution array and running the #!
code with --restart and --debug to skip previously completed
jobs and run the new ones. A single step in the schedule can
be debugged by passing it as the single item in the list:

	runsched $schedule, 'dothis';

will run only a single item in the schedule if "--debug" is
on the command line.

Within Execute.pm, the various modules that will be used
for a given job or set of schedules is added via "use base":


	use base
	qw(
		Foo::Module

		Bar::AnotherModule

		Schedule::Depend
	);

and the que object is prepared within the Execute package
to allow the methods to be called from a schedule.

Separate projects will normally require their own Project::Execute
to tie the pieces of that project together.

=head1 KNOWN BUGS

None, yet.

=head1 2DO

These might be re-written as que methods to clean up
issues accessing the defaults hash.

=head1 AUTHOR

Steven Lembark, Workhorse Computing 
<lembark@wrkhors.com>

=head1 Copyright

(C) 2001-2002 Steven Lembark, Workhorse Computing

This code is released under the same terms as Perl istelf. Please
see the Perl-5.8 distribution (or later) for a full description.

In any case, this code is release as-is, with no implied warranty
of fitness for a particular purpose or warranty of merchantability.
