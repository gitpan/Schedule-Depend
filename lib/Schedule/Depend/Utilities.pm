########################################################################
# Schedule::Depend::Utilities shared utilities. 
# these include logging, notification, and standard output formats.
########################################################################

########################################################################
# housekeeping
########################################################################

package Schedule::Depend::Utilities;

use strict;
use warnings;

our $VERSION=0.50;

use Carp;

use File::Basename;

use FindBin qw( $Bin );

# set the dumper default formats.

use Data::Dumper;
	$Data::Dumper::Purity           = 1;
	$Data::Dumper::Terse            = 1;
	$Data::Dumper::Indent           = 1;
	$Data::Dumper::Deepcopy         = 0;
	$Data::Dumper::Quotekeys        = 0;

# make sure that files created during execution have suitable
# mods for multi-stage processing.

umask 0002;

########################################################################
# exported subs
########################################################################

# deal with use. basic issue is to pullute the caller's
# namespace with our subs.

my @EXPORT = 
qw(
	log_message
	log_error
	log_format

	send_mail
	nastygram

	localpath
	checkdir

	slurp
);


sub import
{
	# stuff the subroutine names into the callers 
	# package space.
	#
	# the caller gets these defined whether they 
	# like it or not...

	my $caller = caller;

	no strict 'refs';

	*{ $caller . '::' . $_ } = __PACKAGE__->can( $_ )
		for @EXPORT;
}

########################################################################
# package variables
########################################################################

our $global = '';

our $basenames = '';

our $defaultz = \%Schedule::Depend::Execute::defaults;

########################################################################
# logging
########################################################################

# standardize log messages format.
# message arives in @_.

{ # isolate $msgseq, etc.

	my $msgseq = 0;

	my $tz = $ENV{TZ} || '';

	sub log_message
	{
		local $| = 1;
		local $\;

		@_ = ( join ' : ', caller ) unless @_;

		print STDOUT &log_format;

		# nothing fatal about being here...

		0
	}

	sub log_error
	{
		local $| = 1;
		local $\;

		@_ = ( join ' : ', caller ) unless @_;

		# put this in both the .out and .err logs.

		my $msg = &log_format;

		print STDOUT $msg;
		print STDERR $msg;

		# make sure the caller gets back non-zero.

		-1
	}	

	sub log_format
	{
		my $msg = join "\n", map { ref $_ ? Dumper $_ : $_ } ( @_, '' );

		join ' ', "\n$$", ++$msgseq, scalar localtime, $msg;
	}	
}

########################################################################
# email notification
########################################################################

########################################################################
# this actually sends the mail.
# it provides sane defaults for the minimal headers.

sub send_mail
{
	use MIME::Lite;

	my %mailargz = ref $_[0] ? %{$_[0]} : @_;

	unless( $mailargz{To} )
	{
		$mailargz{To} = 'root@localhost';

		log_error 'Bogus send_mail: defaulting email to root@localhost';
	}

	$mailargz{To} = join ',', @{ $mailargz{To} }
		if ref $mailargz{To};

	# other things can reasonably be defaulted...

	my $From = (getpwuid $>)[0] . q{@Schedule::Depend.com};

	$mailargz{'X-Schedule::Depend'} ||= "Generic";

	$mailargz{From} ||= $From;

	$mailargz{'Reply-to'} ||= $mailargz{From};

	$mailargz{Subject} ||= 'Empty Subject';

	$mailargz{Subject} =~ s/^/[Unknown-Notify]/
		unless $mailargz{Subject} =~ /^\[/;

	$mailargz{Data} ||= 'This is an automatic notification';

	# mail is flakey enough that this needs to log
	# any failurs. also saves people from complaing
	# that the mail wasn't delivered: at least we 
	# know why from this end...

	eval { MIME::Lite->new( %mailargz )->send };

	log_error "Failed sending email: $@" if( $@ );

	# caller gets back the subject and name list.

	"$mailargz{Subject} -> $mailargz{To}"
}

########################################################################
# que_mail is for sending email from w/in the schedule.
# it is intended to handle automated progress notification.
# use send_mail with a more specific message in order to 
# transmit error/recovery information.

sub progress_mail
{
	my $que = shift
		or croak "Bogus progress_mail: missing que object";

	$DB::single = 1 if $que->debug;

	my $job = shift
		or croak "Bogus notify: missing job argument";

	my $config 	= $que->{user}->moduleconfig
		or die "$$: Bogus notify: que missing user data.";

	my $quename = $config->{quename};

	my $names = $config->{notify}{$job}
		or die "$$: Bogus notify: no '$job' name list configured.";

	my $fromid = $config->{mail_from}
		or die "$$: Bogus notify: no 'mail_from' entry configured.";

	my %mailargz =
	(
		# use to detect test email in mail rules.
		# setting the rules to detect specific programs
		# makes it easeir to segregate based on what is
		# being tested. 

		'X-Schedule::Depend'      => "$quename-$job",

		# set from command line w/ default.

		To              => $names,
		'Reply-to'      => $fromid,
		From            => $fromid,

		Subject         => "[$quename-Progress] $job",

		Data            => 'This is an automated progress message.',
	);

	# caller gets back result of sending the mail.

	send_mail %mailargz
}

########################################################################
# log & email fatal messages.
#
# Note:  this cannot die until the end in order to guarantee
# that the log entries and email are sent. it has to be 
# entirely tolerant of bogus configs and arguments.

sub nastygram
{
	# log the stuff first, this guarantees
	# that at least something will be recorded.

	log_error "Roadkill:", @_;

	# without a que object, use the global defaults
	# to find things instead.

	$global ||= $defaultz->{global_key};

	my $config = $defaultz->{$global}
		or warn "Bogus config: missing global 'Schedule::Depend' entry";

 	my $quename = $config->{quename} || 'Unknown';

	my $fatal = "$quename-Fatal";

	my $notify = $config->{notify}
		or warn "Bogus config:  global missing 'notify' entry";

	my $subject = shift || 'Bad news, boss...';

	$subject =~ s/^/[$fatal]/ unless $subject =~ /^\[/;

	my $message = log_format @_;

	my $mailargz =
	{
		'X-Schedule::Depend'	=> $fatal,

		To   		=> $notify->{fatal} || 'root@localhost',

		From		=> $config->{mail_from} || 'root@localhost',

		Subject 	=> $subject,

		Data		=> $message,
	};

	# die with the message subject and notify list.
	# this will be the last line of the job's .err file.

	die send_mail $mailargz;
}

########################################################################
# directory operations
########################################################################

# various standard file paths based on $Bin.
# empty arg's gives $Bin with a trailing '/'.
#
# note that the directory can be anything, including
# subdir's or file basenames.
#
# make non-absolute paths relative to $Bin instead
# of the current working directory.
#
# creating the file is unlikely to succeed
# unless the directory already exists...

sub localpath
{

	$global ||= $defaultz->{global_key};

	$basenames ||= $defaultz->{$global}{base};

	# make a local copy of the last argument; replace
	# symbolic names from the defaults with real 
	# basenames. making the copy avoids modifying 
	# another argument in place via @_.

	if( @_ )
	{
		my $name = pop;

		$name = $basenames->{$name} if exists $basenames->{$name};

		push @_, $name
	}

	my $path = join '/', ( @_ ? @_ : '' );

	$path = "$Bin/$path" unless $path =~ m{^/};

	# this is a hack and should be configured via
	# values in the default config. that or this
	# should use parsefile to default the extension
	# if none is given. 

	$path .= '.dump' if ! -e $path && -e "$path.dump";

	-e dirname $path
		or carp "Nonexistant directory: $path";

	# caller gets back the path, which may not 
	# exist, yet.

	$path
}

# validate/create a directory.
# default mods are 02775.

sub checkdir
{
	for my $path ( @_ )
	{
		-e $path || mkdir $path, 02775
			or die "Roadkill: unable to find/create $path";

		# at this point the directory should exist
		# and be accessable. since it may have been
		# created on the last step at least one more
		# file test operator has to give the path to
		# force a stat.

		-e $path	or die "Roadkill: non-executable: $path";
		-d _		or die "Roadkill: non-directory: $path";
		-x _		or die "Roadkill: non-executable: $path";
		-w _		or die "Roadkill: non-writeable: $path";
		-r _		or die "Roadkill: non-readable: $path";
	}
}


########################################################################
# un-dump data.
#
# read dumper Dumper output back into a scalar.
# caller gets back the result of string-eval of
# the file contents.
#
# adding '.dump' allows passing in sql keys
# used in unload as basenames.

sub slurp
{
	local $/;

	my $path = shift
		or croak "Bogus slurp: missing path";

	$path .= '.dump' unless -e $path;

	-e $path	or nastygram "Missing $path";
	-r _		or nastygram "Unreadable $path";
	-s _		or nastygram "Empty $path";

	open my $fh, '<', $path
		or nastygram "$path: $!";

	defined ( my $item = <$fh> )
		or nastygram "Failed read on non-empty: $path";

	# caller gets back the result of eval-ing the item
	# if it is anything. $resut will be true even for
	# empty structs (e.g., the referents $a = {}  $b = []
	# are both true).

	my $result = eval $item
		or nastygram "Failed eval during slurp", $item;
}

########################################################################
# keep require happy

1

__END__

=head1 TITLE

Schedule::Depend::Utilities

Kitchen-sink module for configuratin, logging, whatever...

=head1 SYNOPSIS

	use Schedule::Depend::Utilities;

	# send email notification

	notify $message, @mailto;

	# generate path relative to the #!'s $Bin directory.
	# these can be abs-pathed without regard to the 
	# current working directory.

	localpath @path_components

	# sanity check a set of paths.

	checkdir @dirlist;

	# override values from the Defaults module at run time. 

	my $new_config = $config->configure %overrides;

	# return a flattend out defaults hash with the global
	# values overridden by module-specific settings. this
	# allows methods to access the values with a single key.

	sub called_from_schedule_depend
	{
		my $que = shift;

		my $config = $que->{user}->moduleconfig;

		my $value = $config->{key};

		...
	}


=head1 NOTES

notify depends a working sendmail config for the given system.

The purpose of moduleconfig is to simplify access to module-speific
data and allow it to easly override the global values.

Given a defaults hash with:

	{
		Schedule::Depend =>
		{
			verbose => 0,
		},

		Download =>
		{
			httphost => 'there.com',
			httppath => '/pub/here',

			verbose => 1,
		},

		Execute =>
		{
			debug => 1,
			force => 1,
		}

	} 

moduleconfig called within Schedule::Depend::Download returns a
hash with:

	{
		verbose => 1,

		httphost => 'there.com',
		httppath => '/pub/here',
	}

This allows code to access $config->{key} without having to
check for $config->{Download}{$key} || $config->{$global}{$key}
for every value: the merged hashes handle this in one step.

If called within Schedule::Depend::Execute the defaults hash above 
returns:

	{
		verbose => 0,
		debug => 1,
		force => 1
	}

Moving code between modules will have them automatically
getting back the correct defaults for that module.

Most methods of a que object called from the schedule
will begin something like:

	sub download
	{
		my $que		= shift
			or croak "Bogus download: missing que object";

		@_ or croak "Bogus download: missing download basename";

		$DB::single = 1 if $que->debug;

		my $config 	= $que->{user}->moduleconfig
			or die "$$: Bogus download: que missing user data.";


This allows the parse to be used as an alias to handle multiple
cases in the schedule, for example:

	CleanEx.dat = download

	Foo.bar = download

Will eventually call $que->download( 'CleanEx.dat' ) and
$que->download( 'Foo.bar' ). Default configuration data
is held in the que's user area, blessed into the Shared
package so that moduleconfig is available from it -- this is
handled automatically by runsched (in Execute.pm). 

=head1 See Also

=over 4

=item Schedule::Depend

General schdule syntax.

=item Perl Documentation

perl(1) perlreftut(1)

=back

=head1 AUTHOR

Steven Lembark, Workhorse Computing 
<lembark@wrkhors.com>

