########################################################################
# Schedule::Depend::Config
#
# Use the queue's global entry or the caller's package name to 
# generate a list of call-specific entries from
# Schedule::Depend::Execute::defaults. The basic idea is that the 
# defaults hash has some global information and other portions local
# to the job being run. By separating them out in the configuration 
# it allows different modules to re-use some of their tags and 
# simplifies data sharing (prior to forks).
#
# 
########################################################################

########################################################################
# housekeeping
########################################################################

package Schedule::Depend::Config;

use strict;
use warnings;

our $VERSION=0.40;

########################################################################
# deal with the config hash
#
# the result of this is used for $que->{user} in
# S::D::Execute::runsched.
########################################################################

########################################################################
# config takes a list of key => hash_ref and for each of
# the keys (main levels of the defaults hash) merges the
# supplied values into the default hash at that point.
# args will probably look something like. For example a 
# Foobar::Download modules would get 'Foobar' as the 
# global section with 'Download' overriding it:
#
#	( Foobar => { @top_level }, Download => {@downloads} ... )
#
# this is used in Schedule::Depend::Execute to populate
# $que->{user}.

sub configure
{
	my $item = shift;

	# this may be called as a class method, at 
	# which point the item passed in won't be
	# a referent. in that case use the global
	# S::D defaults hash.

	my $config = ref $item ? $item : \%Schedule::Depend::Execute::defaults;

	# convert the remaining arguments to a hash,
	# iterate the keys, and assign values into 
	# the config hash.

	if( my %argz = @_ )
	{
		for( keys %argz )
		{
			# hak alert: this needs to deal with
			# un-nested structures cleanly. for
			# now this will work...

			my $a = $argz{$_};
			my $c = $config->{$_} || {};

			@{$c}{keys %$a} = @{$a}{keys %$a};
		}
	}

	# caller gets back a blessed referent.
	# the config hash will have whatever
	# arguments were passed in here folded
	# into it.

	bless $config, ref $item || $item
}

########################################################################
# merge the general configs into a flat hash for the 
# current module. this saves a level of de-referencing
# in the code and simplifies overrides.
#
# as an example, Schedule::Depend::Execute jobs are run with 
# a $que->{config} area for storing queue configurations.
#
# methods of the que object can use something like:
#
# 	my $que = shift or die;
#	my $config = $que->{config}->module_config;
#
# to get their local arguments. 


sub moduleconfig
{
	my $config = shift
		or die "Bogus moduleconfig: missing config object";
		
	# this makes no sense at all as a class method.

	ref $config
		or die "Bogus moduleconfig: config not a referent";

	# catch: this may be called as "$que->config", which
	# requires taking the results from $config->{user}
	# instead of $config...

	my $valuz = $config->{user} || $config;

	# the caller gets the global values from the 
	# config hash overridden by module-specific
	# ones.

	my ( $global, $local ) = (split /::/, caller)[0,-1];

	$global = $valuz->{global} if $valuz->{global};

	# merge the global and module hashes by simply expanding the
	# full hashes into a single new hash. this flattens the
	# key structure by one level: the caller can use $config->{key}
	# and be done with it.
	#
	# the hash-of-hashes structure leaves this operation
	# pretty effecient since all that gets copied are 
	#
	# caller gets back a hash referent with the
	# global values overridden by the module-specific
	# ones. note that the hash assignment is going to
	# give back shallow copies for nested structures.

	$global = $valuz->{$global} ? $valuz->{$global} : {};
	$local  = $valuz->{$local}  ? $valuz->{$local}  : {};

	my $a = { %$global, %$local };

	wantarray ? %$a : $a
}

# "$que->userconfig" seems like a another descriptive way to 
# name this.

*userconfig = \&moduleconfig;


########################################################################
# keep require happy

1

__END__

=head1 NAME

Schedule::Depend::Config

Extract global and package-specifc data from a configuration
hash.

=head1 SYNOPSIS

	sub sd_job
	{
		my $que = shift;
		my $config = $que->{user}->moduleconfig;

		...
	}

=head1 DESCRIPTION

=head2 $que->{user}

Schedule::Depend and Schedule::Depend::Execute install a 
"user" key into the que hash for storing arbitrary data
the user jobs may need during execution.

S::D uses the constructor "configure" to build the user
hash. This merges any arguments passed in with the current
\%Schedule::Depend::Execute::defaults values.

moduleconfig is called in jobs to extract relavant 
configuration information. The basic idea is to keep
data separated by module in the configuration hash 
to allow for data inheritence and avoid namespace
collisions between modules.

=head2 Example Schedule::Depend::Execute::defaults 

The configuration hash for jobs in Foobar::*::Download
and Foobar::*::Unlaod would look like:

	my $config = 
	{
		Foobar =>
		{
			# anything here is available to both
			# Download and Unload, with tEh modules
			# overloading it.
			#
			# this is used by the "localpath" utility
			# subroutine to convert generic tokens into
			# local paths.

			basenames =>
			{
				# token => file basename
				
				table1 => 'table1-full.dump',
				table2 => 'table2-partial.dump',
			},
		},

		Download =>
		{
			ftpbase => 'ftp://foo.bar.com',

			ftppath =>
			[
				 '/pub/bletch',
				 '/pub/morebletch',
			],
		},

		Unload =>
		{
			unload_tables =>
			[ qw(
				table1
				table2
			) ],
		},
	}

In this case calls from subroutines in Foobar::Download will 
get 'basenames', 'ftpbase' and 'ftppath' keys back in
their configuration; calls from the Foobar::Unload package
will get 'basenames' and 'unload_tables'.

=head2 Setting the global portion.

The default global and local portion of the configuraton
are set via:

	( split /::/ $caller )[0, -1]

This means that "Foobar::Frum::Download" and
"Foobar::Feefie::Download" will get the same results. If 
this not useful then the caller can set $config->{global}
prior to calling moduleconfig in order to set the global
data's key:

	package Foobar::Upload;

	sub some_job
	{
		my $que = shift;

		my $user = $que->{user};

		$user->{global} = 'Baronly';

		my $config = $user->moduleconfig;

		...
	}

Will leave $config with defaults from $user->{Baronly}
and local data from "Upload".

=head1 SEE ALSO

Schedule::Depend Schedule::Depend::Execute

=head1 KNOWN BUGS

None, yet.

=head1 2DO

Add the global and local sections as paramters to moduleconfig.

=head1 AUTHOR

Steven Lembark, Workhorse Computing <lembark@wrkhors.com>

=head1 Copyright

(C) 2001-2002 Steven Lembark, Workhorse Computing

This code is released under the same terms as Perl istelf. Please
see the Perl-5.8 distribution (or later) for a full description.

In any case, this code is release as-is, with no implied warranty
of fitness for a particular purpose or warranty of merchantability.
