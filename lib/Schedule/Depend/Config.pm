########################################################################
# Schedule::Depend::Config
#
# extract portions of $que->{user} relavant to the current
# module.
#
# also contains a few utility sub's that get exported on use.
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
# args will probably look something like:
#
#	( Wormbase => { @top_level }, Download => {@downloads} ... )
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

	$valuz->isa( __PACKAGE__ )
		or die "Bogus moduleconfig: config is not a Config";

	# the caller gets the global values from the 
	# config hash overridden by module-specific
	# ones.

	my ( $global, $module ) = (split /::/, caller)[0,-1];

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
	$module = $valuz->{$module} ? $valuz->{$module} : {};

	my $a = { %$global, %$module };

	wantarray ? %$a : $a
}

# "$que->userconfig" seems like a more descriptive way to 
# describe it.

*userconfig = \&moduleconfig;


########################################################################
# keep require happy

1

__END__

