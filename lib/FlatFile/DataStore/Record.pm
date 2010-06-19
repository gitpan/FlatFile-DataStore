#---------------------------------------------------------------------
  package FlatFile::DataStore::Record;
#---------------------------------------------------------------------

=head1 NAME

FlatFile::DataStore::Record - Perl module that implements a flat
file data store record class.

=head1 SYNOPSYS

 # first, create a preamble object

 use FlatFile::DataStore::Preamble;

 my $preamble = FlatFile::DataStore::Preamble->new( {
     datastore => $ds,         # FlatFile::DataStore object
     indicator => $indicator,  # single-character crud flag
     transind  => $transind,   # single-character crud flag
     date      => $date,       # pre-formatted date
     transnum  => $transint,   # transaction number (integer)
     keynum    => $keynum,     # record sequence number (integer)
     reclen    => $reclen,     # record length (integer)
     thisfnum  => $fnum,       # file number (in base format)
     thisseek  => $datapos,    # seek position (integer)
     prevfnum  => $prevfnum,   # ditto these ...
     prevseek  => $prevseek,
     nextfnum  => $nextfnum,
     nextseek  => $nextseek,
     user      => $user_data,  # pre-formatted user-defined data
     } );

 # then create a record object with the preamble contained in it

 use FlatFile::DataStore::Record;

 my $record = FlatFile::DataStore::Record->new( {
     preamble => $preamble,                 # i.e., a preamble object
     data     => "This is a test record.",  # actual record data
     } );

=head1 DESCRIPTION

FlatFile::DataStore::Record is a Perl module that implements a flat
file data store record class.  This class defines objects used by
FlatFile::DataStore.  You will likely not ever call new() yourself,
(FlatFile::DataStore::create() would, e.g., do that) but you will
likely call the accessors.

=head1 VERSION

FlatFile::DataStore::Record version 0.11

=cut

our $VERSION = '0.11';

use 5.008003;
use strict;
use warnings;

use Carp;

my %Attrs = qw(
    preamble  1
    data      1
    );

#---------------------------------------------------------------------

=head1 CLASS METHODS

=head2 FlatFile::DataStore::Record->new( $parms )

Constructs a new FlatFile::DataStore::Record object.

The parm C<$parms> is a hash reference containing key/value pairs to
populate the record string.  Two keys are recognized:

 - preamble, i.e., a FlatFile::DataStore::Preamble object
 - data,     the actual record data

The record data is stored in the object as a scalar reference.

=cut

sub new {
    my( $class, $parms ) = @_;

    my $self = bless {}, $class;

    $self->init( $parms ) if $parms;
    return $self;
}


#---------------------------------------------------------------------
# init(), called by new() to parse the parms

sub init {
    my( $self, $parms ) = @_;

    # want to store record data as a scalar reference
    for( $parms->{'data'} ) {
        if( defined ) {
            if( ref eq 'SCALAR' ) { $self->data( $_  ) }
            else                  { $self->data( \$_ ) }
        }
        else                      { $self->data( \"" ) }
    }

    if( my $preamble = $parms->{'preamble'} ) {
        $self->preamble( $preamble );
    }
    
    return $self;
}

#---------------------------------------------------------------------

=head1 OBJECT METHODS: Accessors

The following read/write methods set and return their respective
attribute values if C<$value> is given.  Otherwise, they just return
the value.

 $record->data(     $value ); # actual record data as a scalar ref
 $record->preamble( $value ); # FlatFile::DataStore::Preamble object

=cut

sub data     {
    my( $self, $data ) = @_;
    return $self->{data} unless $data;
    for( $data ) {
        if( ref eq 'SCALAR' ) { $self->{data} = $_  }
        else                  { $self->{data} = \$_ }
    }
}

sub preamble {for($_[0]->{preamble}){$_=$_[1]if@_>1;return$_}}

=pod

The following read-only methods just return their respective values.
The values all come from the record's contained preamble object.

 $record->user()
 $record->preamble_string()  # the 'string' attr of the preamble
 $record->indicator()
 $record->transind()
 $record->date()
 $record->transnum()
 $record->keynum()
 $record->reclen()
 $record->thisfnum()
 $record->thisseek()
 $record->prevfnum()
 $record->prevseek()
 $record->nextfnum()
 $record->nextseek()

 $record->is_created()
 $record->is_updated()
 $record->is_deleted()

=cut

sub user {for($_[0]->preamble()){defined&&return$_->user()}}

sub preamble_string {$_[0]->preamble()->string()}

sub datastore {$_[0]->preamble()->datastore()}
sub indicator {$_[0]->preamble()->indicator()}
sub transind  {$_[0]->preamble()->transind() }
sub date      {$_[0]->preamble()->date()     }
sub transnum  {$_[0]->preamble()->transnum() }
sub keynum    {$_[0]->preamble()->keynum()   }
sub reclen    {$_[0]->preamble()->reclen()   }
sub thisfnum  {$_[0]->preamble()->thisfnum() }
sub thisseek  {$_[0]->preamble()->thisseek() }
sub prevfnum  {$_[0]->preamble()->prevfnum() }
sub prevseek  {$_[0]->preamble()->prevseek() }
sub nextfnum  {$_[0]->preamble()->nextfnum() }
sub nextseek  {$_[0]->preamble()->nextseek() }

sub is_created {$_[0]->preamble()->is_created() }
sub is_updated {$_[0]->preamble()->is_updated() }
sub is_deleted {$_[0]->preamble()->is_deleted() }

__END__

=head1 AUTHOR

Brad Baxter, E<lt>bbaxter@cpan.orgE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2009 by Brad Baxter

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.8.8 or,
at your option, any later version of Perl 5 you may have available.

=cut

