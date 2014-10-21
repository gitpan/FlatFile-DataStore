#---------------------------------------------------------------------
  package FlatFile::DataStore::Preamble;
#---------------------------------------------------------------------

=head1 NAME

FlatFile::DataStore::Preamble - Perl module that implements a flat
file data store preamble class.

=head1 SYNOPSYS

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

 my $string = $preamble->string();

 my $clone = FlatFile::DataStore::Preamble->new( {
     datastore => $ds,
     string    => $string
     } );

=head1 DESCRIPTION

FlatFile::DataStore::Preamble - Perl module that implements a flat file
data store preamble class.  This class defines objects used by
FlatFile::DataStore::Record and FlatFile::DataStore.  You will
probably not ever call new() yourself, but you might call some of the
accessors either directly or via a FF::DS::Record object;

A "preamble" is a string of fixed-length fields that precedes every
record in a FlatFile::DataStore data file.  In addition, this string
constitutes the entry in the data store key file for each current
record.

=head1 VERSION

FlatFile::DataStore::Preamble version 0.12

=cut

our $VERSION = '0.12';

use 5.008003;
use strict;
use warnings;

use Carp;
use Math::Int2Base qw( base_chars int2base base2int );
use Data::Omap qw( :ALL );

my %Generated = qw(
    string      1
    );

my %Attrs = ( %Generated, qw(
    indicator 1
    transind  1
    date      1
    transnum  1
    keynum    1
    reclen    1
    thisfnum  1
    thisseek  1
    prevfnum  1
    prevseek  1
    nextfnum  1
    nextseek  1
    user      1
    ) );

my $Ascii_chars = qr/^[ -~]+$/;

#---------------------------------------------------------------------

=head1 CLASS METHODS

=head2 FlatFile::DataStore::Preamble->new( $parms )

Constructs a new FlatFile::DataStore::Preamble object.

The parm C<$parms> is a hash reference containing key/value pairs to
populate the preamble string.  If there is a C<< $parms->{'string'} >>
value, it will be parsed into fields and the resulting key/value pairs
will replace the C<$parms> hash reference.

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

    my $datastore = $parms->{'datastore'} || croak "Missing datastore";
    if( my $string = $parms->{'string'} ) {
        $parms = $datastore->burst_preamble( $string );
    }

    my $crud = $datastore->crud();
    $self->crud( $crud );

    my $create = $crud->{'create'};
    my $update = $crud->{'update'};
    my $delete = $crud->{'delete'};
    my $oldupd = $crud->{'oldupd'};
    my $olddel = $crud->{'olddel'};

    my $indicator = $parms->{'indicator'} || croak "Missing indicator";
    $self->indicator( $indicator );

    my $transind = $parms->{'transind'} || croak "Missing transind";
    $self->transind( $transind );

    my $string = "";
    for my $href ( $datastore->specs() ) {  # each field is href of aref
        my( $field, $aref )     = %$href;
        my( $pos, $len, $parm ) = @$aref;
        my $value               = $parms->{ $field };

        for( $field ) {
            if( /indicator/ or /transind/ ) {
                croak qq'Missing value for "$_"' unless defined $value;
                croak qq'Invalid value for "$_" ($value)' unless length $value == $len;

                $self->{ $_ } = $value;
                $string      .= $value;
            }
            elsif( /date/ ) {
                croak qq'Missing value for "$_"' unless defined $value;
                croak qq'Invalid value for "$_" ($value)' unless length $value == $len;

                $self->{ $_ } = then( $value, $parm );
                $string      .= $value;
            }
            elsif( /user/ ) {
                unless( defined $value ) {
                    $value = $datastore->userdata;
                    croak qq'Missing value for "$_"' unless defined $value;
                }

                my $try = sprintf "%-${len}s", $value;  # pads with blanks
                croak qq'Value of "$_" ($try) too long' if length $try > $len;

                my $user_regx = qr/^[$parm]+ *$/;  # $parm chars already escaped as needed
                croak qq'Invalid value for "$_" ($value)' unless $try =~ $user_regx;

                $self->{ $_ } = $value;
                $string      .= $try;
            }
            elsif( not defined $value ) {
                if( (/keynum|reclen|transnum|thisfnum|thisseek/              ) or
                    (/prevfnum|prevseek/ and $indicator =~ /[\Q$update$delete\E]/) or
                    (/nextfnum|nextseek/ and $indicator =~ /[\Q$oldupd$olddel\E]/) ) {
                    croak qq'Missing value for "$_"';
                }
                $string .= "-" x $len;  # string of '-' for null
            }
            else {
                if( (/nextfnum|nextseek/ and $indicator =~ /[\Q$update$delete\E]/) or
                    (/prevfnum|prevseek/ and $indicator =~ /[\Q$create\E]/       ) ) {
                    croak qq'Setting value of "$_" not permitted';
                }
                my $try = sprintf "%0${len}s", /fnum/? $value: int2base( $value, $parm );
                croak qq'Value of "$_" ($try) too long' if length $try > $len;

                $self->{ $_ } = /fnum/? $try: 0+$value;
                $string      .= $try;
            }
        }
    }

    croak qq'Something is wrong with preamble string: "$string"'
        unless $string =~ $datastore->regx();
    
    $self->string( $string );

    return $self;
}

#---------------------------------------------------------------------

=head1 OBJECT METHODS: ACCESSORS

The following methods set and return their respective attribute values
if C<$value> is given.  Otherwise, they just return the value.

 $preamble->string(    $value ); # full preamble string
 $preamble->indicator( $value ); # single-character crud indicator
 $preamble->transind(  $value ); # single-character crud indicator
 $preamble->date(      $value ); # date as YYYY-MM-DD
 $preamble->transnum(  $value ); # transaction number (integer)
 $preamble->keynum(    $value ); # record sequence number (integer)
 $preamble->reclen(    $value ); # record length (integer)
 $preamble->thisfnum(  $value ); # file number (in base format)
 $preamble->thisseek(  $value ); # seek position (integer)
 $preamble->prevfnum(  $value ); # ditto these ...
 $preamble->prevseek(  $value ); # 
 $preamble->nextfnum(  $value ); # 
 $preamble->nextseek(  $value ); # 
 $preamble->user(      $value ); # pre-formatted user-defined data
 $preamble->crud(      $value ); # hash ref of all crud indicators

Note: the class code uses these accessors to set values in the object
as it is assembling the preamble string in new().  Unless you have a
really good reason, you should not set these values yourself (outside
of a call to new()).  For example: setting the date with date() will
I<not> change the date in the C<string> attribute.

In other words, even though these are read/write accessors, you should
only use them for reading.

=cut

sub string    {for($_[0]->{string}    ){$_=$_[1]if@_>1;return$_}}
sub indicator {for($_[0]->{indicator} ){$_=$_[1]if@_>1;return$_}}
sub transind  {for($_[0]->{transind}  ){$_=$_[1]if@_>1;return$_}}
sub crud      {for($_[0]->{crud}      ){$_=$_[1]if@_>1;return$_}}
sub date      {for($_[0]->{date}      ){$_=$_[1]if@_>1;return$_}}
sub user      {for($_[0]->{user}      ){$_=$_[1]if@_>1;return$_}}

sub keynum    {for($_[0]->{keynum}    ){$_=0+$_[1]if@_>1;return$_}}
sub reclen    {for($_[0]->{reclen}    ){$_=0+$_[1]if@_>1;return$_}}
sub transnum  {for($_[0]->{transnum}  ){$_=0+$_[1]if@_>1;return$_}}
sub thisfnum  {for($_[0]->{thisfnum}  ){$_=  $_[1]if@_>1;return$_}}
sub thisseek  {for($_[0]->{thisseek}  ){$_=0+$_[1]if@_>1;return$_}}
sub prevfnum  {for($_[0]->{prevfnum}  ){$_=  $_[1]if@_>1;return$_}}
sub prevseek  {for($_[0]->{prevseek}  ){$_=0+$_[1]if@_>1;return$_}}
sub nextfnum  {for($_[0]->{nextfnum}  ){$_=  $_[1]if@_>1;return$_}}
sub nextseek  {for($_[0]->{nextseek}  ){$_=0+$_[1]if@_>1;return$_}}

#---------------------------------------------------------------------

=head2 Convenience methods

=head3 is_created(), is_updated(), is_deleted();

These methods and return true if the indicator
matches the value implied by the method name, e.g.,

 print "Deleted!" if $preamble->is_deleted();

=cut

sub is_created {
    my $self = shift;
    $self->indicator eq $self->crud->{'create'};
}
sub is_updated {
    my $self = shift;
    $self->indicator eq $self->crud->{'update'};
}
sub is_deleted {
    my $self = shift;
    $self->indicator eq $self->crud->{'delete'};
}

#---------------------------------------------------------------------
# then(), translates stored date to YYYY-MM-DD
#     Takes a date and a format and returns the date as yyyy-mm-dd.
#     If the format contains 'yyyy' it is assumed to have decimal
#     values for month, day, year.  Otherwise, it is assumed to have
#     base62 values for them.
#
# Private method.

sub then {
    my( $date, $format ) = @_;
    my( $y, $m, $d );
    my $ret;
    for( $format ) {
        if( /yyyy/ ) {  # decimal year/month/day
            $y = substr $date, index( $format, 'yyyy' ), 4;
            $m = substr $date, index( $format, 'mm'   ), 2;
            $d = substr $date, index( $format, 'dd'   ), 2;
        }
        else {  # assume base62 year/month/day
            $y = substr $date, index( $format, 'yy' ), 2;
            $m = substr $date, index( $format, 'm'  ), 1;
            $d = substr $date, index( $format, 'd'  ), 1;
            $y = sprintf "%04d", base2int( $y, 62 );
            $m = sprintf "%02d", base2int( $m, 62 );
            $d = sprintf "%02d", base2int( $d, 62 );
        }
    }
    return "$y-$m-$d";
}

__END__

=head1 AUTHOR

Brad Baxter, E<lt>bbaxter@cpan.orgE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2010 by Brad Baxter

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.8.8 or,
at your option, any later version of Perl 5 you may have available.

=cut

