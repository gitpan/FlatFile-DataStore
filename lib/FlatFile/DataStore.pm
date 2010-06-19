#---------------------------------------------------------------------
  package FlatFile::DataStore;
#---------------------------------------------------------------------

=head1 NAME

FlatFile::DataStore - Perl module that implements a flat file data
store.

=head1 SYNOPSYS

 use FlatFile::DataStore;

 # new datastore object

 my $dir  = "/my/datastore/directory";
 my $name = "dsname";
 my $ds   = FlatFile::DataStore->new( { dir => $dir, name => $name } );

 # create a record

 my $record_data = "This is a test record.";
 my $user_data   = "Test1";
 my $record = $ds->create( $record_data, $user_data );
 my $record_number = $record->keynum;

 # retrieve it

 $record = $ds->retrieve( $record_number );

 # update it

 $record->data( "Updating the test record." );
 $record = $ds->update( $record );

 # delete it

 $record = $ds->delete( $record );

 # get its history

 my @records = $ds->history( $record_number );

=head1 DESCRIPTION

FlatFile::DataStore implements a simple flat file data store.  When you
create (store) a new record, it is appended to the flat file.  When you
update an existing record, the existing entry in the flat file is
flagged as updated, and the updated record is appended to the flat
file.  When you delete a record, the existing entry is flagged as
deleted, and a I<deleted> record is I<appended> to the flat file.

The result is that all versions of a record are retained in the data
store, and running a history will return all of them.  Another result
is that each record in the data store represents a transaction: create,
update, or delete.

Methods support the following actions:

 - create
 - retrieve
 - update
 - delete
 - history

Additionally, FlatFile::DataStore::Utils provides the
methods

 - validate
 - migrate

and others.

See FlatFile::DataStore::Tiehash for a tied interface.

=head1 VERSION

FlatFile::DataStore version 0.11

=cut

our $VERSION = '0.11';

use 5.008003;
use strict;
use warnings;

use URI::Escape;
use File::Path;
use Fcntl qw(:DEFAULT :flock);
use Digest::MD5 qw(md5_hex);
use Data::Dumper;
use Carp;

use FlatFile::DataStore::Preamble;
use FlatFile::DataStore::Record;
use FlatFile::DataStore::Toc;
use Math::Int2Base qw( base_chars int2base base2int );
use Data::Omap qw( :ALL );

#---------------------------------------------------------------------
# globals:

my %Preamble = qw(
    indicator   1
    transind    1
    date        1
    transnum    1
    keynum      1
    reclen      1
    thisfnum    1
    thisseek    1
    prevfnum    1
    prevseek    1
    nextfnum    1
    nextseek    1
    user        1
    );

my %Optional = qw(
    dirmax      1
    dirlev      1
    tocmax      1
    keymax      1
    prevfnum    1
    prevseek    1
    nextfnum    1
    nextseek    1
    userdata    1
    );

# attributes that we generate (vs. user-supplied)
my %Generated = qw(
    uri         1
    crud        1
    dateformat  1
    specs       1
    regx        1
    preamblelen 1
    fnumlen     1
    fnumbase    1
    translen    1
    transbase   1
    keylen      1
    keybase     1
    toclen      1
    datamax     1
    );

# all attributes, including some more user-supplied ones
my %Attrs = ( %Preamble, %Optional, %Generated, qw(
    name        1
    dir         1
    desc        1
    recsep      1
    ) );

my $Ascii_chars = qr/^[ -~]+$/;  # i.e., printables
my( %Read_fh, %Write_fh );  # inside-outish object attributes

#---------------------------------------------------------------------

=head1 CLASS METHODS

=head2 FlatFile::DataStore->new();

Constructs a new FlatFile::DataStore object.

Accepts hash ref giving values for C<dir> and C<name>.

 my $ds = FlatFile::DataStore->new(
     { dir  => $dir,
       name => $name,
     } );

To initialize a new data store, edit the "$dir/$name.uri" file
and enter a configuration URI (as the only line in the file),
or pass the URI as the value of the C<uri> parameter, e.g.,

 my $ds = FlatFile::DataStore->new(
     { dir  => $dir,
       name => $name,
       uri  => join( ";" =>
           "http://example.com?name=$name",
           "desc=My%20Data%20Store",
           "defaults=medium",
           "user=8-%20-%7E",
           "recsep=%0A",
           ),
     } );

(See URI Configuration below.)

Returns a reference to the FlatFile::DataStore object.

=cut

#---------------------------------------------------------------------
# new(), called by user to construct a data store object

sub new {
    my( $class, $parms ) = @_;

    my $self = bless {}, $class;

    $self = $self->init( $parms ) if $parms;  # $self could change ...
    return $self;
}

#---------------------------------------------------------------------
# init(), called by new() to initialize a data store object
#     parms (from hash ref):
#       dir  ... the directory where the data store lives
#       name ... the name of the data store
#       uri  ... a uri to be used to configure the data store
#     init() will look for dir/name.uri and load its values
#
# Private method.

sub init {
    my( $self, $parms ) = @_;

    my $dir  = $parms->{'dir'};
    my $name = $parms->{'name'};
    croak qq/Need "dir" and "name"/
        unless defined $dir and defined $name;
    croak qq/Directory "$dir" doesn't exist./
        unless -d $dir;

    # uri file may be
    # - one line: just the uri, or
    # - four lines: uri, object, object_md5, uri_md5
    #
    # if new_uri and uri file has
    # - one line ... new_uri can replace old one
    # - four lines (and new_uri is different) ...
    #   new_uri can replace the old uri (and object)
    #   but only if there aren't any data files yet

    my $new_uri = $parms->{'uri'};

    my $uri_file = "$dir/$name.uri";
    my( $uri, $obj, $uri_md5, $obj_md5 );
    if( -e $uri_file ) {
        my @lines = $self->read_file( $uri_file ); chomp @lines;
        if( @lines == 4 ) {
            ( $uri, $obj, $uri_md5, $obj_md5 ) = @lines;
            croak "URI MD5 check failed."    unless $uri_md5 eq md5_hex( $uri );
            croak "Object MD5 check failed." unless $obj_md5 eq md5_hex( $obj );
        }
        elsif( @lines == 1 ) {
            $uri = $new_uri || shift @lines;
        }
        else {
            croak "Invalid URI file: '$uri_file'";
        }
    }

    # if database has been initialized, there's an object
    if( $obj ) {
        $self = eval $obj;  # note: *new* $self
        croak qq/Problem with $uri_file: $@/ if $@;
        $self->dir( $dir );  # dir not in object

        # new uri ok only if no data has been added yet
        if( $new_uri         and
            $new_uri ne $uri and
            not -e $self->which_datafile( 1 ) ) {
                $uri = $new_uri;
                $obj = '';  # we want a new one
        }
    }

    # otherwise initialize the database
    unless( $obj ) {
        $uri ||= $new_uri || croak "No URI.";
        $self->uri( $uri );

        # Note: 'require', not 'use'.  This isn't
        # a "true" module--we're just bringing in
        # some more FlatFile::DataStore methods.

        require FlatFile::DataStore::Initialize;

        my $uri_parms = $self->burst_query( \%Preamble );
        for my $attr ( keys %$uri_parms ) {
            croak qq/Unrecognized parameter: "$attr"/ unless $Attrs{ $attr };

            # (note: using $attr as method name here)
            $self->$attr( $uri_parms->{ $attr } );
        }

        # check that all fnums and seeks are the same ...
        #
        # (note: prevfnum, prevseek, nextfnum, and nextseek are
        # optional, but if you have one of them, you must have
        # all four, so checking for one of them here is enough)

        if( $self->prevfnum ) {
            croak qq/fnum parameters differ/
                unless $self->thisfnum eq $self->prevfnum and
                       $self->thisfnum eq $self->nextfnum;
            croak qq/seek parameters differ/
                unless $self->thisseek eq $self->prevseek and
                       $self->thisseek eq $self->nextseek;
        }

        # now for some generated attributes ...
        my( $len, $base );
        # (we can use thisfnum because all fnums are the same)
        ( $len, $base ) = split /-/, $self->thisfnum;
        $self->fnumlen(    $len                        );
        $self->fnumbase(   $base                       );
        ( $len, $base ) = split /-/, $self->transnum;
        $self->translen(   $len                        );
        $self->transbase(  $base                       );
        ( $len, $base ) = split /-/, $self->keynum;
        $self->keylen(     $len                        );
        $self->keybase(    $base                       );
        $self->dateformat( (split /-/, $self->date)[1] );
        $self->regx(       $self->make_preamble_regx   );
        $self->crud(       $self->make_crud            );
        $self->dir(        $dir                        );  # dir not in uri

        $self->toclen( 10          +  # blanks between parts
            3 *    $self->fnumlen  +  # datafnum, tocfnum, keyfnum
            2 *    $self->keylen   +  # numrecs keynum
            6 *    $self->translen +  # transnum and cruds
            length $self->recsep );

        # (we can use thisseek because all seeks are the same)
        ( $len, $base ) = split /-/, $self->thisseek;
        my $maxnum = substr( base_chars( $base ), -1) x $len;
        my $maxint = base2int $maxnum, $base;

        if( my $max = $self->datamax ) {
            $self->datamax( convert_max( $max ) );
            if( $self->datamax > $maxint ) {
                croak join '' =>
                    "datamax (", $self->datamax, ") too large: ",
                    "thisseek is ", $self->thisseek,
                    " so maximum datamax is $maxnum base-$base ",
                    "(decimal: $maxint)";
            }
        }
        else {
            $self->datamax( $maxint );
        }

        if( my $max = $self->dirmax ) {
            $self->dirmax( convert_max( $max ) );
            $self->dirlev( 1 ) unless $self->dirlev;
        }

        if( my $max = $self->keymax ) {
            $self->keymax( convert_max( $max ) );
        }

        if( my $max = $self->tocmax ) {
            $self->tocmax( convert_max( $max ) );
        }

        for my $attr ( keys %Attrs ) {
            croak qq/Uninitialized attribute: "$attr"/
                if not $Optional{ $attr } and not defined $self->$attr;
        }

        $self->initialize;
    }

    for( $parms->{'userdata'} ) {
        $self->userdata( $_ ) if defined; 
    }

    return $self;  # this is either the same self or a new self
}

#---------------------------------------------------------------------

=head1 OBJECT METHODS, Record Processing (CRUD)

=head2 create( $record_data[, $user_data] )

Creates a record.  The parm C<$record_data> may be one of

 - data string
 - scalar reference (to the data string)
 - FlatFile::DataStore::Record object

The parm C<$user_data> may be omitted if C<$record_data> is an object,
in which case the user data will be gotten from it.

Returns a Flatfile::DataStore::Record object.

Note: the record data (but not user data) is stored in the FF::DS::Record
object as a scalar reference.  This is done for efficiency in the cases
where the record data may be very large.  Likewise, the first parm to
create() is allowed to be a scalar reference.

=cut

sub create {
    my( $self, $record_data, $user_data ) = @_;

    my $data_ref;
    if( defined $record_data ) {
        my $reftype = ref $record_data;
        unless( $reftype ) {
            $data_ref = \$record_data; }  # string
        elsif( $reftype eq "SCALAR" ) {
            $data_ref = $record_data; }
        elsif( $reftype =~ /Record/ ) {
            $data_ref = $record_data->data;
            $user_data = $record_data->user unless defined $user_data; }
        else {
            croak qq/Unrecognized: $reftype/; }
    }
    croak qq/No record data./ unless $data_ref;

    # get next keynum
    #   (we don't call nextkeynum(), because we need the
    #   $top_toc object for other things, too)

    my $top_toc = $self->new_toc( { int => 0 } );
    my $keyint  = $top_toc->keynum + 1;
    my $keylen  = $self->keylen;
    my $keybase = $self->keybase;
    my $keynum  = int2base $keyint, $keybase, $keylen;
    croak qq/Database exceeds configured size (keynum: "$keynum" too long)/
        if length $keynum > $keylen;

    # get keyfile
    #   need to lock files before getting seek positions
    #   want to lock keyfile before datafile

    my( $keyfile, $keyfint ) = $self->keyfile( $keyint );
    my $keyfh                = $self->locked_for_write( $keyfile );
    my $keyseek              = -s $keyfile;  # seekpos into keyfile

    # get datafile ($datafnum may increment)
    my $datafnum = $top_toc->datafnum || 1;  # (||1 only in create)
    $datafnum    = int2base $datafnum, $self->fnumbase, $self->fnumlen;
    my $reclen   = length $$data_ref;

    my $datafile;
    ( $datafile, $datafnum ) = $self->datafile( $datafnum, $reclen );
    my $datafh               = $self->locked_for_write( $datafile );
    my $dataseek             = -s $datafile;  # seekpos into datafile

    # get next transaction number
    my $transint = $self->nexttransnum( $top_toc );

    # make new record
    my $record = $self->new_record( {
        data     => $data_ref,
        preamble => {
            indicator => $self->crud->{'create'},
            transind  => $self->crud->{'create'},
            date      => now( $self->dateformat ),
            transnum  => $transint,
            keynum    => $keyint,
            reclen    => $reclen,
            thisfnum  => $datafnum,
            thisseek  => $dataseek,
            user      => $user_data,
            } } );

    # write record to datafile
    my $preamble = $record->preamble_string;
    my $dataline = $preamble . $$data_ref . $self->recsep;
    $self->write_bytes( $datafh, $dataseek, \$dataline );

    # write preamble to keyfile
    $self->write_bytes( $keyfh, $keyseek, \($preamble . $self->recsep) );
    
    # update table of contents (toc) file
    my $toc = $self->new_toc( { num => $datafnum } );

    # (note: datafnum and tocfnum are set in toc->new)
    $toc->keyfnum(   $keyfint          );
    $toc->keynum(    $keyint           );
    $toc->transnum(  $transint         );
    $toc->create(    $toc->create  + 1 );
    $toc->numrecs(   $toc->numrecs + 1 );
    $toc->write_toc( $toc->datafnum    );

    # update top toc
    $top_toc->datafnum( $toc->datafnum        );
    $top_toc->keyfnum(  $toc->keyfnum         );
    $top_toc->tocfnum(  $toc->tocfnum         );
    $top_toc->keynum(   $toc->keynum          );
    $top_toc->transnum( $toc->transnum        );
    $top_toc->create(   $top_toc->create  + 1 );
    $top_toc->numrecs(  $top_toc->numrecs + 1 );

    $top_toc->write_toc( 0 );

    return $record;
}

#---------------------------------------------------------------------

=head2 retrieve( $num[, $pos] )

Retrieves a record.  The parm C<$num> may be one of

 - a key number, i.e., record sequence number
 - a file number

The parm C<$pos> is required if C<$num> is a file number.

Returns a Flatfile::DataStore::Record object.

=cut

sub retrieve {
    my( $self, $num, $pos ) = @_;

    my $fnum;
    my $seekpos;
    my $keystring;

    if( defined $pos ) {
        $fnum    = $num;
        $seekpos = $pos;
    }
    else {
        my $keynum  = $num;
        my $recsep  = $self->recsep;
        my $keyseek = $self->keyseek( $keynum );

        my $keyfile = $self->keyfile( $keynum );
        my $keyfh   = $self->locked_for_read( $keyfile );

        my $trynum  = $self->lastkeynum;
        croak qq/Record doesn't exist: "$keynum"/ if $keynum > $trynum;

        $keystring = $self->read_preamble( $keyfh, $keyseek );
        my $parms  = $self->burst_preamble( $keystring );

        $fnum    = $parms->{'thisfnum'};
        $seekpos = $parms->{'thisseek'};
    }

    my $datafile = $self->which_datafile( $fnum );
    my $datafh   = $self->locked_for_read( $datafile );
    my $record   = $self->read_record( $datafh, $seekpos );

    # if we got the record via key file, check that preambles match
    if( $keystring ) {
        my $string = $record->preamble_string;
        croak qq/Mismatch "$string" vs. "$keystring"/ if $string ne $keystring;
    }

    return $record;
}

#---------------------------------------------------------------------

=head2 retrieve_preamble( $keynum )

Retrieves a preamble.  The parm C<$keynum> is a key number, i.e.,
record sequence number

Returns a Flatfile::DataStore::Preamble object.

This method allows getting information about the record, e.g., if
it's deleted, what's in the user data, etc., without the overhead of
retrieving the full record data.

=cut

sub retrieve_preamble {
    my( $self, $keynum ) = @_;

    my $keyseek = $self->keyseek( $keynum );
    my $keyfile = $self->keyfile( $keynum );
    my $keyfh   = $self->locked_for_read( $keyfile );

    my $trynum  = $self->lastkeynum;
    croak qq/Record doesn't exist: "$keynum"/ if $keynum > $trynum;

    my $keystring = $self->read_preamble( $keyfh, $keyseek );
    my $preamble  = $self->new_preamble( { string => $keystring } );

    return $preamble;
}

#---------------------------------------------------------------------

=head2 update( $object_or_string[, $record_data][, $user_data] )

Updates a record.  The parm $object_or_string may be one of:

 - FlatFile::DataStore::Record object
 - FlatFile::DataStore::Preamble object
 - Preamble string

The parms C<$record_data> and C<$user_data> may be omitted only if
C<$object_or_string> is a FF::DS::Record object, in which case the
record and user data will be gotten from it.

Returns a Flatfile::DataStore::Record object.

Note: the record data (but not user data) is stored in the FF::DS::Record
object as a scalar reference.  This is done for efficiency in the cases
where the record data may be very large.  Likewise, the first parm to
create() is allowed to be a scalar reference.

=cut

sub update {
    my $self = shift;
    my( $obj, $data_ref, $user_data ) = $self->normalize_parms( @_ );

    my $prevnext = $self->prevfnum;  # boolean

    my $prevpreamble = $obj->string;
    my $keyint       = $obj->keynum;
    my $prevind      = $obj->indicator;
    my $prevfnum     = $obj->thisfnum;
    my $prevseek     = $obj->thisseek;

    # update is okay for these:
    my $create = $self->crud->{'create'};
    my $update = $self->crud->{'update'};
    my $delete = $self->crud->{'delete'};

    croak qq/update not allowed: "$prevind"/
        unless $prevind =~ /[\Q$create$update$delete\E]/;

    # get keyfile
    #   need to lock files before getting seek positions
    #   want to lock keyfile before datafile

    my( $keyfile, $keyfint ) = $self->keyfile( $keyint );
    my $keyfh                = $self->locked_for_write( $keyfile );
    my $keyseek              = $self->keyseek( $keyint );

    my $try = $self->read_preamble( $keyfh, $keyseek );
    croak qq/Mismatch [$try] [$prevpreamble]/ unless $try eq $prevpreamble;

    # get datafile ($datafnum may increment)
    my $top_toc  = $self->new_toc( { int => 0 } );
    my $datafnum = int2base $top_toc->datafnum, $self->fnumbase, $self->fnumlen;
    my $reclen   = length $$data_ref;

    my $datafile;
    ( $datafile, $datafnum ) = $self->datafile( $datafnum, $reclen );
    my $datafh               = $self->locked_for_write( $datafile );
    my $dataseek             = -s $datafile;  # seekpos into datafile

    # get next transaction number
    my $transint = $self->nexttransnum( $top_toc );

    # make new record
    my $preamble_hash = {
        indicator => $update,
        transind  => $update,
        date      => now( $self->dateformat ),
        transnum  => $transint,
        keynum    => $keyint,
        reclen    => $reclen,
        thisfnum  => $datafnum,
        thisseek  => $dataseek,
        user      => $user_data,
        };
    if( $prevnext ) {
        $preamble_hash->{'prevfnum'} = $prevfnum;
        $preamble_hash->{'prevseek'} = $prevseek;
    }
    my $record = $self->new_record( {
        data     => $data_ref,
        preamble => $preamble_hash,
        } );

    # write record to datafile
    my $preamble = $record->preamble_string;
    my $dataline = $preamble . $$data_ref . $self->recsep;
    $self->write_bytes( $datafh, $dataseek, \$dataline );

    # write preamble to keyfile (recsep there already)
    $self->write_bytes( $keyfh, $keyseek, \$preamble );

    # update the old preamble
    if( $prevnext ) {
        $prevpreamble = $self->update_preamble( $prevpreamble, {
            indicator => $self->crud->{ 'oldupd' },
            nextfnum  => $datafnum,
            nextseek  => $dataseek,
            } );
        my $prevdatafile = $self->which_datafile( $prevfnum );
        my $prevdatafh   = $self->locked_for_write( $prevdatafile );
        $self->write_bytes( $prevdatafh, $prevseek, \$prevpreamble );
    }

    # update table of contents (toc) file
    my $toc = $self->new_toc( { num => $datafnum } );

    # note: datafnum and tocfnum are set in toc->new
    $toc->keyfnum(  $top_toc->keyfnum );  # keep last nums going
    $toc->keynum(   $top_toc->keynum  );
    $toc->transnum( $transint         );
    $toc->update(   $toc->update  + 1 );
    $toc->numrecs(  $toc->numrecs + 1 );

    # was the previous record in another data file?
    if( $prevnext ) {
        if( $prevfnum ne $datafnum ) {
            my $prevtoc = $self->new_toc( { num => $prevfnum } );
            $prevtoc->oldupd(    $prevtoc->oldupd  + 1 );
            $prevtoc->numrecs(   $prevtoc->numrecs - 1 ) if $prevind ne $delete;
            $prevtoc->write_toc( $prevtoc->datafnum    );
        }
        else {
            $toc->oldupd(  $toc->oldupd  + 1 );
            $toc->numrecs( $toc->numrecs - 1 ) if $prevind ne $delete;
        }
    }
    else {
        $toc->numrecs( $toc->numrecs - 1 ) if $prevind ne $delete;
    }

    $toc->write_toc( $toc->datafnum );

    # update top toc
    $top_toc->datafnum( $toc->datafnum        );
    $top_toc->tocfnum(  $toc->tocfnum         );
    $top_toc->transnum( $toc->transnum        );
    $top_toc->update(   $top_toc->update  + 1 );
    $top_toc->oldupd(   $top_toc->oldupd  + 1 ) if $prevnext;
    $top_toc->numrecs(  $top_toc->numrecs + 1 ) if $prevind eq $delete;

    $top_toc->write_toc( 0 );

    return $record;
}

#---------------------------------------------------------------------

=head2 delete( $object_or_string[, $record_data][, $user_data] )

Deletes a record.  The parm $object_or_string may be one of:

 - FlatFile::DataStore::Record object
 - FlatFile::DataStore::Preamble object
 - Preamble string

The parms C<$record_data> and C<$user_data> may be omitted only if
C<$object_or_string> is a FF::DS::Record object, in which case the
record and user data will be gotten from it.

Returns a Flatfile::DataStore::Record object.

Note: the record data (but not user data) is stored in the FF::DS::Record
object as a scalar reference.  This is done for efficiency in the cases
where the record data may be very large.  Likewise, the first parm to
create() is allowed to be a scalar reference.

=cut

sub delete {
    my $self = shift;
    my( $obj, $data_ref, $user_data ) = $self->normalize_parms( @_ );

    my $prevnext = $self->prevfnum;  # boolean

    my $prevpreamble = $obj->string;
    my $keyint       = $obj->keynum;
    my $prevind      = $obj->indicator;
    my $prevfnum     = $obj->thisfnum;
    my $prevseek     = $obj->thisseek;

    # delete is okay for these:
    my $create = $self->crud->{'create'};
    my $update = $self->crud->{'update'};

    croak qq/'delete' not allowed: "$prevind"/
        unless $prevind =~ /[\Q$create$update\E]/;

    # get keyfile
    # need to lock files before getting seek positions
    # want to lock keyfile before datafile
    my( $keyfile, $keyfint ) = $self->keyfile( $keyint );
    my $keyfh                = $self->locked_for_write( $keyfile );
    my $keyseek              = $self->keyseek( $keyint );

    my $try = $self->read_preamble( $keyfh, $keyseek );
    croak qq/Mismatch [$try] [$prevpreamble]/ unless $try eq $prevpreamble;

    # get datafile ($datafnum may increment)
    my $top_toc  = $self->new_toc( { int => 0 } );
    my $datafnum = int2base $top_toc->datafnum, $self->fnumbase, $self->fnumlen;
    my $reclen   = length $$data_ref;

    my $datafile;
    ( $datafile, $datafnum ) = $self->datafile( $datafnum, $reclen );
    my $datafh               = $self->locked_for_write( $datafile );
    my $dataseek             = -s $datafile;  # seekpos into datafile

    # get next transaction number
    my $transint = $self->nexttransnum( $top_toc );

    # make new record
    my $delete = $self->crud->{'delete'};
    my $preamble_hash = {
        indicator => $delete,
        transind  => $delete,
        date      => now( $self->dateformat ),
        transnum  => $transint,
        keynum    => $keyint,
        reclen    => $reclen,
        thisfnum  => $datafnum,
        thisseek  => $dataseek,
        user      => $user_data,
        };
    if( $prevnext ) {
        $preamble_hash->{'prevfnum'} = $prevfnum;
        $preamble_hash->{'prevseek'} = $prevseek;
    }
    my $record = $self->new_record( {
        data     => $data_ref,
        preamble => $preamble_hash,
        } );

    # write record to datafile
    my $preamble = $record->preamble_string;
    my $dataline = $preamble . $$data_ref . $self->recsep;
    $self->write_bytes( $datafh, $dataseek, \$dataline );

    # write preamble to keyfile (recsep there already)
    $self->write_bytes( $keyfh, $keyseek, \$preamble );

    # update the old preamble
    if( $prevnext ) {
        $prevpreamble = $self->update_preamble( $prevpreamble, {
            indicator => $self->crud->{ 'olddel' },
            nextfnum  => $datafnum,
            nextseek  => $dataseek,
            } );
        my $prevdatafile = $self->which_datafile( $prevfnum );
        my $prevdatafh   = $self->locked_for_write( $prevdatafile );
        $self->write_bytes( $prevdatafh, $prevseek, \$prevpreamble );
    }

    # update table of contents (toc) file
    my $toc = $self->new_toc( { num => $datafnum } );

    # note: datafnum and tocfnum are set in toc->new
    $toc->keyfnum(  $top_toc->keyfnum );  # keep last nums going
    $toc->keynum(   $top_toc->keynum  );
    $toc->transnum( $transint         );
    $toc->delete(   $toc->delete + 1  );

    # was the previous record in another data file?
    if( $prevnext ) {
        if( $prevfnum ne $datafnum ) {
            my $prevtoc = $self->new_toc( { num => $prevfnum } );
            $prevtoc->olddel(    $prevtoc->olddel  + 1 );
            $prevtoc->numrecs(   $prevtoc->numrecs - 1 );
            $prevtoc->write_toc( $prevtoc->datafnum    );
        }
        else {
            $toc->olddel(  $toc->olddel  + 1 );
            $toc->numrecs( $toc->numrecs - 1 );
        }
    }
    else {
        $toc->numrecs( $toc->numrecs - 1 );
    }

    $toc->write_toc( $toc->datafnum );

    # update top toc
    $top_toc->datafnum( $toc->datafnum        );
    $top_toc->tocfnum(  $toc->tocfnum         );
    $top_toc->transnum( $toc->transnum        );
    $top_toc->delete(   $top_toc->delete  + 1 );
    $top_toc->olddel(   $top_toc->olddel  + 1 ) if $prevnext;
    $top_toc->numrecs(  $top_toc->numrecs - 1 );

    $top_toc->write_toc( 0 );

    return $record;
}

#---------------------------------------------------------------------
# $obj         may be preamble string, preamble obj, or record obj
# $record_data may be data string, scalar ref, or record obj
# $user_data   may be data string
#
# $user_data, if not given, will be gotten from $record_data or $obj
# $record_data, if not given, will be gotten from $obj
#
# Private method.

sub normalize_parms {
    my( $self, $obj, $record_data, $user_data ) = @_;

    croak qq/Bad call./ unless $obj;

    # set the preamble object
    my( $preamble, $data_ref, $try_user );
    my $reftype = ref $obj;
    if(   !$reftype ) {  # preamble string
        $preamble = $self->new_preamble( { string => $obj } ); }
    elsif( $reftype =~ /Preamble/ ) {
        $preamble = $obj; }
    elsif( $reftype =~ /Record/ ) {
        $preamble = $obj->preamble;
        $data_ref = $obj->data; }
    else {
        croak qq/Unrecognized: $reftype/; }
    $try_user = $preamble->user;

    # set the record data
    if( defined $record_data ) {
        my $reftype = ref $record_data;
        if(   !$reftype ) {
            $data_ref = \$record_data; }  # string
        elsif( $reftype eq "SCALAR" ) {
            $data_ref = $record_data; }
        elsif( $reftype =~ /Record/ ) {
            $data_ref = $record_data->data;
            $try_user = $record_data->user; }
        else {
            croak qq/Unrecognized: $reftype/; }
    }
    croak qq/No record data./ unless $data_ref;

    # set the user data
    $user_data = $try_user unless defined $user_data;

    return $preamble, $data_ref, $user_data;
}

#---------------------------------------------------------------------

=head2 history( $keynum )

Retrieves a record's history.  The parm C<$keynum> is always a key
number, i.e., a record sequence number.

Returns an array of FlatFile::DataStore::Record objects.

The first element of this array is the current record.  The last
element is the original record.  That is, the array is in reverse
chronological order.

=cut

sub history {
    my( $self, $keynum ) = @_;

    my @history;

    my $rec = $self->retrieve( $keynum );
    push @history, $rec;

    my $prevfnum = $rec->prevfnum;
    my $prevseek = $rec->prevseek;

    while( $prevfnum ) {

        my $rec = $self->retrieve( $prevfnum, $prevseek );
        push @history, $rec;

        $prevfnum = $rec->prevfnum;
        $prevseek = $rec->prevseek;
    }

    return @history;
}

#---------------------------------------------------------------------

=head1 OBJECT METHODS, Accessors

=head2 $ds->specs( [$omap] )

Sets and returns the C<specs> attribute value if C<$omap> is given,
otherwise just returns the value.

An 'omap' is an ordered hash as defined in

 http://yaml.org/type/omap.html

That is, it's an array of single-key hashes.  This ordered hash
contains the specifications for constructing and parsing a record
preamble as defined in the name.uri file.

=cut

sub specs {
    my( $self, $omap ) = @_;
    for( $self->{specs} ) {
        if( $omap ) {
            croak qq/Invalid omap: /.omap_errstr()
                unless omap_is_valid( $omap );
            $_ = $omap;
        }
        return unless defined;
        return @$_ if wantarray;
        return $_;
    }
}

#---------------------------------------------------------------------

=head2 $ds->dir( [$dir] )

Sets and returns the C<dir> attribute value if C<$dir> is given,
otherwise just returns the value.

If C<$dir> is given and is a null string, the C<dir> object attribute
is removed from the object.  If C<$dir> is not null, the directory
must already exist.

=cut

sub dir {
    my( $self, $dir ) = @_;
    if( defined $dir and $dir eq "" ) { delete $self->{dir} }
    else {
        for( $self->{dir} ) {
            if( defined $dir ) {
                croak qq/$dir doesn't exist/ unless -d $dir;
                $_ = $dir
            }
            return $_;
        }
    }
}

#---------------------------------------------------------------------

=head2 Preamble accessors (from the uri)

The following methods set and return their respective attribute values
if C<$value> is given.  Otherwise, they just return the value.

 $ds->indicator( [$value] );  # length-characters
 $ds->transind(  [$value] );  # length-characters
 $ds->date(      [$value] );  # length-format
 $ds->transnum(  [$value] );  # length-base
 $ds->keynum(    [$value] );  # length-base
 $ds->reclen(    [$value] );  # length-base
 $ds->thisfnum(  [$value] );  # length-base
 $ds->thisseek(  [$value] );  # length-base
 $ds->prevfnum(  [$value] );  # length-base
 $ds->prevseek(  [$value] );  # length-base
 $ds->nextfnum(  [$value] );  # length-base
 $ds->nextseek(  [$value] );  # length-base
 $ds->user(      [$value] );  # length-characters

=head2 Other accessors

 $ds->name(        [$value] ); # from uri, name of data store
 $ds->desc(        [$value] ); # from uri, description of data store
 $ds->recsep(      [$value] ); # from uri, character(s)
 $ds->uri(         [$value] ); # full uri as is
 $ds->preamblelen( [$value] ); # length of preamble string
 $ds->toclen(      [$value] ); # length of toc entry
 $ds->keylen(      [$value] ); # length of stored keynum
 $ds->keybase(     [$value] ); # base   of stored keynum
 $ds->translen(    [$value] ); # length of stored transaction number
 $ds->transbase(   [$value] ); # base   of stored transation number
 $ds->fnumlen(     [$value] ); # length of stored file number
 $ds->fnumbase(    [$value] ); # base   of stored file number
 $ds->dateformat(  [$value] ); # format from uri
 $ds->regx(        [$value] ); # capturing regx for preamble string
 $ds->datamax(     [$value] ); # maximum bytes in a data file
 $ds->crud(        [$value] ); # hash ref, e.g.,

     {
        create => '+',
        oldupd => '#',
        update => '=',
        olddel => '*',
        delete => '-',
        '+' => 'create',
        '#' => 'oldupd',
        '=' => 'update',
        '*' => 'olddel',
        '-' => 'delete',
     }

 (logical actions <=> symbolic indicators)

=head2 Accessors for optional attributes

 $ds->dirmax( [$value] );  # maximum files in a directory
 $ds->dirlev( [$value] );  # number of directory levels
 $ds->tocmax( [$value] );  # maximum toc entries
 $ds->keymax( [$value] );  # maximum key entries

If no C<dirmax>, directories will keep being added to.

If no C<dirlev>, toc, key, and data files will reside in top-level
directory.  If C<dirmax> given, C<dirlev> defaults to 1.

If no C<tocmax>, there will be only one toc file, which will grow
indefinitely.

If no C<keymax>, there will be only one key file, which will grow
indefinitely.

=cut

sub indicator {for($_[0]->{indicator} ){$_=$_[1]if@_>1;return$_}}
sub transind  {for($_[0]->{transind}  ){$_=$_[1]if@_>1;return$_}}
sub date      {for($_[0]->{date}      ){$_=$_[1]if@_>1;return$_}}
sub transnum  {for($_[0]->{transnum}  ){$_=$_[1]if@_>1;return$_}}
sub keynum    {for($_[0]->{keynum}    ){$_=$_[1]if@_>1;return$_}}
sub reclen    {for($_[0]->{reclen}    ){$_=$_[1]if@_>1;return$_}}
sub thisfnum  {for($_[0]->{thisfnum}  ){$_=$_[1]if@_>1;return$_}}
sub thisseek  {for($_[0]->{thisseek}  ){$_=$_[1]if@_>1;return$_}}

# prevfnum, prevseek, nextfnum, nextseek are optional attributes;
# prevfnum() is set up to avoid autovivification, because it is
# the accessor used to test if these optional attributes are set

sub prevfnum {
    my $self = shift;
    return $self->{prevfnum} = $_[0] if @_;
    return $self->{prevfnum} if exists $self->{prevfnum};
}

sub prevseek  {for($_[0]->{prevseek}  ){$_=$_[1]if@_>1;return$_}}
sub nextfnum  {for($_[0]->{nextfnum}  ){$_=$_[1]if@_>1;return$_}}
sub nextseek  {for($_[0]->{nextseek}  ){$_=$_[1]if@_>1;return$_}}
sub user      {for($_[0]->{user}      ){$_=$_[1]if@_>1;return$_}}

sub name        {for($_[0]->{name}        ){$_=$_[1]if@_>1;return$_}}
sub desc        {for($_[0]->{desc}        ){$_=$_[1]if@_>1;return$_}}
sub recsep      {for($_[0]->{recsep}      ){$_=$_[1]if@_>1;return$_}}
sub uri         {for($_[0]->{uri}         ){$_=$_[1]if@_>1;return$_}}
sub dateformat  {for($_[0]->{dateformat}  ){$_=$_[1]if@_>1;return$_}}
sub regx        {for($_[0]->{regx}        ){$_=$_[1]if@_>1;return$_}}
sub crud        {for($_[0]->{crud}        ){$_=$_[1]if@_>1;return$_}}
sub datamax     {for($_[0]->{datamax}     ){$_=$_[1]if@_>1;return$_}}

sub preamblelen {for($_[0]->{preamblelen} ){$_=0+$_[1]if@_>1;return$_}}
sub toclen      {for($_[0]->{toclen}      ){$_=0+$_[1]if@_>1;return$_}}
sub keylen      {for($_[0]->{keylen}      ){$_=0+$_[1]if@_>1;return$_}}
sub keybase     {for($_[0]->{keybase}     ){$_=0+$_[1]if@_>1;return$_}}
sub translen    {for($_[0]->{translen}    ){$_=0+$_[1]if@_>1;return$_}}
sub transbase   {for($_[0]->{transbase}   ){$_=0+$_[1]if@_>1;return$_}}
sub fnumlen     {for($_[0]->{fnumlen}     ){$_=0+$_[1]if@_>1;return$_}}
sub fnumbase    {for($_[0]->{fnumbase}    ){$_=0+$_[1]if@_>1;return$_}}

# optional (set up to avoid autovivification):

sub dirmax {
    my $self = shift;
    return $self->{dirmax} = $_[0] if @_;
    return $self->{dirmax} if exists $self->{dirmax};
}
sub dirlev {
    my $self = shift;
    return $self->{dirlev} = 0+$_[0] if @_;
    return $self->{dirlev} if exists $self->{dirlev};
}
sub tocmax {
    my $self = shift;
    return $self->{tocmax} = $_[0] if @_;
    return $self->{tocmax} if exists $self->{tocmax};
}
sub keymax {
    my $self = shift;
    return $self->{keymax} = $_[0] if @_;
    return $self->{keymax} if exists $self->{keymax};
}

# default to null string (will get space-padded)
sub userdata {
    my $self = shift;
    return $self->{userdata} = $_[0] if @_;
    return '' unless exists $self->{userdata};
    return $self->{userdata};
}

#---------------------------------------------------------------------
# new_toc( \%parms )
#     This method is a wrapper for FlatFile::DataStore::Toc->new().
#
# Private method.

sub new_toc {
    my( $self, $parms ) = @_;
    $parms->{'datastore'} = $self;
    FlatFile::DataStore::Toc->new( $parms );
}

#---------------------------------------------------------------------
# new_preamble( \%parms )
#     This method is a wrapper for
#     FlatFile::DataStore::Preamble->new().
#
# Private method.

sub new_preamble {
    my( $self, $parms ) = @_;
    $parms->{'datastore'} = $self;
    FlatFile::DataStore::Preamble->new( $parms );
}

#---------------------------------------------------------------------
# new_record( \%parms )
#     This method is a wrapper for FlatFile::DataStore::Record->new().
#
# Private method.

sub new_record {
    my( $self, $parms ) = @_;
    my $preamble = $parms->{'preamble'};
    if( ref $preamble eq 'HASH' ) {  # not an object
        $parms->{'preamble'} = $self->new_preamble( $preamble );
    }
    FlatFile::DataStore::Record->new( $parms );
}

#---------------------------------------------------------------------
# keyfile()
#    takes an integer that is the record sequence number and returns
#    the path to the keyfile where that record's preamble is
#
# Private method.

sub keyfile {
    my( $self, $keyint ) = @_;

    my $name     = $self->name;
    my $fnumlen  = $self->fnumlen;
    my $fnumbase = $self->fnumbase;

    my $keyfint = 1;
    my $keyfile = $name;

    # get key file number (if any) based on keymax and keyint
    if( my $keymax = $self->keymax ) {
        $keyfint = int( $keyint / $keymax ) + 1;
        my $keyfnum = int2base $keyfint, $fnumbase, $fnumlen;
        croak qq/Database exceeds configured size (keyfnum: "$keyfnum" too long)/
            if length $keyfnum > $fnumlen;
        $keyfile .= ".$keyfnum";
    }

    $keyfile .= ".key";

    # get path based on dirlev, dirmax, and key file number
    if( my $dirlev = $self->dirlev ) {
        my $dirmax = $self->dirmax;
        my $path   = "";
        my $this   = $keyfint;
        for( 1 .. $dirlev ) {
            my $dirint = $dirmax? (int( ( $this - 1 ) / $dirmax ) + 1): 1;
            my $dirnum = int2base $dirint, $fnumbase, $fnumlen;
            $path = $path? "$dirnum/$path": $dirnum;
            $this = $dirint;
        }
        $path = $self->dir . "/$name/key$path";
        mkpath( $path ) unless -d $path;
        $keyfile = "$path/$keyfile";
    }
    else {
        $keyfile = $self->dir . "/$keyfile";
    }

    return ( $keyfile, $keyfint ) if wantarray;
    return $keyfile;

}

#---------------------------------------------------------------------
# datafile(), called by create(), update(), and delete()
#     Similarly to which_datafile(), this method takes a file number
#     and returns the path to that datafile.  Unlike which_datafile(),
#     this method also takes a record length to check for overflow.
#     That is, if the record about to be written would make a datafile
#     become too large (> datamax), the file number is incremented,
#     and the path to that new datafile is returned--along with the
#     new file number.  Calls to datafile() should always take this
#     new file number into account.
#
#     Will croak if the record is way too big or if the new file
#     number is longer than the max length for file numbers.  In
#     either case, a new data store must be configured to handle the
#     extra data, and the old data store must be migrated to it.
#
# Private method.

sub datafile {
    my( $self, $fnum, $reclen ) = @_;

    my $datafile = $self->which_datafile( $fnum );

    # check if we're about to overfill the data file
    # and if so, increment fnum for new datafile
    my $datamax   = $self->datamax;
    my $checksize = $self->preamblelen + $reclen + length $self->recsep;
    my $datasize = -s $datafile || 0;

    if( $datasize + $checksize > $datamax ) {

        croak qq/Record too long/ if $checksize > $datamax;
        my $fnumlen  = $self->fnumlen;
        my $fnumbase = $self->fnumbase;
        $fnum = int2base( 1 + base2int( $fnum, $fnumbase ), $fnumbase, $fnumlen );
        croak qq/Database exceeds configured size (fnum: "$fnum" too long)/
            if length $fnum > $fnumlen;

        $datafile = $self->which_datafile( $fnum );
    }

    return $datafile, $fnum;
}

#---------------------------------------------------------------------
# which_datafile()
#     Takes a file number and returns the path to that datafile.
#     Takes into account dirlev and dirmax, if set, and will create
#     new directories as needed.
#
# Private method.

sub which_datafile {
    my( $self, $datafnum ) = @_;

    my $name     = $self->name;
    my $datafile = "$name.$datafnum.data";

    # get path based on dirlev, dirmax, and data file number
    if( my $dirlev   = $self->dirlev ) {
        my $fnumlen  = $self->fnumlen;
        my $fnumbase = $self->fnumbase;
        my $dirmax   = $self->dirmax;
        my $path     = "";
        my $this     = base2int $datafnum, $fnumbase;
        for( 1 .. $dirlev ) {
            my $dirint = $dirmax? (int( ( $this - 1 ) / $dirmax ) + 1): 1;
            my $dirnum = int2base $dirint, $fnumbase, $fnumlen;
            $path = $path? "$dirnum/$path": $dirnum;
            $this = $dirint;
        }
        $path = $self->dir . "/$name/data$path";
        mkpath( $path ) unless -d $path;
        $datafile = "$path/$datafile";
    }
    else {
        $datafile = $self->dir . "/$datafile";
    }

    return $datafile;
}

#---------------------------------------------------------------------
# sub all_datafiles(), called by validate utility
#     Returns an array of paths for all of the data files in the data
#     store.
#
# Private method.

sub all_datafiles {
    my( $self ) = @_;

    my $fnumlen  = $self->fnumlen;
    my $fnumbase = $self->fnumbase;
    my $top_toc  = $self->new_toc( { int => 0 } );
    my $datafint = $top_toc->datafnum;
    my @files;
    for( 1 .. $datafint ) {
        my $datafnum = int2base $_, $fnumbase, $fnumlen;
        push @files, $self->which_datafile( $datafnum );
    }
    return @files;
}

#---------------------------------------------------------------------

=head1 OBJECT METHODS, Other

=head2 howmany( [$regx] )

Returns count of records whose indicators match regx, e.g.,

    $self->howmany( qr/create|update/ );
    $self->howmany( qr/delete/ );
    $self->howmany( qr/oldupd|olddel/ );

If no regx, howmany() returns numrecs from the toc file, which
should give the same number as qw/create|update/.

=cut

sub howmany {
    my( $self, $regx ) = @_;

    my $top_toc = $self->new_toc( { int => 0 } );

    return $top_toc->numrecs unless $regx;

    my $howmany = 0;
    for( qw( create update delete oldupd olddel ) ) {
        $howmany += $top_toc->$_() if /$regx/ }
    return $howmany;
}

#---------------------------------------------------------------------

=head2 lastkeynum()

Returns the last key number used, i.e., the sequence number of the
last record added to the data store, as an integer.

=cut

sub lastkeynum {
    my( $self ) = @_;

    my $top_toc = $self->new_toc( { int => 0 } );
    my $keyint  = $top_toc->keynum;

    return $keyint;
}

=head2 nextkeynum()

Returns lastkeynum()+1 (a convenience method).  This would be useful
for adding a new record to a hash tied to a data store, e.g.,

    $h{ $ds->nextkeynum } = "New record data.";

=cut

sub nextkeynum {
   for( $_[0]->lastkeynum ) {
       return 0 unless defined;
       return $_ + 1;
   }
}

#---------------------------------------------------------------------
# keyseek(), seek to a particular line in the key file
#     Takes the record sequence number as an integer and returns
#     the seek position needed to retrieve the record's preamble from
#     the pertinent keyfile.  Interestingly, this seek position is
#     only a function of the keyint and keymax values, so this
#     routine doesn't need to know which keyfile we're seeking into.
#
# Private method.
            
sub keyseek {
    my( $self, $keyint ) = @_;

    my $keylen = $self->preamblelen + length( $self->recsep );

    my $keyseek;
    if( my $keymax = $self->keymax ) {
        my $skip = int( $keyint / $keymax );
        $keyseek = $keylen * ( $keyint - ( $skip * $keymax ) ); }
    else {
        $keyseek = $keylen * $keyint; }

    return $keyseek;
}

#---------------------------------------------------------------------
# nexttransnum(), get next transaction number
#     Takes a FF::DS::Toc (table of contents) object, which should be
#     "top" Toc that has many of the key values for the data store.
#     Returns the next transaction number as an integer.
#     Will croak if this number is longer than allowed by the current
#     configuration.  In that case, a new datastore that allows for
#     more transactions must be configured and the old data store
#     migrated to it.
#
# Private method.

sub nexttransnum {
    my( $self, $top_toc ) = @_;

    $top_toc ||= $self->new_toc( { int => 0 } );

    my $transint  = $top_toc->transnum + 1;
    my $translen  = $self->translen;
    my $transbase = $self->transbase;
    my $transnum  = int2base $transint, $transbase, $translen;
    croak qq/Database exceeds configured size (transnum: "$transnum" too long)/
        if length $transnum > $translen;

    return $transint;
}

#---------------------------------------------------------------------
# burst_pramble(), called various places to parse preamble string
#     Takes a preamble string (as stored on disk) and parses out all
#     of the values, based on regx and specs.  Returns a hash ref of
#     these values.  Called by FF::DS::Preamble->new() to create an
#     object from a string, and by retrieve() to get the file number
#     and seek pos for reading a record.
#
# Private method.

sub burst_preamble {
    my( $self, $string ) = @_;
    croak qq/No preamble to burst/ unless $string;

    my @fields = $string =~ $self->regx;
    croak qq/Something is wrong with "$string"/ unless @fields;

    my %parms;
    my $i;
    for( $self->specs ) {  # specs() returns an array of hashrefs
        my( $key, $aref )       = %$_;
        my( $pos, $len, $parm ) = @$aref;
        my $field = $fields[ $i++ ];
        for( $key ) {
            if( /indicator|transind|date/ ) {
                $parms{ $key } = $field;
            }
            elsif( /user/ ) {
                my $try = $field;
                $try =~ s/\s+$//;
                $parms{ $key } = $try;
            }
            elsif( /fnum/ ) {
                next if $field =~ /^-+$/;
                $parms{ $key } = $field;
            }
            else {
                next if $field =~ /^-+$/;
                $parms{ $key } = base2int( $field, $parm );
            }
        }
    }
    return \%parms;
}

#---------------------------------------------------------------------
# update_preamble(), called by update() and delete() to flag old recs
#     Take a preamble string and a hash ref of values to change, and
#     returns a new preamble string with those values changed.  Will
#     croak if the new preamble does match the regx attribute
#
# Private method.

sub update_preamble {
    my( $self, $preamble, $parms ) = @_;

    my $omap = $self->specs;

    for( keys %$parms ) {

        my $value = $parms->{ $_ };
        my( $pos, $len, $parm ) = @{omap_get_values( $omap, $_ )};

        my $try;
        if( /indicator|transind|date|user/ ) {
            $try = sprintf "%-${len}s", $value;
            croak qq/Invalid value for "$_" ($try)/
                unless $try =~ $Ascii_chars;
        }
        # the fnums should be in their base form already
        elsif( /fnum/ ) {
            $try = sprintf "%0${len}s", $value;
        }
        else {
            $try = int2base $value, $parm, $len;
        }
        croak qq/Value of "$_" ($try) too long/ if length $try > $len;

        substr $preamble, $pos, $len, $try;  # update the field
    }

    croak qq/Something is wrong with preamble: "$preamble"/
        unless $preamble =~ $self->regx;

    return $preamble;
}

#---------------------------------------------------------------------
# file read/write:
#---------------------------------------------------------------------

#---------------------------------------------------------------------
# DESTROY() supports tied and untied objects

sub DESTROY {
    my $self = shift;
    $self->close_files;
}

#---------------------------------------------------------------------

=head2 close_files()

This routine will close all open files associated with the data store
object.  This is used in DESTROY(), but could conceivably be called by
the application if it detects too many open files.

 $ds->close_files();

The intention is that close_files() can be called any time -- new files
would be opened again as needed.

=cut

sub close_files {
    my $self = shift;

    if( my $href = $Read_fh{ $self } ) {
        while( my( $file, $fh ) = each %$href ) {
            close $fh or die "Can't close $file: $!";
        }
        delete $Read_fh{ $self };
    }

    if( my $href = $Write_fh{ $self } ) {
        while( my( $file, $fh ) = each %$href ) {
            close $fh or die "Can't close $file: $!";
        }
        delete $Write_fh{ $self };
    }
}

#---------------------------------------------------------------------
# locked_for_read()
#     Takes a file name, opens it for input, locks it, and returns the
#     open file handle.  It caches this file handle, and the cached
#     handle will be returned instead if it exists in the cache.
#
# Private method.

sub locked_for_read {
    my( $self, $file ) = @_;

    my $open_fh = $Read_fh{ $self }{ $file };
    return $open_fh if $open_fh;

    my $fh;
    open $fh, '<', $file or croak "Can't open for read $file: $!";
    flock $fh, LOCK_SH   or croak "Can't lock shared $file: $!";
    binmode $fh;

    $Read_fh{ $self }{ $file } = $fh;
    return $fh;
}

#---------------------------------------------------------------------
# locked_for_write()
#     Takes a file name, opens it for read/write, locks it, and
#     returns the open file handle.  It caches this file handle, and
#     the cached handle will be returned instead if it exists in the
#     cache.
#
# Private method.

sub locked_for_write {
    my( $self, $file ) = @_;

    my $open_fh = $Write_fh{ $self }{ $file };
    return $open_fh if $open_fh;

    # remove possible shared lock on file
    if( exists $Read_fh{ $self }{ $file } ) {
        close  $Read_fh{ $self }{ $file };
        delete $Read_fh{ $self }{ $file };
    }

    my $fh;
    sysopen( $fh, $file, O_RDWR|O_CREAT ) or croak "Can't open for read/write $file: $!";
    my $ofh = select( $fh ); $| = 1; select ( $ofh );  # flush buffers
    flock $fh, LOCK_EX                    or croak "Can't lock exclusive $file: $!";
    binmode $fh;

    $Write_fh{ $self }{ $file } = $fh;
    return $fh;
}

#---------------------------------------------------------------------
# read_record()
#     Takes an open file handle and a seek position and
#     - seeks there to read the preamble
#     - seeks to the record data and reads that
#     - returns a record object created from the preamble and data
#
# Private method.

sub read_record {
    my( $self, $fh, $seekpos ) = @_;

    # we don't call read_preamble() because we need len anyway
    my $len  = $self->preamblelen;
    my $sref = $self->read_bytes( $fh, $seekpos, $len ); 
    my $preamble = $self->new_preamble( { string => $$sref } );

    $seekpos    += $len;
    $len         = $preamble->reclen;
    my $recdata  = $self->read_bytes( $fh, $seekpos, $len ); 

    my $record = $self->new_record( {
        preamble => $preamble,
        data     => $recdata,  # scalar ref
        } );

    return $record;
}

#---------------------------------------------------------------------
# read_preamble()
#     Takes an open file handle (probably the key file) and a seek
#     position and
#     - seeks there to read the preamble
#     - returns the preamble string (not an object)
#
# Private method.

sub read_preamble {
    my( $self, $fh, $seekpos ) = @_;

    my $len  = $self->preamblelen;
    my $sref = $self->read_bytes( $fh, $seekpos, $len ); 

    return $$sref;  # want the string, not the ref
}

#---------------------------------------------------------------------
# read_bytes()
#     Takes an open file handle, a seek position and a length, reads
#     that many bytes from that position, and returns a scalar
#     reference to that data.  It is expected that the file is set
#     to binmode.
#
# Private method.

sub read_bytes {
    my( $self, $fh, $seekpos, $len ) = @_;

    my $string;
    sysseek $fh, $seekpos, 0 or croak "Can't seek: $!";
    my $rc = sysread $fh, $string, $len;
    croak "Can't read: $!" unless defined $rc;

    return \$string;
}

#---------------------------------------------------------------------
# write_bytes()
#     Takes an open file handle, a seek position, and a scalar
#     reference and writes that data to the file at that position.
#     It is expected that the file is set to binmode.
#
# Private method.

sub write_bytes {
    my( $self, $fh, $seekpos, $sref ) = @_;

    sysseek  $fh, $seekpos, 0 or croak "Can't seek: $!";
    syswrite $fh, $$sref      or croak "Can't write: $!";

}

#---------------------------------------------------------------------
# read_file()
#     Takes a file name, locks it for reading, and returnes the
#     contents as an array of lines
#
# Private method.

sub read_file {
    my( $self, $file ) = @_;

    my $fh = $self->locked_for_read( $file );
    return <$fh>;
}

#---------------------------------------------------------------------
# now(), expects 'yyyymmdd' or 'yymd' (or 'mmddyyyy', 'mdyy', etc.)
#     Returns current date formatted as requested.
#
# Private method.

sub now {
    my( $format ) = @_;
    my( $y, $m, $d ) =
        sub{($_[5]+1900,$_[4]+1,$_[3])}->(localtime);
    for( $format ) {
        if( /yyyy/ ) {  # decimal year/month/day
            s/ yyyy / sprintf("%04d",$y) /ex;
            s/ mm   / sprintf("%02d",$m) /ex;
            s/ dd   / sprintf("%02d",$d) /ex;
        }
        else {  # assume base62 year/month/day
            s/ yy / int2base( $y, 62) /ex;
            s/ m  / int2base( $m, 62) /ex;
            s/ d  / int2base( $d, 62) /ex;
        }
    }
    return $format;
}

#---------------------------------------------------------------------
# TIEHASH() supports tied hash access
#     Returns data store object

# Note: because of how new_toc and new_record are implemented, I
# couldn't make Tiehash a subclass, so I'm requiring it into this
# class.  This may change in the future -- or not.

sub TIEHASH {

    # Note: 'require', not 'use'.  This isn't
    # a "true" module--we're just bringing in
    # some more FlatFile::DataStore methods.

    require FlatFile::DataStore::Tiehash;

    my $class = shift;
    $class->new( @_ );
}

1;  # returned

__END__

=head1 URI Configuration

It may seem odd to use a URI as a configuration file.  I needed some
configuration approach and wanted to stay as lightweight as possible.
The specs for a URI are fairly well-known, and it allows for everything
we need, so I chose that approach.

The examples all show a URL, because I thought it would be a nice touch
to be able to visit the URL and have the page tell you things about the
data store.  This is what the utils/flatfile-datastore.cgi program is
intended to do, but it is in a very young/rough state so far.

Following are the URI configuration parameters.  The order of the
preamble parameters I<does> matter: that's the order those fields will
appear in each record preamble.  Otherwise the order doesn't matter.

Where needed, parameter values may be uri-escaped, e.g., %20 for space.

Parameter values should be percent-encoded (uri escaped).  Use %20 for
space (don't be tempted to use '+').  Use URI::Escape::uri_escape , if
desired, e.g.,

    my $name = 'example';
    my $dir  = '/example/dir';

    use URI::Escape;
    my $datastore = FlatFile::DataStore::->new( {
        name => $name,
        dir  => $dir,
        uri  => join( ';' =>
            "http://example.com?name=$name",
            "desc=" . uri_escape( 'My DataStore' ),
            "defaults=medium",
            "user=" . uri_escape( '8- -~' ),
            "recsep=%0A",
        ) }
    );

=head2 Preamble parameters

All of the preamble parameters are required.

(In fact, four of them are optional, but leaving them out means that
you're giving up keeping the linked list of record history, so don't do
that unless you have a very good reason.)

=over 8

=item indicator

The indicator parameter specifies the single-character record
indicators that appear in each record preamble.  This parameter has the
following form: C<indicator=length-5CharacterString>,
e.g.,

    indicator=1-+#=*-

The length is always 1.  The five characters represent the five states
of a record in the data store (in this order):

    create(+): the record has not changed since being added
    oldupd(#): the record was updated, and this entry is an old version
    update(=): this entry is the updated version of a record
    olddel(*): the record was deleted, and this entry is the old version
    delete(-): the record is deleted, and this entry is the "delete record"

(The reason for a "delete record" is for storing information about the
delete process, such has when it was deleted and by whom.)

The five characters shown in the example are the ones used by all
examples in the documentation.  You're free to use your own characters,
but the length must always be 1.

=item transind

The transind parameter describes the single-character transaction
indicators that appear in each record preamble.  This parameter has the
same format and meanings as the indicator parameter, e.g.,

    transind=1-+#=*-

(Note that only three of these are used, but all five must be given and
must match the indicator parameter.)

The three characters that are used are create(+), update(=), and
delete(-).  While the record indicators will change, e.g., from create
to oldupd, or from update to olddel, etc., the transaction indicators
never change from their original values.  So a transaction that created
a record will always have the create value, and the same for update and
delete.

=item date

The date parameter specifies how the transaction date is stored in the
preamble.  It has the form: C<date=length-format>, e.g.,

    date=8-yyyymmdd
    date=4-yymd

The example shows the two choices for length: 4 or 8.  When the length
is 8, the format must contain 'yyyy', 'mm', and 'dd' in some order.
When the length is 4, the format must contain 'yy', 'm', and 'd' in
some order, e.g.,

    date=8-mmddyyyy, date=8-ddmmyyyy, etc.
    date=4-mdyy, date=4-dmyy, etc.

When the length is 8, the year, month, and day are stored as decimal
numbers, e.g., '20100615' for June, 15, 2010.  When the length is 4,
they are stored as base62 numbers, e.g. 'WQ6F' (yymd) for June 15,
2010.

=item transnum

The transnum parameter specifies how the transaction number is stored
in the preamble.  It has the form: C<transnum=length-base>,
e.g.,

    transnum=4-62

The example says the number is stored as a four-digit base62 integer.
The highest transaction number this allows is 'zzzz' base62 which is
14,776,335 decimal.  Therefore, the datastore will accommodate up to
that many transactions (creates, updates, deletes).

=item keynum

The keynum parameter specifies how the record sequence number is stored
in the preamble.  It has the form: C<keynum=length-base>,
e.g.,

    keynum=4-62

As with the transnum example above, the keynum would be stored as a
four-digit base62 integer, and the highest record sequence number
allowed would be 14,776,335 ('zzzz' base62).  Therefore, the data store
could not store more than this many records.

=item reclen

The reclen parameter specifies how the record length is stored in the
preamble.  It has the form: C<reclen=length-base>, e.g.,

    reclen=4-62

This example allows records to be up to 14,776,335 bytes long.

=item thisfnum

The thisfnum parameter specifies how the file numbers are stored in the
preamble.  There are three file number parameters, thisfnum, prevfnum,
and nextfnum.  They must match each other in length and base.  The
parameter has the form: C<thisfnum=length-base>, e.g.,

    thisfnum=2-36

There is an extra constraint imposed on the file number parameters:
they may not use a number base higher than 36.  The reason is that the
file number appears in file names, and base36 numbers match [0-9A-Z].
By limiting to base36, file names will therefore never differ only by
case, e.g., there may be a file named example.Z.data, but never one
named example.z.data.

The above example states that the file numbers will be stored as
two-digit base36 integers.  The highest file number is 'ZZ' base36,
which is 1,295 decimal.  Therefore, the datastore will allow up to that
many data files before filling up.  (If a datastore "fills up", it must
be migrated to a newly configured datastore that has bigger numbers
where needed.)

In a preamble, thisfnum is the number of the datafile where the record
is stored.  This number combined with the thisseek value and the reclen
value gives the precise location of the record data.

=item thisseek

The thisseek parameter specifies how the seek positions are stored in
the preamble.  There are three seek parameters, thisseek, prevseek, and
nextseek.  They must match each other in length and base.  The
parameter has the form:  C<thisseek=length-base>, e.g.,

    thisseek=5-62

This example states that the seek positions will be stored as
five-digit base62 integers.  So the highest seek position is 'zzzzz'
base62, which is 916,132,831 decimal.  Therefore, each of the
datastore's data files may contain up to that many bytes (record data
plus preambles).

Incidentally, no record (plus its preamble) may be longer than this,
because it just wouldn't fit in a data file.

Also, the size of each data file may be further limited using the
datamax parameter (see below).  For example, a seek value of '4-62'
would allow datafiles up to 14,776,335 bytes long.  If you want bigger
files, but don't want them bigger than 500 Meg, you can give
C<thisseek=5-62> and C<datamax=500M>.

=item prevfnum (optional)

The prevfnum parameter specifies how the "previous" file numbers are
stored in the preamble.  The value of this parameter must exactly match
thisfnum (see thisfnum above for more details).  It has the form:
C<prevfnum=length-base>, e.g.,

    prevfnum=2-36

In a preamble, the prevfnum is the number of the datafile where the
previous version of the record is stored.  This number combined with
the prevseek value gives the beginning location of the previous
record's data.

This is the first of the four "optional" preamble parameters.  If you
don't provide this one, don't provide the other three either.  If you
leave these off, you will not be able to get a record's history of
changes, and you will not be able to migrate any history to a new
datastore.

So why would to not provide these?  You might have a datastore that has
very transient data, e.g., indexes, and you don't care about change
history.  By not including these four optional parameters, when the
module updates a record, it will not perform the extra bit of IO to
update a previous record's nextfnum and nextseek values.  And the
preambles will be a little bit shorter.

=item prevseek (optional)

The prevseek parameter specifies how the "previous" seek positions are
stored in the preamble.  The value of this parameter must exactly match
thisseek (see thisseek above for more details).  It has the form
C<prevseek=length-base>, e.g.,

    prevseek=5-62

=item nextfnum (optional)

The nextfnum parameter specifies how the "next" file numbers are stored
in the preamble.  The value of this parameter must exactly match
thisfnum (see thisfnum above for more details).  It has the form:
C<nextfnum=length-base>, e.g.,

    nextfnum=2-36

In a preamble, the nextfnum is the number of the datafile where the
next version of the record is stored.  This number combined with the
nextseek value gives the beginning location of the next record's data.

You would have a nextfnum and nextseek in a preamble when it's a
previous version of a record whose current version appears later in the
datastore.  While thisfnum and thisseek are critical for all record
retrievals, prevfnum, prevseek, nextfnum, and nextseek are only needed
for getting a record's history.  They are also used during a migration
to help validate that all the data (and transactions) were migrated
intact.

=item nextseek (optional)

The nextseek parameter specifies how the "next" seek positions are
stored in the preamble.  The value of this parameter must exactly match
thisseek (see thisseek above for more details).  It has the form
C<nextseek=length-base>, e.g.,

    nextseek=5-62

=item user

The user parameter specifies the length and character class for
extra user data stored in the preamble.  It has the form:
C<user=length-CharacterClass>, e.g.,

    user=8-+-~    (must match /[ -~]+ */ and not be longer than 8)
    user=10-0-9   (must match /[0-9]+ */ and not be longer than 10)
    user=1-:      (must be literally ':')

When a record is created, the application supplies a value to store
as "user" data.  This might be a userid, an md5 digest, multiple
fixed-length fields -- whatever is needed or wanted.

This field is required but may be preassigned using the userdata
parameter (see below).  If no user data is provided or preassigned,
it will default to a space.

When this data is stored in the preamble, it is padded on the right
with spaces.

=back

=head2 Preamble defaults

All of the preamble parameters -- except user -- may be set using one
of the defaults provided, e.g.,

    http://example.com?name=example;defaults=medium;user=8-+-~
    http://example.com?name=example;defaults=large;user=10-0-9

Note that these are in a default order also.  And the user parameter
is still part of the preamble, so you can make it appear first if you
want, e.g.,

    http://example.com?name=example;user=8-+-~;defaults=medium
    http://example.com?name=example;user=10-0-9;defaults=large

The C<_nohist> versions leave out the optional preamble parameters --
the above caveat about record history still applies.

Finally, if non of these suits, they may still be good starting points
for defining your own preambles.

=over 8

=item xsmall, xsmall_nohist

When the URI contains C<defaults=xsmall>, the following values are
set:

    indicator=1-+#=*-
    transind=1-+#=*-
    date=4-yymd
    transnum=2-62   3,843 transactions
    keynum=2-62     3,843 records
    reclen=2-62     3,843 bytes/record
    thisfnum=1-36   35 data files
    thisseek=4-62   14,776,335 bytes/file
    prevfnum=1-36
    prevseek=4-62
    nextfnum=1-36
    nextseek=4-62

The last four are not set for C<defaults=xsmall_nohist>.

Rough estimates: 3800 records (or transactions), no larger than
3800 bytes each; 517 Megs total (35 * 14.7M).

=item small, small_nohist

For C<defaults=small>:

    indicator=1-+#=*-
    transind=1-+#=*-
    date=4-yymd
    transnum=3-62   238,327 transactions
    keynum=2-62     238,327 records
    reclen=2-62     238,327 bytes/record
    thisfnum=1-36   35 data files
    thisseek=5-62   916,132,831 bytes/file
    prevfnum=1-36
    prevseek=5-62
    nextfnum=1-36
    nextseek=5-62

The last four are not set for C<defaults=small_nohist>.

Rough estimates: 238K records (or transactions), no larger than 238K
bytes each; 32 Gigs total (35 * 916M).

=item medium, medium_nohist

For C<defaults=medium>:

    indicator=1-+#=*-
    transind=1-+#=*-
    date=4-yymd
    transnum=4-62   14,776,335 transactions
    keynum=4-62     14,776,335 records
    reclen=4-62     14,776,335 bytes/record
    thisfnum=2-36   1,295 data files
    thisseek=5-62   916,132,831 bytes/file
    prevfnum=2-36
    prevseek=5-62
    nextfnum=2-36
    nextseek=5-62

The last four are not set for C<defaults=medium_nohist>.

Rough estimates: 14.7M records (or transactions), no larger than 14.7M
bytes each; 1 Terabyte total (1,295 * 916M).

=item large, large_nohist

For C<defaults=large>:

    datamax=1.9G    1,900,000,000 bytes/file
    dirmax=300
    keymax=100_000
    indicator=1-+#=*-
    transind=1-+#=*-
    date=4-yymd
    transnum=5-62   916,132,831 transactions
    keynum=5-62     916,132,831 records
    reclen=5-62     916,132,831 bytes/record
    thisfnum=3-36   46,655 data files
    thisseek=6-62   56G per file (but see datamax)
    prevfnum=3-36
    prevseek=6-62
    nextfnum=3-36
    nextseek=6-62

The last four are not set for C<defaults=large_nohist>.

Rough estimates: 916M records/transactions, no larger than 916M bytes
each; 88 Terabytes total (46,655 * 1.9G).

=item xlarge, xlarge_nohist

For C<defaults=xlarge>:

    datamax=1.9G    1,900,000,000 bytes/file
    dirmax=300
    dirlev=2
    keymax=100_000
    tocmax=100_000
    indicator=1-+#=*-
    transind=1-+#=*-
    date=4-yymd
    transnum=6-62   56B transactions
    keynum=6-62     56B records
    reclen=6-62     56G per record (limited to 1.9G by datamax)
    thisfnum=4-36   1,679,615 data files
    thisseek=6-62   56G per file (but see datamax)
    prevfnum=4-36
    prevseek=6-62
    nextfnum=4-36
    nextseek=6-62

The last four are not set for C<defaults=xlarge_nohist>.

Rough estimates: 56B records/transactions, no larger than 1.9G bytes
each; 3 Petabytes total (1,679,615 * 1.9G).

=back

=head2 Other required parameters

=over 8

=item name

The name parameter identifies the datastore by name.  This name should
be short and uncomplicated, because it is used as the root for the
datastore's files.

=item recsep

The recsep parameter gives the ascii character(s) that will make up the
record separator.  The "flat file" stategy suggests that these
characters ought to match what your OS considers to be a "newline".
But in fact, you could use an string of ascii characters.

    recsep=%0A       (LF)
    recsep=%0D%0A    (CR+LF)
    recsep=%0D       (CR)

    recsep=%0A---%0A (HR--sort of)

Also, if you develop your data on unix with recsep=%0A and then copy it
to a windows machine, the module will continue to use the configured
recsep, i.e., it is not tied the to OS.

=back

=head2 Other optional parameters

=over 8

=item desc

The desc parameter provides a means to give a short description (or perhaps a
longer name) of the datastore.

=item datamax

The datamax parameter give the maximum number of bytes a data file may contain.
If you don't provide a datamax, it will be computed from the thisseek value (see
thisseek above for more details).

The datamax value is simply a number, e.g.,

    datamax=1000000000   (1 Gig)

To make things easier to see, you can add underscores, e.g.,

    datamax=1_000_000_000   (1 Gig)

You can also shorten the number with an 'M' for megabytes (10**6) or a
'G' for gigabytes (10**9), e.g.,

    datamax=1000M  (1 Gig)
    datamax=1G     (1 Gig)

Finally, with 'M' or 'G', you can use fractions, e.g.

    datamax=.5M  (500_000)
    datamax=1.9G (1_900_000_000)

=item keymax

The keymax parameter gives the number of record keys that may be stored
in a key file.  This simply limits the size of the key files, e.g.,

    keymax=10_000

The maximum bytes would be:

    keymax * (preamble length + recsep length)

The numeric value may use underscores and 'M' or 'G' as described above
for datamax.

=item tocmax

The tocmax parameter gives the number of data file entries that may be stored
in a toc (table of contents) file.  This simply limits the size of the toc
files, e.g.,

    tocmax=10_000

Each (fairly short) line in a toc file describes a single data file, so
you would need a tocmax only in the extreme case of a datastore with
thousands or millions of data files.

The numeric value may use underscores and 'M' or 'G' as described above
for datamax.

=item dirmax

The dirmax parameter gives the number of files (and directories) that
may be stored in a datastore directory, e.g.,

    dirmax=300

This allows a large number of data files (and key/toc files) to be
created without there being too many files in a single directory.

(The numeric value may use underscores and 'M' or 'G' as described above
for datamax.)

If you specify dirmax without dirlev (see below), dirlev will default
to 1.

Without dirmax and dirlev, a datastore's data files (and key/toc files)
will reside in the same directory as the uri file, and the module will
not limit how many you may create (though the size of your filesystem
might).

With dirmax and dirlev, these files will reside in subdirectories.

Giving a value for dirmax will also limit the number of data files (and
key/toc files) a datastore may have, by this formula:

 max files = dirmax ** (dirlev + 1)

So dirmax=300 and dirlev=1 would result in a limit of 90,000 data
files.  If you go to dirlev=2, the limit becomes 27,000,000, which is
why you're unlikely to need a dirlev greater than 2.

=item dirlev

The dirlev parameter gives the number of levels of directories that a
datastore may use, e.g.,

    dirlev=1

You can give a dirlev without a dirmax, which would store the data
files (and key/toc files) in subdirectories, but wouldn't limit how
many files may be in each directory.

=item userdata

The userdata parameter is similar to the userdata parameter in the call
to new().  It specifies the default value to use if the application
does not provide a value when creating, updating, or deleting a
record.

Those provided values will override the value given in the call to new(),
which will override the value given here in the uri.

If you don't specify a default value here or in the call to new(), the
value defaults to a space (which may then be padded with more spaces).

    userdata=:

The example is contrived for a hypothetical datastore that doesn't need
this field.  Since the field is required, the above setting will always
store a colon (and the user parameter might be C<user=1-:>).

=back

=head1 CAVEATS

This module is still in an experimental state.  The tests are sparse.
When I start using it in production, I'll bump the version to 1.00.

Until then (afterwards, too) please use with care.

=head1 AUTHOR

Brad Baxter, E<lt>bbaxter@cpan.orgE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2010 by Brad Baxter

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.8.8 or,
at your option, any later version of Perl 5 you may have available.

=cut

