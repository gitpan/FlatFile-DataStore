#---------------------------------------------------------------------
  package FlatFile::DataStore;
#---------------------------------------------------------------------

=head1 NAME

FlatFile::DataStore - Perl module that implements a flat file data store.

=head1 SYNOPSYS

 use FlatFile::DataStore;

 # new datastore object

 my $dir  = "/my/datastore/area";
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

Scripts supplied in the distribution perform:

 - validation
 - migration

=head1 VERSION

FlatFile::DataStore version 0.10

=cut

our $VERSION = '0.10';

use 5.008003;
use strict;
use warnings;

use File::Path;
use Fcntl qw(:DEFAULT :flock);
use URI;
use URI::Escape;
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
    user        1
    indicator   1
    date        1
    keynum      1
    reclen      1
    transnum    1
    thisfnum    1
    thisseek    1
    prevfnum    1
    prevseek    1
    nextfnum    1
    nextseek    1
    );

my %Optional = qw(
    dirmax      1
    dirlev      1
    tocmax      1
    keymax      1
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

my $Ascii_chars = qr/^[ -~]+$/;
my( %Read_fh, %Write_fh );  # inside-outish object attributes

#---------------------------------------------------------------------

=head1 CLASS METHODS

=head2 FlatFile::DataStore->new();

Constructs a new FlatFile::DataStore object.

Accepts hash ref giving values for C<dir> and C<name>.

 my $ds = FlatFile::DataStore->new( { dir => $dir, name => $name } );

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
#     parms: dir,  the directory where the data store lives
#            name, the name of the data store
#     will look for name.obj or name.uri and load those values

sub init {
    my( $self, $parms ) = @_;

    my $dir  = $parms->{'dir'};
    my $name = $parms->{'name'};
    croak qq/Need "dir" and "name"/
        unless defined $dir and defined $name;

    my $obj_file = "$dir/$name.obj";

    # if database has been initialized, there's an object file
    if( -e $obj_file ) {
        my $obj = $self->read_file( $obj_file );
        $self = eval $obj;  # note: *new* $self
        croak qq/Problem with $obj_file: $@/ if $@;
        $self->dir( $dir );  # dir not in obj_file
    }

    # otherwise read the uri file and initialize the database
    else {
        my $uri_file = "$dir/$name.uri";
        my $uri = $self->read_file( $uri_file );
        chomp $uri;

        $self->uri( $uri );

        my $uri_parms = $self->burst_query;
        for my $attr ( keys %$uri_parms ) {
            croak qq/Unrecognized parameter: "$attr"/ unless $Attrs{ $attr };
            # (using $attr as method name:)
            $self->$attr( $uri_parms->{ $attr } );
        }

        # check that all fnums and seeks are the same
        croak qq/fnum parameters differ/
            unless $self->thisfnum eq $self->prevfnum and
                   $self->thisfnum eq $self->nextfnum;
        croak qq/seek parameters differ/
            unless $self->thisseek eq $self->prevseek and
                   $self->thisseek eq $self->nextseek;

        # now for some generated attributes ...
        my( $len, $base );
        ( $len, $base ) = split /-/, $self->thisfnum;  # fnums are equal
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

        $self->toclen( 10          +  # blanks
            3 *    $self->fnumlen  +  # datafnum, tocfnum, keyfnum
            2 *    $self->keylen   +  # numrecs keynum
            6 *    $self->translen +  # transnum and cruds
            length $self->recsep );

        ( $len, $base ) = split /-/, $self->thisseek;  # seeks are equal
        my $maxnum = substr( base_chars( $base ), -1) x $len;
        my $maxint = base2int $maxnum, $base;

        if( $self->datamax ) {
            $self->datamax( $self->convert_datamax );
            croak qq/datamax too large/ if $self->datamax > $maxint;
        }
        else {
            $self->datamax( $maxint );
        }

        if( $self->dirmax ) {
            $self->dirlev( 1 ) unless $self->dirlev;
        }

        for my $attr ( keys %Attrs ) {
            croak qq/Uninitialized attribute: "$_"/
                if not defined $self->$attr and not $Optional{ $attr };
        }

        $self->initialize;
    }

    return $self;  # this is either the same self or a new self
}

#---------------------------------------------------------------------
# burst_query(), called by init() to parse the name.uri file
#     also generates values for 'spec' and 'preamblelen'

sub burst_query {
    my( $self ) = @_;

    my $uri   = $self->uri;
    my $query = URI->new( $uri )->query();

    my @pairs = split /[;&]/, $query;
    my $omap  = [];  # psuedo-new(), ordered hash
    my $pos   = 0;
    my %parms;
    for( @pairs ) {
        my( $name, $val ) = split /=/, $_, 2;

        $name = uri_unescape( $name );
        $val  = uri_unescape( $val );

        croak qq/"$name" duplicated in uri/ if $parms{ $name };

        $parms{ $name } = $val;
        if( $Preamble{ $name } ) {
            my( $len, $parm ) = split /-/, $val, 2;
            omap_add( $omap, $name => [ $pos, 0+$len, $parm ] );
            $pos += $len;
        }
    }

    # some attributes are generated here:
    $parms{'specs'}       = $omap;
    $parms{'preamblelen'} = $pos;

    return \%parms;
}

#---------------------------------------------------------------------
# make_preamble_regx(), called by init() to construct a regular
#     expression that should match any record's preamble
#     this regx should capture each fields value

sub make_preamble_regx {
    my( $self ) = @_;

    my $regx = "";
    for( $self->specs ) {  # specs() returns an array of hashrefs
        my( $key, $aref )       = %$_;
        my( $pos, $len, $parm ) = @$aref;

        for( $key ) {
            if( /indicator/ ) {
                $regx .= ($len == 1 ? "([\Q$parm\E])" : "([\Q$parm\E]{$len})");
            }
            elsif( /user/ ) {  # should only allow $Ascii_chars
                $regx .= ($len == 1 ? "([$parm])" : "([$parm]{$len})");
            }
            elsif( /date/ ) {
                $regx .= ($len == 8 ? "([0-9]{8})" : "([0-9A-Za-z]{4})");
            }
            else {
                my $chars = base_chars( $parm );
                $chars =~ s/([0-9])[0-9]+([0-9])/$1-$2/;  # compress
                $chars =~ s/([A-Z])[A-Z]+([A-Z])/$1-$2/;
                $chars =~ s/([a-z])[a-z]+([a-z])/$1-$2/;
                # '-' is 'null' character:
                $regx .= ($len == 1 ? "([-$chars])" : "([-$chars]{$len})");
            }
        }
    }
    return qr/$regx/;
}

#---------------------------------------------------------------------
# make_crud(), called by init() to construct a hash of indicators
#     CRUD indicators: Create, Retrieve, Update, Delete
#     the following are suggested, but configurable in uri
#         + Create
#         # Old Update (old record flagged as updated)
#         = Update
#         * Old Delete (old record flagged as deleted)
#         - Delete
#     (no indicator for Retrieve, n/a--but didn't want to say CUD)

sub make_crud {
    my( $self ) = @_;

    my( $len, $chars ) = split /-/, $self->indicator, 2;
    croak qq/Only single-character indicators supported./ if $len != 1;

    my @c = split //, $chars;
    my %c = map { $_ => 1 } @c;
    my @n = keys %c;
    croak qq/Need five unique indicator characters/ if @n != 5 or @c != 5;

    my %crud;
    @crud{ qw( create oldupd update olddel delete ) } = @c;
    return \%crud;
}

#---------------------------------------------------------------------
# convert_datamax(), called by init() to convert user-supplied
#     datamax value into an integer: one can say, "500_000_000",
#     "500M", or ".5G" to mean 500,000,000 bytes

sub convert_datamax {
    my( $self ) = @_;

    # ignoring M/G ambiguities and using round numbers:
    my %sizes = ( M => 10**6, G => 10**9 );

    my $max = $self->datamax;
    $max =~ s/_//g;
    if( $max =~ /^([.0-9]+)([MG])/ ) {
        my( $n, $s ) = ( $1, $2 );
        $max = $n * $sizes{ $s };
    }

    return 0+$max;
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
        unless( $reftype ) {  # string
            $data_ref = \$record_data; }
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
    my $top_toc = $self->new_toc( { int => 0 } );
    my $keyint  = $top_toc->keynum + 1;
    my $keylen  = $self->keylen;
    my $keybase = $self->keybase;
    my $keynum  = int2base $keyint, $keybase, $keylen;
    croak qq/Database exceeds configured size (keynum: "$keynum" too long)/
        if length $keynum > $keylen;

    # get keyfile
    # need to lock files before getting seek positions
    # want to lock keyfile before datafile
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
            date      => now( $self->dateformat ),
            transnum  => $transint,
            keynum    => $keyint,
            reclen    => $reclen,
            thisfnum  => $datafnum,
            thisseek  => $dataseek,
            user      => $user_data,
            } } );

    # write record to datafile
    my $preamble = $record->string;
    my $dataline = $preamble . $$data_ref . $self->recsep;
    $self->write_bytes( $datafh, $dataseek, $dataline );

    # write preamble to keyfile
    $self->write_bytes( $keyfh, $keyseek, $preamble . $self->recsep );
    
    # update table of contents (toc) file
    my $toc = $self->new_toc( { num => $datafnum } );

    # note: datafnum and tocfnum are set in toc->new
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

    if( $keystring ) {
        my $string = $record->string;
        croak qq/Mismatch "$string" vs. "$keystring"/ if $string ne $keystring;
    }

    return $record;
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

=cut

sub update {
    my $self = shift;
    my( $obj, $data_ref, $user_data ) = $self->normalize_parms( @_ );

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
    my $record = $self->new_record( {
        data     => $data_ref,
        preamble => {
            indicator => $self->crud->{'update'},
            date      => now( $self->dateformat ),
            transnum  => $transint,
            keynum    => $keyint,
            reclen    => $reclen,
            thisfnum  => $datafnum,
            thisseek  => $dataseek,
            prevfnum  => $prevfnum,
            prevseek  => $prevseek,
            user      => $user_data,
            } } );

    # write record to datafile
    my $preamble = $record->string;
    my $dataline = $preamble . $$data_ref . $self->recsep;
    $self->write_bytes( $datafh, $dataseek, $dataline );

    # write preamble to keyfile (recsep there already)
    $self->write_bytes( $keyfh, $keyseek, $preamble );

    # update the old preamble
    $prevpreamble = $self->update_preamble( $prevpreamble, {
        indicator => $self->crud->{ 'oldupd' },
        nextfnum  => $datafnum,
        nextseek  => $dataseek,
        } );
    my $prevdatafile = $self->which_datafile( $prevfnum );
    my $prevdatafh   = $self->locked_for_write( $prevdatafile );
    $self->write_bytes( $prevdatafh, $prevseek, $prevpreamble );

    # update table of contents (toc) file
    my $toc = $self->new_toc( { num => $datafnum } );

    # note: datafnum and tocfnum are set in toc->new
    $toc->keyfnum(  $top_toc->keyfnum );  # keep last nums going
    $toc->keynum(   $top_toc->keynum  );
    $toc->transnum( $transint         );
    $toc->update(   $toc->update  + 1 );
    $toc->numrecs(  $toc->numrecs + 1 );

    # was the previous record in another data file?
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

    $toc->write_toc( $toc->datafnum );

    # update top toc
    $top_toc->datafnum( $toc->datafnum        );
    $top_toc->tocfnum(  $toc->tocfnum         );
    $top_toc->transnum( $toc->transnum        );
    $top_toc->update(   $top_toc->update  + 1 );
    $top_toc->oldupd(   $top_toc->oldupd  + 1 );
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

=cut

sub delete {
    my $self = shift;
    my( $obj, $data_ref, $user_data ) = $self->normalize_parms( @_ );

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
    my $record = $self->new_record( {
        data     => $data_ref,
        preamble => {
            indicator => $self->crud->{'delete'},
            date      => now( $self->dateformat ),
            transnum  => $transint,
            keynum    => $keyint,
            reclen    => $reclen,
            thisfnum  => $datafnum,
            thisseek  => $dataseek,
            prevfnum  => $prevfnum,
            prevseek  => $prevseek,
            user      => $user_data,
            } } );

    # write record to datafile
    my $preamble = $record->string;
    my $dataline = $preamble . $$data_ref . $self->recsep;
    $self->write_bytes( $datafh, $dataseek, $dataline );

    # write preamble to keyfile (recsep there already)
    $self->write_bytes( $keyfh, $keyseek, $preamble );

    # update the old preamble
    $prevpreamble = $self->update_preamble( $prevpreamble, {
        indicator => $self->crud->{ 'olddel' },
        nextfnum  => $datafnum,
        nextseek  => $dataseek,
        } );
    my $prevdatafile = $self->which_datafile( $prevfnum );
    my $prevdatafh   = $self->locked_for_write( $prevdatafile );
    $self->write_bytes( $prevdatafh, $prevseek, $prevpreamble );

    # update table of contents (toc) file
    my $toc = $self->new_toc( { num => $datafnum } );

    # note: datafnum and tocfnum are set in toc->new
    $toc->keyfnum(  $top_toc->keyfnum );  # keep last nums going
    $toc->keynum(   $top_toc->keynum  );
    $toc->transnum( $transint         );
    $toc->delete(   $toc->delete + 1  );

    # was the previous record in another data file?
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

    $toc->write_toc( $toc->datafnum );

    # update top toc
    $top_toc->datafnum( $toc->datafnum        );
    $top_toc->tocfnum(  $toc->tocfnum         );
    $top_toc->transnum( $toc->transnum        );
    $top_toc->delete(   $top_toc->delete  + 1 );
    $top_toc->olddel(   $top_toc->olddel  + 1 );
    $top_toc->numrecs(  $top_toc->numrecs - 1 );

    $top_toc->write_toc( 0 );

    return $record;
}

#---------------------------------------------------------------------
# $obj         may be string, preamble obj, or record obj
# $record_data may be string, scalar ref  , or record obj
# $user_data   may be string
#
# $user_data, if not given, will be gotten from $record_data or $obj
# $record_data, if not given, will be gotten from $obj

sub normalize_parms {
    my( $self, $obj, $record_data, $user_data ) = @_;

    croak qq/Bad call./ unless $obj;

    # set the preamble object
    my( $preamble, $data_ref, $try_user );
    my $reftype = ref $obj;
    unless( $reftype ) {  # string
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
        unless( $reftype ) {  # string
            $data_ref = \$record_data; }
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

If C<$dir> is given, the directory must already exist.

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

=head2 Preamble accessors

The following methods set and return their respective attribute values
if C<$value> is given.  Otherwise, they just return the value.

 $ds->indicator( [$value] ); # from uri (length-characters)
 $ds->date(      [$value] ); # from uri (length-format)
 $ds->transnum(  [$value] ); # from uri (length-base)
 $ds->keynum(    [$value] ); # from uri (length-base)
 $ds->reclen(    [$value] ); # from uri (length-base)
 $ds->thisfnum(  [$value] ); # from uri (length-base)
 $ds->thisseek(  [$value] ); # from uri (length-base)
 $ds->prevfnum(  [$value] ); # from uri (length-base)
 $ds->prevseek(  [$value] ); # from uri (length-base)
 $ds->nextfnum(  [$value] ); # from uri (length-base)
 $ds->nextseek(  [$value] ); # from uri (length-base)
 $ds->user(      [$value] ); # from uri (length-characters)

=head2 Other accessors

 $ds->name(        [$value] ); # from uri, name of data store
 $ds->desc(        [$value] ); # from uri, description of data store
 $ds->recsep(      [$value] ); # from uri (character(s))
 $ds->uri(         [$value] ); # full uri as is
 $ds->preamblelen( [$value] ); # length of full preamble string
 $ds->toclen(      [$value] ); # length of toc entry
 $ds->keylen(      [$value] ); # length of stored keynum
 $ds->keybase(     [$value] ); # base of stored keynum
 $ds->translen(    [$value] ); # length of stored transaction number
 $ds->transbase(   [$value] ); # base of stored trancation number
 $ds->fnumlen(     [$value] ); # length of stored file number
 $ds->fnumbase(    [$value] ); # base of stored file number
 $ds->dateformat(  [$value] ); # format from uri
 $ds->regx(        [$value] ); # capturing regx for preamble string
 $ds->datamax(     [$value] ); # maximum bytes in a data file
 $ds->crud(        [$value] ); # hash ref, e.g.,

     {
        create => '+',
        oldupd => '#',
        update => '=',
        olddel => '*',
        delete => '-'
     }

 (translates logical actions into their symbolic indicators)

=head2 Optional accessors

 $ds->dirmax(  [$value] ); # maximum files in a directory
 $ds->dirlev(  [$value] ); # number of directory levels
 $ds->tocmax(  [$value] ); # maximum toc entries
 $ds->keymax(  [$value] ); # maximum key entries

If no C<dirmax>, directories will keep being added to.

If no C<dirlev>, toc, key, and data files will reside in top-level
directory.  If C<dirmax> given, C<dirlev> defaults to 1.

If no C<tocmax>, there will be only one toc file, which will grow
indefinitely.

If no C<keymax>, there will be only one key file, which will grow
indefinitely.

=cut

sub indicator {for($_[0]->{indicator} ){$_=$_[1]if@_>1;return$_}}
sub date      {for($_[0]->{date}      ){$_=$_[1]if@_>1;return$_}}
sub transnum  {for($_[0]->{transnum}  ){$_=$_[1]if@_>1;return$_}}
sub keynum    {for($_[0]->{keynum}    ){$_=$_[1]if@_>1;return$_}}
sub reclen    {for($_[0]->{reclen}    ){$_=$_[1]if@_>1;return$_}}
sub thisfnum  {for($_[0]->{thisfnum}  ){$_=$_[1]if@_>1;return$_}}
sub thisseek  {for($_[0]->{thisseek}  ){$_=$_[1]if@_>1;return$_}}
sub prevfnum  {for($_[0]->{prevfnum}  ){$_=$_[1]if@_>1;return$_}}
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

# optional:

sub dirmax {
    my $self = shift;
    return $self->{dirmax} = 0+$_[0] if @_;
    return $self->{dirmax} if exists $self->{dirmax};
}
sub dirlev {
    my $self = shift;
    return $self->{dirlev} = 0+$_[0] if @_;
    return $self->{dirlev} if exists $self->{dirlev};
}
sub tocmax {
    my $self = shift;
    return $self->{tocmax} = 0+$_[0] if @_;
    return $self->{tocmax} if exists $self->{tocmax};
}
sub keymax {
    my $self = shift;
    return $self->{keymax} = 0+$_[0] if @_;
    return $self->{keymax} if exists $self->{keymax};
}

#---------------------------------------------------------------------

=head1 OBJECT METHODS, UTILITARIAN

TODO: more pod here ...

=cut

#---------------------------------------------------------------------
# initialize(), called by init() when datastore is first used
#     creates name.obj file to bypass uri parsing from now on

sub initialize {
    my( $self ) = @_;

    # can't initialize after data has been added
    my $fnum     = int2base 1, $self->fnumbase, $self->fnumlen;
    my $datafile = $self->which_datafile( $fnum );
    croak qq/Can't initialize database: data files exist (e.g., $datafile)./
        if -e $datafile;

    # make object a one-liner
    local $Data::Dumper::Quotekeys = 0;
    local $Data::Dumper::Pair      = '=>';
    local $Data::Dumper::Useqq     = 1;
    local $Data::Dumper::Terse     = 1;
    local $Data::Dumper::Indent    = 0;

    # delete dir, don't want it in obj file
    my $savedir = $self->dir;
    $self->dir("");

    my $obj_file = "$savedir/" . $self->name . ".obj";
    $self->write_file( $obj_file, Dumper $self );

    # restore dir
    $self->dir( $savedir );

}

#---------------------------------------------------------------------
# new_toc()
#     wrapper for FlatFile::DataStore::Toc->new()

sub new_toc {
    my( $self, $parms ) = @_;
    $parms->{'datastore'} = $self;
    FlatFile::DataStore::Toc->new( $parms );
}

#---------------------------------------------------------------------
# new_preamble(), called by various subs
#     wrapper for FlatFile::DataStore::Preamble->new()

sub new_preamble {
    my( $self, $parms ) = @_;
    $parms->{'datastore'} = $self;
    FlatFile::DataStore::Preamble->new( $parms );
}

#---------------------------------------------------------------------
# new_record(), called by various subs
#     wrapper for FlatFile::DataStore::Record->new()

sub new_record {
    my( $self, $parms ) = @_;
    my $preamble = $parms->{'preamble'};
    if( ref $preamble eq 'HASH' ) {  # not an object
        $parms->{'preamble'} = $self->new_preamble( $preamble );
    }
    FlatFile::DataStore::Record->new( $parms );
}

#---------------------------------------------------------------------
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
        $path = "$name/key$path";
        mkpath( $path ) unless -d $path;
        $keyfile = "$path/$keyfile";
    }
    $keyfile = $self->dir . "/$keyfile";

    return ( $keyfile, $keyfint ) if wantarray;
    return $keyfile;

}

#---------------------------------------------------------------------
# datafile(), called by create(), update(), and delete() to
#     get the current data file; the parm $reclen is used to see if
#     new_datafile() needs to be called

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
        $path = "$name/data$path";
        mkpath( $path ) unless -d $path;
        $datafile = "$path/$datafile";
    }
    $datafile = $self->dir . "/$datafile";

    return $datafile;

}

#---------------------------------------------------------------------

=head2 howmany( [$regx] )

Returns count of records whose indicators match regx, e.g.,

 $self->howmany( qr/create|update/ );
 $self->howmany( qr/delete/ );
 $self->howmany( qr/oldupd|olddel/ );

If no regx, howmany() returns numrecs from the toc file.

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
# keyseek(), seek to a particular line in the key file
            
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
# lastkeynum(), called by retrieve to check if requested number exists

sub lastkeynum {
    my( $self ) = @_;

    my $top_toc = $self->new_toc( { int => 0 } );
    my $keyint  = $top_toc->keynum;

    return $keyint;
}

#---------------------------------------------------------------------
# nexttransnum, get next transaction number

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
# sub all_datafiles(), called in migrate_validate utility script

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
# burst_pramble(), called various places to parse preamble string

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
            if( /indicator|date/ ) {
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

sub update_preamble {
    my( $self, $preamble, $parms ) = @_;

    my $omap = $self->specs;

    for( keys %$parms ) {

        my $value = $parms->{ $_ };
        my( $pos, $len, $parm ) = @{omap_get_values( $omap, $_ )};

        my $try;
        if( /indicator|date|user/ ) {
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
sub DESTROY {
    my $self = shift;
    $self->close_files;
}

#---------------------------------------------------------------------
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
sub locked_for_read {
    my( $self, $file ) = @_;

    my $open_fh = $Read_fh{ $self }{ $file };
    return $open_fh if $open_fh;

    my $fh;
    open $fh, '<', $file or croak "Can't open $file: $!";
    flock $fh, LOCK_SH   or croak "Can't lock $file: $!";
    binmode $fh;

    $Read_fh{ $self }{ $file } = $fh;
    return $fh;
}

#---------------------------------------------------------------------
sub locked_for_write {
    my( $self, $file ) = @_;

    my $open_fh = $Write_fh{ $self }{ $file };
    return $open_fh if $open_fh;

    my $fh;
    sysopen( $fh, $file, O_RDWR|O_CREAT ) or croak "Can't open $file: $!";
    my $ofh = select( $fh ); $| = 1; select ( $ofh );
    flock $fh, LOCK_EX                    or croak "Can't lock $file: $!";
    binmode $fh;

    $Write_fh{ $self }{ $file } = $fh;
    return $fh;
}

#---------------------------------------------------------------------
sub read_record {
    my( $self, $fh, $seekpos ) = @_;

    my $len      = $self->preamblelen;
    my $string   = $self->read_bytes( $fh, $seekpos, $len );
    my $preamble = $self->new_preamble( { string => $string } );

    $seekpos    += $len;
    $len         = $preamble->reclen;
    my $recdata  = $self->read_bytes( $fh, $seekpos, $len ); 

    my $record = $self->new_record( {
        preamble => $preamble,
        data     => \$recdata,
        } );

    return $record;
}

#---------------------------------------------------------------------
sub read_preamble {
    my( $self, $fh, $seekpos ) = @_;

    my $len = $self->preamblelen;

    return $self->read_bytes( $fh, $seekpos, $len ); 
}

#---------------------------------------------------------------------
sub read_bytes {
    my( $self, $fh, $seekpos, $len ) = @_;

    my $string;
    sysseek $fh, $seekpos, 0 or croak "Can't seek: $!";
    my $rc = sysread $fh, $string, $len;
    croak "Can't read: $!" unless defined $rc;

    return $string;
}

#---------------------------------------------------------------------
sub write_bytes {
    my( $self, $fh, $seekpos, $string ) = @_;

    sysseek  $fh, $seekpos, 0 or croak "Can't seek: $!";
    syswrite $fh, $string     or croak "Can't write: $!";

}

#---------------------------------------------------------------------
# read_file(), slurp contents of file

sub read_file {
    my( $self, $file ) = @_;

    my $fh = $self->locked_for_read( $file );
    local $/;
    return <$fh>;
}

#---------------------------------------------------------------------
# write_file(), dump contents to file (opposite of slurp, sort of)

sub write_file {
    my( $self, $file, $contents ) = @_;

    my $fh = $self->locked_for_write( $file );
    my $type = ref $contents;
    if( $type ) {
        if   ( $type eq 'SCALAR' ) { print $fh $$contents           }
        elsif( $type eq 'ARRAY'  ) { print $fh join "", @$contents  }
        else                       { croak "Unrecognized type: $type" }
    }
    else { print $fh $contents }
}

#---------------------------------------------------------------------
# new(), expects yyyymmdd or yymd (or mmddyyyy, mdyy, etc.)
#        returns current date formatted as requested
# called by create(), update(), delete()

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
# then(), translates stored date to YYYY-MM-DD
# called by FF::DD::Preamble::init()

sub then {
    my( $self, $date, $format ) = @_;
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

1;  # returned

__END__

=head1 CAVEATS

This module is still in an experimental state.  The tests and pod are
sparse.  When I start using it in production, I'll up the version to
1.00.

Until then (afterwards, too) please use with care.

=head1 TODO

 - iteration
 - cgi to analyze data store configuation (for uri to point to)
 - more tests
 - more pod

=head1 AUTHOR

Brad Baxter, E<lt>bbaxter@cpan.orgE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2009 by Brad Baxter

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.8.8 or,
at your option, any later version of Perl 5 you may have available.

=cut

