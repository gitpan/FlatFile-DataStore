#---------------------------------------------------------------------

=head1 NAME

FlatFile::DataStore::FMTEYEWTK - POD containing in-depth discussion
of FlatFile::DataStore.

=head1 VERSION

Discusses FlatFile::DataStore version 0.04.

=head1 SYNOPSYS

 man FlatFile::DataStore
 man FlatFile::DataStore::Tutorial
 man FlatFile::DataStore::FMTEYEWTK

or

 perldoc FlatFile::DataStore
 perldoc FlatFile::DataStore::Tutorial
 perldoc FlatFile::DataStore::FMTEYEWTK

or

 http://search.cpan.org/dist/FlatFile-DataStore/

=head1 DESCRIPTION

=head2 Overview

This discussion only contains POD, so don't do this:

 use FlatFile::DataStore::FMTEYEWTK;  # don't do this

Instead, simply read the POD (as you are doing). Also please read
the docs for FlatFile::DataStore, which is essentially the reference
manual, and for FlatFile::DataStore::Tutorial.

This discussion is intended to augment those docs with
longer explanations for the design of the module, more usage
examples, and other materials that will hopefully help you make
better use of it.

=head1 DISCUSSION

=head2 Overview

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

=head2 Data Store Files and Directories

Key files acts as an index into the data files.  The different versions
of the records in the data files act as linked lists:

 - the first version of a record links just to it's successor
 - a second, third, etc., versions link to their successors and predecessors
 - the final (current) version links just to its predecessor

A key file entry always points to the final (current) version of a
record. It may have a pointer to a previous version, but it will never
have a pointer to a "next" version, because there isn't one.

Each record is stored with a I<preamble>, which is a fixed-length
string of fields containing:

 - crud indicator       (flag for created, updated, deleted, etc.)
 - transaction number   (incremented when a record is touched)
 - date                 (of the "transaction")
 - key number           (record sequence number)
 - record length        (in bytes)
 - user data            (for out-of-band* user-defined data)
 - "this" file number   (linked list pointers ...)
 - "this" seek position
 - "prev" file number
 - "prev" seek position
 - "next" file number
 - "next" seek position

*That is, data about the record not stored in the record.

The formats and sizes of these fixed-length fields may be configured
when the data store is first defined, and will determine certain
constraints on the size of the data store.  For example, if the file
number is base-10 and 2 bytes in size, then the data store may have
up to 99 data files.  And if the seek position is base-10 and 9 bytes
in size, then each data file may contain up to 1 Gig of data.

Number bases larger than base-10 (up to base-36 for file numbers and up
to base-62 for other numbers) may be used to help shorten the length of
the preamble string.

A data store will have the following files:

 - uri  file,  contains the uri, which defines the configuration parameters
 - obj  file,  contains dump of generic perl object constructed from uri*
 - toc  files, contain transaction numbers for each data file
 - key  files, contain pointers to every current record version
 - data files, contain all the versions of all the records

*('generic' because the object does not include the 'dir' attribute)

If the data store is small, it might have only one toc, key, and/or
data file.

If C<dirlev> (see below) is 0 or undefined, the toc, key, or data files
will reside at the same level as the url and obj files, e.g.,

    - name.uri
    - name.obj
    - name.toc     (or name.1.toc if C<tocmax> is set)
    - name.key     (or name.1.key if C<keymax> is set)
    - name.1.data  (the filenum, e.g., 1, is always present)

If C<dirlev> > 0, the directory structure follows this scheme (note
that file/dir numbers start with 1):

- dir
    - name.uri
    - name.obj
    - name
        - toc1
            - name.1.toc,
            - name.2.toc,
            - etc.
        - toc2,
        - etc.
        - key1
            - name.1.key,
            - name.2.key,
            - etc.
        - key2,
        - etc.
        - data1
            - name.1.data
            - name.2.data,
            - etc.
        - data2,
        - etc.

If C<tocmax> is not defined, there will never be more than one toc
file and so the name will be C<name.toc> instead of C<name.1.toc>.

If C<keymax> is not defined, there will never be more than one key
file and so the name will be C<name.key> instead of C<name.1.key>.

Different data stores may coexist in the same top-level directory--they
just have to have different names.

To retrieve a record, one must know the data file number and the seek
position into that data file, or one must know the record's sequence
number (the order it was added to the data store).  With a sequence
number, the file number and seek position can be looked up in a key
file, so these sequence numbers are called "key numbers" or C<keynum>.

Methods support the following actions:

 - create
 - retrieve
 - update
 - delete
 - history
 - iterate (over all transactions in the data files)

Scripts supplied in the distribution perform:

 - validation of a data store
 - migration of data store records to newly configured data store
 - comparison of pre-migration and post-migration data stores

=head2 Motivation

Several factors motivated the development of this module:

 - the desire for simple, efficient reading and writing of records
 - the desire to handle any number and size of records
 - the desire to identify records using sequence numbers
 - the need to retain previous versions of records and to view history
 - the ability to store any sort of data: binary or text in any encoding
 - the desire for a relatively simple file structure
 - the desire for the data to be fairly easily read by a human
 - the ability to easily increase the data store size (through migration)

The key file makes it easy and efficient to retrieve the current
version of a record--you just need the record's sequence number.  Other
retrievals via file number and seek position (e.g., gotten from a
history list) are also fast and easy.

Because the size and number of data files is configurable, the data
store should scale up to large numbers of (perhaps large) records.
This while still retaining efficient reading and writing.

(In the extreme case that a record is too large for a single file,
users might break up the record into parts, store them as multiple data
store records and store a "directory" record to guide the reassembly.
While that's outside the scope of this module, that sort of scheme is
accommodated by the fact that the data store doesn't care if the record
data is not a complete unit of a known format.)

When a record is created, it is assigned a sequence number (keynum)
that persistently identifies that record for the life of the data
store.  This should help user-developed indexing schemes that
employ, e.g., bit maps to remain correct.

Since a record links to it's predecessors, it's easy to get a history
of that record's changes over time.  This can facilitate recovery and
reporting.

Since record retrieval is by seek position and record length (in
bytes), any sequence of bytes may be stored and retrieved.  Disparate
types of data may be stored in the same data store.

Outside of the record data itself, the data store file structure uses
ascii characters for the key file and preambles.  It appends a record
separator, typically a newline character, after each record.  This is
intended to make the file structure relatively simple and more easily
read by a human--to aid copying, debugging, disaster recovery, simple
curiosity, etc.

Migration scripts are included in the module distribution.  If your
initial configuration values prove too small to accommodate your data,
you can configure a new data store with larger values and migrate all
the records to the new data store.  All of the transaction and sequence
numbers remain the same; the record data and user data are identical;
and interfacing with the new data store vs. the old one should be
completely transparent to programs using the FlatFile::DataStore
module.

=head2 CRUD cases

 Create: no previous preamble required or allowed
    - create a record object (with no previous)
    - write the record
    - return the record object
 Retrieve:
    - read a data record
    - create a record object (with a preamble, which may become a previous)
    - return the record object
 Update: previous preamble required (and it must not have changed)
    - create a record object (with a previous preamble)
    - write the record (updating the previous in the data store)
    - return the record object
 Delete: previous preamble required (and it must not have changed)
    - create a record object (with a previous preamble)
    - write the record (updating the previous in the data store)
    - return the record object

Some notes about the "previous" preamble:

In order to protect data from conflicting concurrent updates, you may
not update or delete a record without first retrieving it from the data
store.  Supplying the previous preamble along with the new record data
is proof that you did this.  Before the new record is written, the
supplied previous preamble is compared with what's in the data store,
and if they are not exactly the same, it means that someone else
retrieved and updated/deleted the record between the time you read it
and the time you tried to update/delete it.

So unless you supply a previous preamble and unless the one you supply
matches exactly the one in the data store, your update/delete will not
be accepted--you will have to re-retrieve the new version of the record
(getting a more recent preamble) and apply your updates to it.

=head2 Scaling to infinity (and beyond)

Past experience designing data stores reveals that once a design is
in place, you will always want to throw a lot more data at it that
you thought you were going to.

So in this module, I want to make an extra effort to accommodate all the
data anyone might want to put in a data store.  For that reason, any
file that increases in size as data is stored (toc file, key file, data
file) may be split into multiple files.  Logically, these are one entity:
the toc files are one table of contents, the key files are one index,
the data files make up a single data store.  But by allowing them to be
split up, the file sizes can be kept manageable.

Similarly, since the number of files increases, and too many files in a
single directory can be problematic for a number of reasons, the module
accommodates multiple directories for these files.  That is, as the
number of data files grows, they can be stored in multiple data
directories.  As the number of key files grows, and as the number of
toc files grows, they can be stored in multiple key and toc
directories.

To keep the API simpler, the specs for the data file number can be
applied to the toc files, the key files, and also to the toc, key, and
data directories.  That is, if the data store designer specifies that
the data file number should be two base-36 characters (so he can
accommodate up to 1295 files), that should be more than sufficient to
use for toc's, key's, and dir's.

But some additional parameters are needed:

 - dirmax,  the maximum number of files per directory
 - dirlev,  the number of directory levels for data, toc, and key files
 - tocmax,  the maximum number of entries per toc file
 - keymax,  the maximum number of entries per key file
 - datamax, the maximum size in bytes of any data file (was C<maxfilesize>)

The C<dirmax> and C<dirlev> parms is available for handling big data
stores.  If C<dirmax> is set, C<dirlev> defaults to 1, but may be set
higher.  The C<dirmax> parm will determine when a new directory is
created to hold more data, key, or toc files.

The C<dirlev> parm indicates how many directory levels to maintain.  A
level of 1 will accommodate C<dirmax> * C<dirmax> files.  A level of 2,
C<dirmax> * C<dirmax> * C<dirmax> files, etc.  It's unlikely you'll
need a C<dirlev> higher than 1 so if C<dirmax> is needed at all, just
setting it and letting C<dirlev> default to 1 is the most likely case.

The C<tocmax> and C<keymax> parms will determine when a new toc or key
file is created to hold more entries.  The C<datamax> parm will
determine when a new data file is created: when storing a record would
cause the size of the data file to exceed that number, a new data file
is created for it, and subsequent records are stored there.

=head2 File/Directory Pseudocode

This is what the code should make happen:

- the mininum requirement to instantiate a data store is a uri file
- when FlatFile::DataStore->new() is called
  - if there is an obj file, it is evalled to construct the object
  - otherwise, the uri file is read and parsed, and
    - the object is constructed from the uri parms
    - the object (less the C<dir> attribute) is dumped to an obj file
- when a record is created, updated, or deleted
  - if there isn't a data file yet, one is created per C<datalev>
    - if C<datalev> is 0 (or undef), file is "dir/name.1.data"
    - if 1, file is "dir/name.data1/name.1.data"
    - if 2, file is "dir/name.data1/1/name.1.data"
    - if 3, file is "dir/name.data1/1/1/name.1.data"
    - etc.
    - and the (new, updated, or deleted) record is stored in it
  - otherwise, if there is a data file, the record length is checked
    - if the record length would cause the data file to exceed C<datamax>
      - a new data file is created per C<datalev> as above, except
        - if C<dirmax> then
          - for each containing directory
            - if the new file would cause the directory to exceed C<dirmax>
              - a new directory at the same level is created
              - and if that new directory would increase the parent directory too much
                - a new directory at the parent's level is created
                - ad infinitum
      - and the new data file is stored in its data directory
    - and the (new, updated, or deleted) record is stored in it
  - in any case, if the record is longer than C<datamax>, it's an error.
  - in the case of created
    - if the new record's entry in the key file would exceed C<keymax>
      - a new key file is created per C<keylev> as above, including per C<dirmax>.
    - an entry is added to the key file that is the preamble of the record.
  - in the case of updated or deleted
    - the current record's preamble contains:
      - prevfilenum and prevseekpos with previous record's location
    - the previous version of the record's preamble is modified in place:
      - indicator is changed to oldupd or olddel
      - nextfilenum and nextseekpos are updated with current record's location
    - the current record's entry in the key file is updated with the current record's preamble
  - in all cases, the toc file is updated with the transaction number
    - if a new data file was created
      - if a new entry in the toc file would exceed C<tocmax>
        - a new toc file is created per C<toclev> as above, including per C<dirmax>
      - a new entry is added to the toc file

That's a jumbled mess, but it's my first pass.  All of the logic for
when to create a new file/directory should be encapsulated by routines
that return the desired path.  In the routine, new directories and
files would be created as needed--probably employing integer arithmatic
with C<dirmax>, C<keymax>, and C<tocmax>.

=head2 Toc file structure

The toc file will have a total line at the top and a detail line for
each data file.

The fields in these lines are as follows:
 -  1. len FN, last toc file
 -  2. len FN, last key file
 -  3. len FN, last data file
 -  4. len KN, last keynum
 -  5. len TN, last transaction number
 -  6. len TN, #created
 -  7. len TN, #oldupd
 -  8. len TN, #updated
 -  9. len TN, #olddel
 - 10. len TN, #delete
 - 11. len RS, recsep 

For example:

 1 1 9 00Aj 01Bk 00AK 0027 0027 0003 0003\n

FN is filenumlen; KN is keynumlen; TN is transnumlen; RS is recseplen.

On the detail lines, these values will be of historical/validation
interest.

On the total line, these values will be where the program looks for:
 - last toc file
 - last key file
 - last data file
 - last keynum
 - last transaction number 

On each transaction, the module will update these.

In addition, on each transaction, the module will update:
 - the last line (because it has details for the current data file)
 - possibly another line (in possibly another toc file) (because the transaction may update a preamble in another data file)
 - the first line (crud totals in addition to the "last" numbers) 

Random access by line is accommodated because we know the length of each line and
 - line 0 is the total line
 - line 1 is details for example.1.data
 - line 2 is details for example.2.data
 - etc. 

Random access to a given field is likewise accommodated because we know the lengths of each field.
 - i.e., we can seek to a line and then seek to a field and just read or write that field 

So that adds to the writes a transaction might do:
 - example.1.toc: 2 or 3 writes (i.e., multiple fields on 2 or 3 lines)
 - example.1.key: 2 writes (the last line and a previous line)
 - example.2.data: 2 writes (the last line and a previous line)
 - In each case the second write may be in another file 

=head2 defining a data store

=head2 designing a program to analyze a data store definition

=head2 validating a data store

=head2 migrating a data store

=head2 interating over the data store transactions

=cut

