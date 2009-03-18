#---------------------------------------------------------------------

=head1 NAME

FlatFile::DataStore::Tutorial - POD containing in-depth discussion
of and tutorials for using FlatFile::DataStore.

=head1 VERSION

Discusses FlatFile::DataStore version 0.06.

=head1 SYNOPSYS

 man FlatFile::DataStore
 man FlatFile::DataStore::Tutorial

or

 perldoc FlatFile::DataStore
 perldoc FlatFile::DataStore::Tutorial

or

 http://search.cpan.org/dist/FlatFile-DataStore/

=head1 DESCRIPTION

=head2 Overview

This tutorial only contains POD, so don't do this:

 use FlatFile::DataStore::Tutorial;  # don't do this

Instead, simply read the POD (as you are doing). Also please read
the docs for FlatFile::DataStore, which is essentially the reference
manual.

This tutorial/discussion is intended to augment those docs with
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

Key files act as an index into the data files.  The different versions
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

=head2 defining a data store

=head2 designing a program to analyze a data store definition

=head2 validating a data store

=head2 migrating a data store

=head2 interating over the data store transactions


=cut

