use strict;
use warnings;

use Test::More 'no_plan';
use File::Path;
use URI::Escape;
use Data::Dumper;
$Data::Dumper::Terse    = 1;
$Data::Dumper::Indent   = 0;
$Data::Dumper::Sortkeys = 1;

#---------------------------------------------------------------------
# tempfiles cleanup

sub delete_tempfiles {
    my( $dir ) = @_;
    return unless $dir;

    for( glob "$dir/*" ) {
        if( -d $_ ) { rmtree( $_ ) }
        else        { unlink $_ or die "Can't delete $_: $!" }
    }
}

my $dir;
BEGIN { $dir  = "./tempdir"      }
NOW:  { delete_tempfiles( $dir ) }
END   { delete_tempfiles( $dir ) }

#---------------------------------------------------------------------
BEGIN { use_ok('FlatFile::DataStore') };

my $name = "example";
my $desc = "Example FlatFile::DataStore";

{  # uri configuration pod

    my $name = 'example';
    # my $dir  = '/example/dir';  (commented out to use our $dir)

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

ok( $datastore, "uri configuration" );


 my $ds = FlatFile::DataStore::->new(
     { dir  => $dir,
       name => $name,
     } );

ok( $ds, "new(dir,name)" );

}

