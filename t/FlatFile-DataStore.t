use strict;
use warnings;

use Test::More 'no_plan';
use File::Temp qw/ tempfile tempdir /;
use Data::Dumper;
$Data::Dumper::Terse    = 1;
$Data::Dumper::Indent   = 0;
$Data::Dumper::Sortkeys = 1;

BEGIN { use_ok('FlatFile::DataStore') };

my $dir  = tempdir( CLEANUP => 0 );
my $name = "example";
my $desc = "Example+FlatFile::DataStore";

{  # accessors

    my $uri = join ";",
        qq'http://example.com?name=$name',
        qq'desc=$desc',
        qw(
            datamax=9_000
            recsep=%0A
            indicator=1-%2B%23%3D%2A%2D
            date=8-yyyymmdd
            transnum=2-10
            keynum=2-10
            reclen=2-10
            thisfnum=1-10
            thisseek=4-10
            prevfnum=1-10
            prevseek=4-10
            nextfnum=1-10
            nextseek=4-10
            user=10-%20-%7E
        );

    my $urifile = "$dir/$name.uri";
    open my $fh, '>', $urifile or die qq/Can't open $urifile: $!/;
    print $fh $uri;
    close $fh;

    my $ds = FlatFile::DataStore->new( {
        dir  => $dir,
        name => $name,
    } );

    ok( $ds );

    is( $ds->dir,         $dir,         "dir()"         );
    is( $ds->name,        $name,        "name()"        );
    is( $ds->desc,        $desc,        "desc()"        );
    is( $ds->datamax,     9_000,        "datamax()"     );
    is( $ds->recsep,      "\x0A",       "recseip()"     );
    is( $ds->indicator,   "1-+#=*-",    "indicator()"   );
    is( $ds->date,        "8-yyyymmdd", "date()"        );
    is( $ds->transnum,    "2-10",       "transnum()"    );
    is( $ds->keynum,      "2-10",       "keynum()"      );
    is( $ds->reclen,      "2-10",       "reclen()"      );
    is( $ds->thisfnum,    "1-10",       "thisfnum()"    );
    is( $ds->thisseek,    "4-10",       "thisseek()"    );
    is( $ds->prevfnum,    "1-10",       "prevfnum()"    );
    is( $ds->prevseek,    "4-10",       "prevseek()"    );
    is( $ds->nextfnum,    "1-10",       "nextfnum()"    );
    is( $ds->nextseek,    "4-10",       "nextseek()"    );
    is( $ds->user,        "10- -~",     "user()"        );
    is( $ds->dateformat,  "yyyymmdd",   "dateformat()"  );
    is( $ds->fnumbase,    "10",         "fnumbase()"    );
    is( $ds->fnumlen,     1,            "fnumlen()"     );
    is( $ds->uri,         $uri,         "uri()"         );
    is( $ds->preamblelen, 40,           "preamblelen()" );
    is( $ds->translen,    2,            "translen()"    );
    is( $ds->transbase,   "10",         "transbase()"   );
    is( Dumper($ds->crud),
        "{'create' => '+','delete' => '-','olddel' => '*','oldupd' => '#','update' => '='}",
        "crud()" );
    is( Dumper($ds->regx),
        "qr/(?-xism:([+#=*-])([0-9]{8})([-0-9]{2})([-0-9]{2})([-0-9]{2})([-0-9])([-0-9]{4})([-0-9])([-0-9]{4})([-0-9])([-0-9]{4})([ -~]{10}))/",
        "regx()" );
    is( Dumper($ds->specs),
        "{'indicator' => [0,1,'+#=*-']}{'date' => [1,8,'yyyymmdd']}{'transnum' => [9,2,'10']}{'keynum' => [11,2,'10']}{'reclen' => [13,2,'10']}{'thisfnum' => [15,1,'10']}{'thisseek' => [16,4,'10']}{'prevfnum' => [20,1,'10']}{'prevseek' => [21,4,'10']}{'nextfnum' => [25,1,'10']}{'nextseek' => [26,4,'10']}{'user' => [30,10,' -~']}",
        "specs()" );
}

{  # crud

    my( $y, $m, $d ) = sub{($_[5]+1900,$_[4]+1,$_[3])}->(localtime);
    my $yyyy_mm_dd   = sprintf "%04d-%02d-%02d", $y, $m, $d;
    my $yyyymmdd     = sprintf "%04d%02d%02d",   $y, $m, $d;

    my $ds = FlatFile::DataStore->new( {
        dir  => $dir,
        name => $name,
    } );

    ok( $ds );

    my $user_data   = "Testing1";
    #                  ----+----1----+----2---- (reclen:24)
    my $record_data = "This is testing record1.";

    my $record = $ds->create( $record_data, $user_data );
    is( Dumper($record),
        qq/bless( {'data' => \\'This is testing record1.','preamble' => bless( {'date' => '$yyyy_mm_dd','indicator' => '+','keynum' => 0,'reclen' => 24,'string' => '+${yyyymmdd}01002410000----------Testing1  ','thisfnum' => '1','thisseek' => 0,'transnum' => 1,'user' => 'Testing1'}, 'FlatFile::DataStore::Preamble' )}, 'FlatFile::DataStore::Record' )/,
        "create()" );

    my $data = $record->data();
    is( $$data, "This is testing record1.", "data()" );

    my $keynum = $record->keynum();
    is( $keynum, 0, "keynum()" );

    my $user = $record->user();
    is( $user, "Testing1", "user()" );

    my $string = $record->string();
    is( $string, "+${yyyymmdd}01002410000----------Testing1  ", "string()" );

    my $indicator = $record->indicator();
    is( $indicator, "+", "indicator()" );

    my $date = $record->date();
    is( $date, $yyyy_mm_dd, "date()" );

    my $reclen = $record->reclen();
    is( $reclen, 24, "reclen()" );

    my $transnum = $record->transnum();
    is( $transnum, 1, "transnum()" );

    my $thisfnum = $record->thisfnum();
    is( $thisfnum, "1", "thisfnum()" );

    my $thisseek = $record->thisseek();
    is( $thisseek, 0, "thisseek()" );

    my $record2 = $ds->retrieve( $keynum );
    is( Dumper($record), Dumper($record2), "retrieve()" );

    my $updrec = $ds->update( $record, "Updated Record", "Updated1" );

    my $rec_data = $updrec->data;
    is( $$rec_data,    "Updated Record", "rec->data()" );
    is( $updrec->user, "Updated1",       "rec->user()" );

    my $delrec = $ds->delete( $updrec );
    is( $delrec->indicator, $ds->crud()->{'delete'}, "deleted indicator()" );

}

