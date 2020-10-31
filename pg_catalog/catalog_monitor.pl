#!/usr/bin/perl
use strict;
use Time::Local;
use File::Basename;
use warnings;
use Switch;
use POSIX ":sys_wait_h";
use POSIX;
open(STDERR, ">&STDOUT");

if ($#ARGV != 0 )  {
  print "Argument number Error\nExample:\nperl $0 dbname\n" ;
  exit (1) ; 
}

my ($hostname,$port,$username,$password,$database);
$hostname="localhost";
$port="5432";
$database=$ARGV[0];
$username=`whoami`;
chomp($username);
my $gpver;


sub set_env
{  
   $ENV{"PGHOST"}="localhost";
   #$ENV{"PGPORT"}="5432";
   $ENV{"PGDATABASE"}=$database;
   $ENV{"PGUSER"}=$username;
   $ENV{"PGPASSWORD"}="";

   return 0;
}


sub get_gpver {
  my @tmpstr;
  my @tmpver;
  my $sql = qq{select version();};
  my $sver=`psql -A -X -t -c "$sql" -d postgres` ;
  my $ret=$?;
  if($ret) { 
    print "Get GP version error!\n";
    exit 1;
  }
  chomp($sver);
  @tmpstr = split / /,$sver;
  print $tmpstr[4]."\n";
  @tmpver = split /\./,$tmpstr[4];
  print $tmpver[0]."\n";
  
  return $tmpver[0];
}



sub catalog_history{
	my $sql;
	my $ret;
  
  $sql = qq{select count(*) from pg_tables;};
  my $table_count=`psql -A -X -t -c "$sql"` ;
  $ret = $? >> 8;
  if ($ret) {
    print("pg_tables count error! \n");
    return(-1);
  }
  chomp($table_count);
  
  $sql = qq{select count(*) from pg_views;};
  my $view_count=`psql -A -X -t -c "$sql"` ;
  $ret = $? >> 8;
  if ($ret) {
    print("pg_views count error! \n");
    return(-1);
  }
  chomp($view_count);
  
  ########pg_namespace
  $sql = qq{select pg_relation_size('pg_namespace');};
  my $pg_namespace_size=`psql -A -X -t -c "$sql"` ;
  $ret = $? >> 8;
  if ($ret) {
    print("pg_namespace size error! \n");
    return(-1);
  }
  chomp($pg_namespace_size);
  
  $sql = qq{select pg_relation_size('pg_namespace');};
  my $pg_namespace_master=`env PGOPTIONS='-c gp_session_role=utility' psql -A -X -t -c "$sql"` ;
  $ret = $? >> 8;
  if ($ret) {
    print("pg_namespace master size error! \n");
    return(-1);
  }
  chomp($pg_namespace_master);
  
  $sql = qq{select pg_relation_size('pg_namespace') from gp_dist_random('gp_id') where gp_segment_id=0;};
  my $pg_namespace_gpseg0=`psql -A -X -t -c "$sql"` ;
  $ret = $? >> 8;
  if ($ret) {
    print("pg_namespace gpseg0 size error! \n");
    return(-1);
  }
  chomp($pg_namespace_gpseg0);
  
  $sql = qq{create temp table tmp_pg_namespace_record as select * from pg_namespace distributed randomly;
            select pg_relation_size('tmp_pg_namespace_record');};
  my $pg_namespace_realsize=`env PGOPTIONS='-c gp_session_role=utility' psql -A -X -q -t -c "$sql"` ;     ####Use -q :run quietly (no messages, only query output)
  $ret = $? >> 8;
  if ($ret) {
    print("pg_namespace realsize error! \n");
    return(-1);
  }
  chomp($pg_namespace_realsize);
  my $pg_namespace_master_bloat = $pg_namespace_master / $pg_namespace_realsize;
  
  $sql = qq{select count(*) from pg_namespace;};
  my $pg_namespace_count=`psql -A -X -t -c "$sql"` ;
  $ret = $? >> 8;
  if ($ret) {
    print("pg_namespace count error! \n");
    return(-1);
  }
  chomp($pg_namespace_count);
  
  ########pg_class
  $sql = qq{select pg_relation_size('pg_class');};
  my $pg_class_size=`psql -A -X -t -c "$sql"` ;
  $ret = $? >> 8;
  if ($ret) {
    print("pg_class size error! \n");
    return(-1);
  }
  chomp($pg_class_size);
  
  $sql = qq{select pg_relation_size('pg_class');};
  my $pg_class_master=`env PGOPTIONS='-c gp_session_role=utility' psql -A -X -t -c "$sql"` ;
  $ret = $? >> 8;
  if ($ret) {
    print("pg_class master size error! \n");
    return(-1);
  }
  chomp($pg_class_master);
  
  $sql = qq{select pg_relation_size('pg_class') from gp_dist_random('gp_id') where gp_segment_id=0;};
  my $pg_class_gpseg0=`psql -A -X -t -c "$sql"` ;
  $ret = $? >> 8;
  if ($ret) {
    print("pg_class gpseg0 size error! \n");
    return(-1);
  }
  chomp($pg_class_gpseg0);
  
  $sql = qq{create temp table tmp_pg_class_record as select * from pg_class distributed randomly;
            select pg_relation_size('tmp_pg_class_record');};
  my $pg_class_realsize=`env PGOPTIONS='-c gp_session_role=utility' psql -A -X -q -t -c "$sql"` ;     ####Use -q :run quietly (no messages, only query output)
  $ret = $? >> 8;
  if ($ret) {
    print("pg_class realsize error! \n");
    return(-1);
  }
  chomp($pg_class_realsize);
  my $pg_class_master_bloat = $pg_class_master / $pg_class_realsize;
  
  $sql = qq{select count(*) from pg_class;};
  my $pg_class_count=`psql -A -X -t -c "$sql"` ;
  $ret = $? >> 8;
  if ($ret) {
    print("pg_class count error! \n");
    return(-1);
  }
  chomp($pg_class_count);
  
  ########pg_attribute
  $sql = qq{select pg_relation_size('pg_attribute');};
  my $pg_attribute_size=`psql -A -X -t -c "$sql"` ;
  $ret = $? >> 8;
  if ($ret) {
    print("pg_attribute size error! \n");
    return(-1);
  }
  chomp($pg_attribute_size);
  
  $sql = qq{select pg_relation_size('pg_attribute');};
  my $pg_attribute_master=`env PGOPTIONS='-c gp_session_role=utility' psql -A -X -t -c "$sql"` ;
  $ret = $? >> 8;
  if ($ret) {
    print("pg_attribute master size error! \n");
    return(-1);
  }
  chomp($pg_attribute_master);
  
  $sql = qq{select pg_relation_size('pg_attribute') from gp_dist_random('gp_id') where gp_segment_id=0;};
  my $pg_attribute_gpseg0=`psql -A -X -t -c "$sql"` ;
  $ret = $? >> 8;
  if ($ret) {
    print("pg_attribute gpseg0 size error! \n");
    return(-1);
  }
  chomp($pg_attribute_gpseg0);

  $sql = qq{create temp table tmp_pg_attribute_record as select * from pg_attribute distributed randomly;
            select pg_relation_size('tmp_pg_attribute_record');};
  my $pg_attribute_realsize=`env PGOPTIONS='-c gp_session_role=utility' psql -A -X -q -t -c "$sql"` ;     ####Use -q :run quietly (no messages, only query output)
  $ret = $? >> 8;
  if ($ret) {
    print("pg_attribute realsize error! \n");
    return(-1);
  }
  chomp($pg_attribute_realsize);
  my $pg_attribute_master_bloat = $pg_attribute_master / $pg_attribute_realsize;
  
  $sql = qq{select count(*) from pg_attribute;};
  my $pg_attribute_count=`psql -A -X -t -c "$sql"` ;
  $ret = $? >> 8;
  if ($ret) {
    print("pg_attribute count error! \n");
    return(-1);
  }
  chomp($pg_attribute_count);
  
  ########pg_partition_rule
  $sql = qq{select pg_relation_size('pg_partition_rule');};    ####pg_partition_rule records only on master
  my $pg_partition_rule_size=`psql -A -X -t -c "$sql"` ;
  $ret = $? >> 8;
  if ($ret) {
    print("pg_partition_rule size error! \n");
    return(-1);
  }
  chomp($pg_partition_rule_size);

  $sql = qq{create temp table tmp_pg_partition_rule_record as select * from pg_partition_rule distributed randomly;
            select pg_relation_size('tmp_pg_partition_rule_record');};
  my $pg_partition_rule_realsize=`env PGOPTIONS='-c gp_session_role=utility' psql -A -X -q -t -c "$sql"` ;     ####Use -q :run quietly (no messages, only query output)
  $ret = $? >> 8;
  if ($ret) {
    print("pg_partition_rule realsize error! \n");
    return(-1);
  }
  chomp($pg_partition_rule_realsize);
  my $pg_partition_rule_bloat = $pg_partition_rule_size / $pg_partition_rule_realsize;
  
  $sql = qq{select count(*) from pg_partition_rule;};
  my $pg_partition_rule_count=`psql -A -X -t -c "$sql"` ;
  $ret = $? >> 8;
  if ($ret) {
    print("pg_partition_rule count error! \n");
    return(-1);
  }
  chomp($pg_partition_rule_count);
  
  ########pg_statistic
  $sql = qq{select pg_relation_size('pg_statistic');};    ####pg_statistic records only on master
  my $pg_statistic_size=`psql -A -X -t -c "$sql"` ;
  $ret = $? >> 8;
  if ($ret) {
    print("pg_statistic size error! \n");
    return(-1);
  }
  chomp($pg_statistic_size);
  
  $sql = qq{select count(*) from pg_statistic;};
  my $pg_statistic_count=`psql -A -X -t -c "$sql"` ;
  $ret = $? >> 8;
  if ($ret) {
    print("pg_statistic count error! \n");
    return(-1);
  }
  chomp($pg_statistic_count);
  
  #$sql = qq{insert into gpcheck_admin.catalog_info_history values (
  #	        '$database',$table_count,$view_count,
  #	        $pg_namespace_size,$pg_namespace_master,$pg_namespace_gpseg0,$pg_namespace_master_bloat,$pg_namespace_count,
  #	        $pg_class_size,$pg_class_master,$pg_class_gpseg0,$pg_class_master_bloat,$pg_class_count,
  #	        $pg_attribute_size,$pg_attribute_master,$pg_attribute_gpseg0,$pg_attribute_master_bloat,$pg_attribute_count,
  #	        $pg_partition_rule_size,$pg_partition_rule_bloat,$pg_partition_rule_count,
  #	        $pg_statistic_size,$pg_statistic_count,
  #	        now()::timestamp without time zone)
  #	       };
  #`psql -A -X -t -c "$sql"` ;
  #$ret = $? >> 8;
  #if ($ret) {
  #  print("insert into gpcheck_admin.catalog_info_history error! \n");
  #  return(-1);
  #}
  print("---$database---pg_catalog info---\n");
  print("pg_tables count:                     $table_count\n");
  print("pg_views count:                      $view_count\n");
  print("pg_namespace count:                  $pg_namespace_count\n");
  print("pg_namespace size:                   $pg_namespace_size\n");
  print("pg_namespace size in master:         $pg_namespace_master\n");
  print("pg_namespace size in gpseg0:         $pg_namespace_gpseg0\n");
  print("pg_namespace bloat in master:        $pg_namespace_master_bloat\n");
  print("pg_class count:                      $pg_class_count\n");
  print("pg_class size:                       $pg_class_size\n");
  print("pg_class size in master:             $pg_class_master\n");
  print("pg_class size in gpseg0:             $pg_class_gpseg0\n");
  print("pg_class bloat in master:            $pg_class_master_bloat\n");
  print("pg_attribute count:                  $pg_attribute_count\n");
  print("pg_attribute size:                   $pg_attribute_size\n");
  print("pg_attribute size in master:         $pg_attribute_master\n");
  print("pg_attribute size in gpseg0:         $pg_attribute_gpseg0\n");
  print("pg_attribute bloat in master:        $pg_attribute_master_bloat\n");
  print("pg_partition_rule count:             $pg_partition_rule_count\n");
  print("pg_partition_rule size in master:    $pg_partition_rule_size\n");
  print("pg_partition_rule bloat in master:   $pg_partition_rule_bloat\n");
  print("pg_statistic count:                  $pg_statistic_count\n");
  print("pg_statistic size in master:         $pg_statistic_size\n");
  
  print("\n");

  
}




sub main{
  set_env();
  $gpver=get_gpver();

  catalog_history();
  
  return 0;  
}

my $ret = main();

exit $ret;
