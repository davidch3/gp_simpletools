#!/usr/bin/perl
use strict;
use Time::Local;
use Switch;

if ($#ARGV != 0 )  {
  print "Argument number Error\nExample:\nperl $0 run_command\n" ;
  exit (1) ; 
}

my $run_cmd = $ARGV[0];


sub getCurrentDateTime
{
   my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time());
   my $current = "";
   
   $year += 1900;
   $mon = sprintf("%02d", $mon + 1);
   $mday = sprintf("%02d", $mday);
   $hour = sprintf("%02d", $hour);
   $min  = sprintf("%02d", $min);
   $sec  = sprintf("%02d", $sec);
   $current = "${year}${mon}${mday}${hour}${min}${sec}";

   return $current;
}


sub get_gpver {
  my @tmpstr;
  my @tmpver;
  my $sql = qq{select version();};
  my $sver=`PGOPTIONS='-c gp_session_role=utility' psql -A -X -t -c "$sql" -d postgres` ;
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




sub get_seg_dir {
  #0 all instances
  #1 master
  #2 gpseg0
  my ($iflag) = @_;
  my $sql;
  my $gpver=get_gpver();
  
  switch ($iflag) {
    case 0 { 
      if ($gpver>=6) {
        $sql =qq{ 
          SELECT conf.hostname||','||conf.datadir
          FROM gp_segment_configuration conf
          ORDER BY conf.dbid;
        };
      } else {
        $sql =qq{ 
          SELECT conf.hostname||','||pgfse.fselocation
          FROM pg_filespace_entry pgfse, gp_segment_configuration conf
          WHERE pgfse.fsefsoid=3052 AND conf.dbid=pgfse.fsedbid
          ORDER BY conf.dbid;
        };
      }  
    }
    case 1 {
      if ($gpver>=6) { 
        $sql =qq{ 
          SELECT conf.hostname||','||conf.datadir
          FROM gp_segment_configuration conf
          WHERE conf.content=-1 AND conf.dbid=1;
        };
      } else {
        $sql =qq{ 
          SELECT conf.hostname||','||pgfse.fselocation
          FROM pg_filespace_entry pgfse, gp_segment_configuration conf
          WHERE pgfse.fsefsoid=3052 AND conf.dbid=pgfse.fsedbid
          AND conf.content=-1 AND conf.dbid=1;
        };
      }
    }
    case 2 {
      if ($gpver>=6) { 
        $sql =qq{ 
          SELECT conf.hostname||','||conf.datadir
          FROM gp_segment_configuration conf
          WHERE conf.content=0 AND conf.dbid=2;
        };
      } else {
        $sql =qq{ 
          SELECT conf.hostname||','||pgfse.fselocation
          FROM pg_filespace_entry pgfse, gp_segment_configuration conf
          WHERE pgfse.fsefsoid=3052 AND conf.dbid=pgfse.fsedbid
          AND conf.content=0 AND conf.dbid=2;
        };
      }
    }
  }
  
  print "PGOPTIONS='-c gp_session_role=utility' psql -A -X -t -c \"$sql\" -d postgres\n";
  my @tmplist=`PGOPTIONS='-c gp_session_role=utility' psql -A -X -t -c "$sql" -d postgres` ;
  my $ret=$?;
  if($ret) { 
    my $sInfo="psql error =$sql="; 
    return(1,$sInfo);
  }
  
  return(0,@tmplist);
  
}


sub run_on_segdir{
  my @cmdstr;
  my @seg_dir_list;
  my $ret;
  my $cmd;
  my $mychoice;
  my $checkresult;
  
  #get segment list
  ($ret,@seg_dir_list)=get_seg_dir(0);
  if ($ret) { return $ret; }
  #print @seg_dir_list;
    
  #Confirmation info
  @cmdstr=split /,/,$seg_dir_list[0];
  chomp($cmdstr[0]);
  chomp($cmdstr[1]);
  $cmd=qq{ssh $cmdstr[0] "cd $cmdstr[1]; $run_cmd"};
  while (1==1) {
    print "Please Confirm Command: \n$cmd\n(Yy/Nn)\n";
    $mychoice=<STDIN>;
    chomp($mychoice);
    #print "My option is <".$mytmp.">\n";
    if ( $mychoice eq "y" || $mychoice eq "Y" ) {last;}
    elsif ( $mychoice eq "n" || $mychoice eq "N" ) {exit 0;}
  }
  
  my $jj;
  for ($jj=0;$jj<$#seg_dir_list+1;$jj++) {
    @cmdstr=split /,/,$seg_dir_list[$jj];
    chomp($cmdstr[0]);
    chomp($cmdstr[1]);
    $cmd=qq{ssh $cmdstr[0] "cd $cmdstr[1]; $run_cmd" 2>&1};
    print "cmd[".$cmd."]\n";
    $checkresult=`$cmd`;
    print $checkresult;
  }
  
  return 0;
}



############################################################################################
########################################Main funcion########################################
############################################################################################
sub main{
  my $ret;
    
  $ret=run_on_segdir();
  return($ret);
}

my $ret = main();
exit($ret);


