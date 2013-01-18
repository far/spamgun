#!/usr/bin/perl


#sub POE::Kernel::ASSERT_DEFAULT () { 1 }
#sub POE::Kernel::TRACE_DEFAULT () { 1 }
use POE;
use POE::Component::Client::TCP;
use POE::Component::LaDBI;
use DBI;

$|++;

&daemonize();

$SIG{TERM} = \&SIGTERM_handler;

DESTROY {

	unlink '/var/log/mmaild/.pid';

}

use constant DEBUG			=> 1;
use constant SET_ERRSTR			=> 0;
use constant CHECK_NUMROWS_TIMEOUT	=> 10;
use constant NOMMSG_TIMEOUT		=> 5;
use constant SEND_MMSG_TIMEOUT		=> 2;
use constant CHECK_COMMAND_TIMEOUT	=> 5;
use constant MRCPT_LIMIT		=> 100;

use constant SIG1 => 'TERM';

our $config_file = '/home/audial/mmaild/config';

my $db_host	= 'localhost';
my $db_name	= 'mmail3';
my $db_user	= 'root';
my $db_pass	= '';
my $dsn		= "DBI:mysql:database=$db_name;host=$db_host;";

my $dbh = DBI->connect("DBI:mysql:database=$db_name;host=$db_host;", $db_user, $db_pass);
my $sth = $dbh->prepare(q{
		UPDATE mmanage 
		SET mstate=1, mcommand=2
});
if( ! $sth->execute() ) {
	die "\nCan't set state of daemon in database: $!";
}



our @mservers_arr = ();

&get_mservers();

sub get_mservers {

	open CONFIG, "<$config_file";

	while( <CONFIG> ) {
	
		s/\s//g;
	
		if( $_ !~ /#/ && $_ ne '' ) {
			
			my @arr = split /\|/, $_;
	
			push @mservers_arr, \@arr;

		}
	}

	close CONFIG;

}



	if( $#mservers_arr < 0 ) {
		
		die("No data about mail servers. No sockets to build. Exit.");	

	}


	my $LADBI_ALIAS = 'ladbi';
	my $SIGSESS_COMP_ALIAS = 'SIGNAL_SESSION_COMPONENT';

	POE::Component::LaDBI->create( Alias => $SIGSESS_COMP_ALIAS );

	POE::Session->create 
	  ( args => [ $dsn, $db_user, $db_pass ],
	  inline_states =>
	   { 
 	     _start		=> sub {
	
		my ( $dsn, $db_user, $db_pass ) = @_[ARG0..ARG2];
	
		$_[KERNEL]->call( 
			$SIGSESS_COMP_ALIAS	=> "register",
			OfflineEvent		=> 'db_offline'
		);  

		$_[KERNEL]->sig( SIG1, stop_handler );

		$_[KERNEL]->alias_set( "SIGNAL_SESSION" );	
		
		print "\nSIGNAL SESSION: Session starting...";	

		$_[KERNEL]->post( $SIGSESS_COMP_ALIAS => "connect",
					SuccessEvent => "_temp",
					Args => [ $dsn, $db_user, $db_pass] 
		);
	     },

	     _temp		=> sub {

	
		$_[HEAP]->{dbh_id} = $_[ARG0];

		$_[HEAP]->{state} = 1;
		$_[KERNEL]->yield( "prepare_check_command" );
		
	     },


	     set_state	=> sub {

		$_[KERNEL]->post( $SIGSESS_COMP_ALIAS => "do",
					SuccessEvent => "check_command",
				 	FailureEvent => "db_error",
				 	HandleId     => $_[HEAP]->{dbh_id},
				 	Args	     => [ "UPDATE mmanage SET mstate='".$_[HEAP]->{state}."'" ]
		);
		
	     },

	     set_wait		=> sub {

                my $dbi_session = $_[KERNEL]->alias_resolve( "DBI_SESSION" );
		$dbi_session->get_heap()->{numrows_all_mrcpt} = -1;
		$dbi_session->get_heap()->{waiting} = 1;
	
		foreach my $mserver_alias ( @mservers_aliases ) {
			
			my $mserver_session = $_[KERNEL]->alias_resolve( $mserver_alias );

			if( defined $mserver_session ) {
				if( defined $mserver_session->get_heap()->{wheel} ) {
					$mserver_session->get_heap()->{wheel}->put( "QUIT" );
				}
				$mserver_session->get_heap()->{last_mrcpt_id} = -1;
				$mserver_session->get_heap()->{starting} = 0;
				$mserver_session->get_heap()->{exiting} = 0;
				$mserver_session->get_heap()->{waiting} = 1;
			}
	
			$_[KERNEL]->post( $mserver_alias => "stay_waiting" );
		}

		$_[KERNEL]->yield( "set_state" );

	
	     },

	     set_start		=> sub {

		my $dbi_session = $_[KERNEL]->alias_resolve( "DBI_SESSION" );
		$dbi_session->get_heap()->{starting} = 1;
		$_[KERNEL]->post( "DBI_SESSION", 'get_mmsg_batch' );

		foreach my $mserver_alias ( @mservers_aliases ) {
			
			my $mserver_session = $_[KERNEL]->alias_resolve( $mserver_alias );

			if( defined $mserver_session ) {
				$mserver_session->get_heap()->{starting} = 1;
				$mserver_session->get_heap()->{exiting} = 0;
				$mserver_session->get_heap()->{waiting} = 0;
			}

			$_[KERNEL]->post( $mserver_session, 'get_message' );
		}

		$_[HEAP]->{state} = 1;
		$_[KERNEL]->yield( "set_state" );

	     },

	     set_exit		=> sub {

		print "\nSIGNAL SESSION: set_exit";

		foreach my $mserver_alias ( @mservers_aliases ) {
			
			$_[KERNEL]->post( $mserver_alias => "shutdown" );


			my $mserver_session = $_[KERNEL]->alias_resolve( $mserver_alias );

			if( defined $mserver_session ) {
				$mserver_session->get_heap()->{starting} = 0;
				$mserver_session->get_heap()->{exiting} = 1;
				$mserver_session->get_heap()->{waiting} = 0;
				
		#		if( $_[KERNEL]->alias_resolve( $mserver_alias )->get_heap()->{step} < 5 ) {
					if( defined $mserver_session->get_heap()->{wheel} ) {
						$mserver_session->get_heap()->{wheel}->put( "QUIT" );
					}
					$mserver_session->get_heap()->{sess_state} = 0;
					$_[KERNEL]->post( $mserver_alias => "shutdown" );
		#		}
			}

		}

		#$_[KERNEL]->post( "DBI_SESSION" => "check_smtp_sessions" ); 
		$_[KERNEL]->alias_resolve( "DBI_SESSION" )->get_heap()->{exiting} = 1;

		$_[KERNEL]->post( "DBI_SESSION" => "shutdown" );

		$_[KERNEL]->yield( "shutdown ");

	     },

	     prepare_check_command	=> sub {

		$_[KERNEL]->post( $SIGSESS_COMP_ALIAS => "prepare",
					SuccessEvent => "execute_check_command",
				 	FailureEvent => "db_error",
				 	HandleId     => $_[HEAP]->{dbh_id},
				 	Args	     => [ "SELECT mcommand FROM mmanage" ]
		);
		
	     },

	     execute_check_command	=> sub {

		$_[HEAP]->{sth_check_command} = $_[ARG0];

		$_[KERNEL]->post( $SIGSESS_COMP_ALIAS => "execute",
				 	SuccessEvent => "executed_check_command",
				 	FailureEvent => "db_error",
				 	HandleId     => $_[ARG0]
		); 
					
	     },

	     executed_check_command	=> sub {
		$_[KERNEL]->post( $SIGSESS_COMP_ALIAS => "fetchrow",
				 	SuccessEvent => "command_fetched",	
				 	FailureEvent => "db_error", 
				 	HandleId     => $_[HEAP]->{sth_check_command}
		);    
	     },
	
	     command_fetched	=> sub {


		if( $_[ARG2]->[0] != $_[HEAP]->{state} ) {
	
			if( $_[ARG2]->[0] == 0 ) {
				$_[KERNEL]->yield( "set_exit" );
			}
			elsif( $_[ARG2]->[0] == 1 ) {
				$_[KERNEL]->yield( "set_start" );
			}
			elsif( $_[ARG2]->[0] == 2 ) {
				$_[KERNEL]->yield( "set_wait" );
			}	


		}

		$_[KERNEL]->delay( "check_command", CHECK_COMMAND_TIMEOUT );

	     },

	     check_command	=> sub {


		if( $_[HEAP]->{state} != 0 ) {
	
			if( ! defined $_[HEAP]->{sth_check_command} ) {
				$_[KERNEL]->yield( "prepare_check_command" );
			}
			else {
				$_[KERNEL]->post( $SIGSESS_COMP_ALIAS => "execute",
						 	SuccessEvent => "executed_check_command",
						 	FailureEvent => "db_error",
					 		HandleId     => $_[HEAP]->{sth_check_command}
				); 
			}
		}
		else {
			$_[KERNEL]->yield( "shutdown" );
		}
			
	     },

	     db_error		=> sub {
		print STDERR "\nDBI ERROR: ".@_;
		print "\nDBI ERROR: $_[ARG2]";
    	     },

	     stop_handler	=> sub {

		$_[KERNEL]->sig_handled();	     

	     },

	     reconnect		=> sub {

		print "\nSIGNAL SESSION: reconnecting...";
		print STDERR "\nSIGNAL SESSION: reconnnecting...";

		$_[KERNEL]->post($SIGSESS_COMP_ALIAS => "connect",
				 SuccessEvent => "prepare_check_command",
				 FailureEvent => "db_error",
				 Args => [ $dsn, $db_user, $db_pass ]
		);

	     },

	     db_offline		=> sub {
	
		print "\nSIGNAL SESSION: Error : ".$_[ARG1];
		print "\nSIGNAL SESSION: [offline] going to reconnect...";

		$_[KERNEL]->yield( "reconnect" );		

	     },
	     _stop		=> sub {

		print "\nSIGNAL SESSION: Session ended.";

	     }
	   }
	);


#
#	DBI COMPONENT CREATING
#

	POE::Component::LaDBI->create( Alias => "ladbi" );
	
	POE::Session->create
	  (args => [ $dsn, $db_user, $db_pass ],
	   inline_states => 
	    {
	     _start		=> sub { 
		my ( $dsn, $db_user, $db_pass ) = @_[ARG0..ARG2];

		$_[KERNEL]->call(
			$LADBI_ALIAS => "register",
			OfflineEvent => 'db_offline'
		);

		$_[KERNEL]->sig( SIG1, stop_handler );

		$_[KERNEL]->alias_set( "DBI_SESSION" ); 

		print "\nDBI: Session starting...";


		$_[HEAP]->{sql_all_mmsg} = "SELECT msg.mmsg_id, msg.mmsg_subject, msg.mmsg_conttype, msg.mmsg_data, list.mlist_id, rcpt.mrcpt_id, rcpt.mrcpt_to FROM mmsg msg, mlist list, mrcpt rcpt WHERE rcpt.mlist_id=list.mlist_id AND msg.mmsg_id=list.mmsg_id AND rcpt.mrcpt_sended=0 AND list.mlist_active=1 ORDER BY list.mlist_priority ASC, rcpt.mrcpt_id ASC LIMIT ".MRCPT_LIMIT;


		$_[HEAP]->{starting} = 1;

 		$_[KERNEL]->post($LADBI_ALIAS => "connect",	
				 SuccessEvent => "prepare_mmsg",
				 FailureEvent => "db_error",
				 Args => [ $dsn, $db_user, $db_pass] 
		);
	     },

	     reconnect		=> sub {

		print "\nDBI: reconnecting...";
		print STDERR "\nDBI: reconnnecting...";

		$_[KERNEL]->post($LADBI_ALIAS => "connect",
				 SuccessEvent => "prepare_mmsg",
				 FailureEvent => "db_error",
				 Args => [ $dsn, $db_user, $db_pass ]
		);

	     },

	     _stop		=> sub {

		print "\nDBI: Session ended.";

	     },

	     check_smtp_sessions => sub {


		my $have_smtp_sessions = 0;

		foreach my $mserver ( @mservers_data ) {

			if( $_[KERNEL]->alias_resolve( $mserver->[6] )->get_heap()->{sess_state} ) {
				$have_smtp_sessions = 1;
				last;
			}	

		}
		
		if( ! $have_smtp_sessions ) {
			$_[KERNEL]->post( "DBI_SESSION" => "shutdown" );
		}
					

	     },

	     shutdown		=> sub {
		print "\nDBI: Sending shutdown...";
		$_[KERNEL]->post($LADBI_ALIAS => "shutdown");
	     },
	
	     prepare_mmsg	=> sub {

		my $dbh_id = $_[ARG0];
		$_[HEAP]->{dbh_id} = $dbh_id;
		$_[KERNEL]->post($LADBI_ALIAS => "prepare",
				 SuccessEvent => "execute_mmsg",
				 FailureEvent => "db_error",
				 HandleId     => $dbh_id,
				 Args	      => [ $_[HEAP]->{sql_all_mmsg} ]
		);
		
	     },

	     execute_mmsg	=> sub {
	     	my $sth_id = $_[ARG0]; 
		$_[HEAP]->{sth_all_mmsg} = $sth_id;
		$_[KERNEL]->post($LADBI_ALIAS => "execute",
				 SuccessEvent => "executed_mmsg",
				 FailureEvent => "db_error",
				 HandleId     => $sth_id
		); 
					
	     },

	     executed_mmsg	=> sub {

		print "\nDBI: executed_mmsg" if DEBUG;
		print "\nDBI: numrows=".$_[ARG2] if DEBUG;

		$_[HEAP]->{numrows_all_mrcpt} = $_[ARG2];

		if( $_[HEAP]->{starting} ) {
			$_[HEAP]->{waiting} = 0;
			$_[HEAP]->{starting} = 0;
		}
	
		$_[KERNEL]->delay( "check_num_rows", CHECK_NUMROWS_TIMEOUT );

	     },

	     check_num_rows	=> sub {
	
		print "\nDBI: check_num_rows" if DEBUG;

		if( $_[HEAP]->{waiting} ) {
			$_[KERNEL]->yield("get_mmsg_batch");
		}
		elsif( $_[HEAP]->{exiting} ) {
			print "\nDBI: going to shutdown";
		}
		else {

			if( $_[HEAP]->{numrows_all_mrcpt} < 1 ) {
				$_[KERNEL]->yield("get_mmsg_batch");
			} 
			else {
			    $_[KERNEL]->delay( "check_num_rows", CHECK_NUMROWS_TIMEOUT );
			}
		}

	     },	    

	     get_mmsg_batch	=> sub {
		
		print "\nDBI: get_mmsg_batch" if DEBUG;
		$_[KERNEL]->post($LADBI_ALIAS => "execute",
				 SuccessEvent => "executed_mmsg",
				 FailureEvent => "db_error",
				 HandleId     => $_[HEAP]->{sth_all_mmsg}
		);	     	
	     }, 
	
	     get_mmsg		=> sub {
	
		print "\nDBI: get_mmsg" if DEBUG;

		if( $_[HEAP]->{starting} ) {
			my @arr = (0);
			$_[ARG0]->( @arr );
		}
		elsif( $_[HEAP]->{numrows_all_mrcpt} < 1 ) {
			my @arr = (0);
			$_[ARG0]->( @arr );
		}
		else {

			$_[KERNEL]->post($LADBI_ALIAS => "fetchrow",
					 SuccessEvent => "mmsg_fetched",	
					 FailureEvent => "not_fetched", 
					 HandleId     => $_[HEAP]->{sth_all_mmsg},
					 UserData     => $_[ARG0]
			);
		}

	     },

	     mmsg_fetched	=> sub {

	     	print "\nDBI: mmsg_fetched" if DEBUG;

		$_[ARG3]->( @{$_[ARG2]} );

	     },	     

	     set_errstr	=> sub {

		$_[ARG1] =~ s/\'/\\\'/g;

		my $sql_update = "";

		if( SET_ERRSTR ) {

	     		$sql_update = "UPDATE mrcpt SET mrcpt_errstr='".$_[ARG1]."', mrcpt_sended=-1 WHERE mrcpt_id=".$_[ARG0];
		}
		else {
			$sql_update = "UPDATE mrcpt SET mrcpt_sended=-1 WHERE mrcpt_id=".$_[ARG0];
		}

	        $_[KERNEL]->post($LADBI_ALIAS => "do",
				 SuccessEvent => "dec_num_msg",
				 FailureEvent => "db_error",
				 HandleId     => $_[HEAP]->{dbh_id},
				 Args	      => [ $sql_update ],
				 UserData     => $_[ARG0]
		);

	     },

	     set_sended		=> sub {
	     
		print "\nDBI: set sended flag for recepient ID (rcpt_id) = ".$_[ARG0]." from session ID = ".$_[SENDER]->ID if DEBUG;
		
		my $sql_update = "UPDATE mrcpt SET mrcpt_sended=1 WHERE mrcpt_id=".$_[ARG0];

		$_[KERNEL]->post($LADBI_ALIAS => "do",
				 SuccessEvent => "dec_num_msg",
				 FailureEvent => "db_error",
				 HandleId     => $_[HEAP]->{dbh_id},
				 Args	      => [ $sql_update ]
		);

	     },

	     dec_num_msg	=> sub {
	
		$_[HEAP]->{numrows_all_mrcpt} = $_[HEAP]->{numrows_all_mrcpt} - 1;
		$_[KERNEL]->yield( "check_num_rows" );	
	
	     },	

	     db_error		=> sub {
		print STDERR "\nDBI ERROR: ".@_;
		print "\nDBI ERROR: $_[ARG2]";
    	     },

	     not_fetched	=> sub {
		print "\nDBI fetchrow error ".$_[ARG1].$_[ARG2].$_[ARG3] if DEBUG;
		$_[ARG4]->( undef );
	     },
	     
	     db_offline		=> sub {
	
		if( ! $_[HEAP]->{exiting} ) {

			print "\nDBI: Error : ".$_[ARG1];
			print "\nDBI: [offline] going to reconnect...";

			$_[KERNEL]->yield( "reconnect" );		

		}
	     },
	
	     stop_handler	=> sub {

		$_[KERNEL]->sig_handled();	     

	     }
	     
	   }
	);

#
#	Client::TCP COMPONENT CREATING 
#

	our @mservers_aliases = ();

	for ( @mservers_arr ) {

	   for( my $i=0; $i < $_->[4]; $i++ ) {
			
		push @mservers_aliases, "mserver_id:".$_->[0].";mserver_sock_idx:".$i;
	
		$_->[2] |= '25';

		POE::Component::Client::TCP->new
		  ( RemoteAddress	=> $_->[1],
		    RemotePort		=> $_->[2],
		    SessionParams	=> [ options => { debug => 0, trace => 0 } ],

		    Alias		=> "mserver_id:".$_->[0].";mserver_sock_idx:".$i, 
			
		    Started		=> sub {

			$_[KERNEL]->sig( SIG1, stop_handler ); 

			$_[HEAP]->{sess_state} = 1;

		    	$_[HEAP]->{mserver_host} = $_[ARG0];
			$_[HEAP]->{mserver_port} = $_[ARG1];
			$_[HEAP]->{mserver_from} = $_[ARG2];
			$_[HEAP]->{last_rcpt_id} = -1;
			$_[HEAP]->{waiting} = 1;
		    },

		    Args		=> [ $_->[1], $_->[2], $_->[5] ],  
		
		    Connected		=> sub {
			print "\nConnected";
			$_[HEAP]->{step} = "0";
			$_[HEAP]->{sess_state} = 1;
			if( $_[HEAP]->{starting} ) {
				$_[KERNEL]->yield( "get_message" );
			}
			elsif( $_[HEAP]->{waiting} ) {
				$_[KERNEL]->yield( "stay_waiting" );
			}
			elsif( $_[HEAP]->{exiting} ) {
				$_[KERNEL]->yield( "shutdown" );
			} 
		    },	
		    ConnectError	=> sub {
			print STDERR "\nSESSION ".$_[SESSION]->ID." Error : ".$_[ARG2]; 
			$_[KERNEL]->yield( "reconnect" );
		    }, 

		    ServerError		=> sub {
			print STDERR "\nSESSION ".$_[SESSION]->ID." ServerError: ".$_[ARG2];		    
			print STDERR "\nSESSION ".$_[SESSION]->ID." reconnecting...";
			$_[KERNEL]->yield( "reconnect" );
		    },


		    ServerInput		=> sub {

			print "\nSESSION ".$_[SESSION]->ID." got message: $_[ARG0]" if DEBUG;

			$_[HEAP]->{servlog} .= "\n".$_[ARG0];

			if( $_[HEAP]->{waiting} ) {
				$_[KERNEL]->post( $_[SESSION] => "stay_waiting" );
			}

			if( $_[HEAP]->{step} == 0 ) {
		
				$_[KERNEL]->delay( "get_message", 3 );

			}
			elsif( $_[HEAP]->{step} > 0 ) {
	
				if( $_[ARG0] =~ /250|354/ ) {
					
					if( $_[HEAP]->{step} == 1 ) {
						$_[KERNEL]->yield( "send_from" );
					}
					elsif( $_[HEAP]->{step} == 2 ) {
						$_[KERNEL]->yield( "send_to" );
					}
					elsif( $_[HEAP]->{step} == 3 ) {
						$_[KERNEL]->yield( "send_databegin" );
					}
					elsif( $_[HEAP]->{step} == 4 ) {
						$_[KERNEL]->yield( "send_data" );
					}
					elsif( $_[ARG0] =~ /Message\saccepted\sfor\sdelivery/ && $_[HEAP]->{step} == 5 ) {

						$_[KERNEL]->post( "DBI_SESSION", "set_sended", $_[HEAP]->{curr_mrcpt_id} );
						$_[HEAP]->{step} = -1;

						if( ! $_[HEAP]->{exiting} ) {
							$_[KERNEL]->delay( "get_message", SEND_MMSG_TIMEOUT );
						}
						else {
							$_[KERNEL]->post( $_[SESSION] => "shutdown" );
						}

					}

				}
				elsif( $_[ARG0] !~ /221/ )  {

						$_[KERNEL]->post( "DBI_SESSION", "set_errstr", $_[HEAP]->{curr_mrcpt_id}, $_[HEAP]->{servlog} );	
						$_[KERNEL]->delay( "get_message", SEND_MMSG_TIMEOUT );
				}
			}

		    },
		    InlineStates => 
		      {

  			get_message => sub {

				print "\nSESSION ".$_[SESSION]->ID.": get_message" if DEBUG;

				if( ! $_[HEAP]->{connected} ) {
					$_[KERNEL]->yield( "reconnect" );	
				}

				if( $_[HEAP]->{exiting} ) {
					$_[KERNEL]->post( $_[SESSION] => "shutdown" );
				}
				elsif( $_[HEAP]->{waiting} ) {
					$_[KERNEL]->post( $_[SESSION] => "stay_waiting" );
				}
				else {
	
					$_[HEAP]->{step} = 0;

					$_[HEAP]->{servlog} = "";

					my $postback = $_[SESSION]->postback( "send_message");
	
					$_[KERNEL]->post( "DBI_SESSION", "get_mmsg", $postback );				
				}
			},
		
			send_message => sub {

				print "\nSESSION ".$_[SESSION]->ID.": send message" if DEBUG;
		
				if( ! $_[ARG1]->[0] ) { 
					print "\nSESSION ".$_[SESSION]->ID.": no messages. Sleeping ".NOMMSG_TIMEOUT." seconds..." if DEBUG;	
					$_[KERNEL]->delay( "get_message", NOMMSG_TIMEOUT );
				}
#				elsif( $_[ARG1]->[5] != $_[HEAP]->{last_mrcpt_id} ) {
				else {
					$_[HEAP]->{last_mrcpt_id}	= $_[ARG1]->[5];

					$_[HEAP]->{curr_mmsg_id}	= $_[ARG1]->[0];
					$_[HEAP]->{curr_mmsg_subject}	= $_[ARG1]->[1];
					$_[HEAP]->{curr_mmsg_conttype}	= $_[ARG1]->[2];
					$_[HEAP]->{curr_mmsg_data}	= $_[ARG1]->[3];

					$_[HEAP]->{curr_mlist_id}	= $_[ARG1]->[4];

					$_[HEAP]->{curr_mrcpt_id}	= $_[ARG1]->[5];
					$_[HEAP]->{curr_mrcpt_to}	= $_[ARG1]->[6];	
	
					$_[KERNEL]->yield( "send_rset" );
	
				}
#				else {
#					$_[KERNEL]->yield( "get_message" );
#				}
			},
		
			send_rset => sub {
				$_[HEAP]->{step} = 1;
				$_[HEAP]->{server}->put("RSET");
			},
	
			send_from => sub {
				$_[HEAP]->{step} = 2;
				$_[HEAP]->{server}->put("MAIL FROM: ".$_[HEAP]->{mserver_from});
			},
		
			send_to => sub {
				$_[HEAP]->{step} = 3;
				$_[HEAP]->{server}->put("RCPT TO: ".$_[HEAP]->{curr_mrcpt_to});	
			}, 
	
			send_databegin => sub {
				$_[HEAP]->{step} = 4;	
				$_[HEAP]->{server}->put("DATA");
			},
	
			send_data => sub {
				$_[HEAP]->{step} = 5;
		
				if( $_[HEAP]->{curr_mmsg_conttype} =~ /html/ ) {

					$_[HEAP]->{server}->put(
						"From: ".$_[HEAP]->{mserver_from},
						"To: ".$_[HEAP]->{curr_mrcpt_to},
						"Subject: ".$_[HEAP]->{curr_mmsg_subject},
						"Content-Type: ".$_[HEAP]->{curr_mmsg_conttype}.";",
						"",
						$_[HEAP]->{curr_mmsg_data}, 
						"." 
					);		
				}
				else {
                                        $_[HEAP]->{server}->put(
                                                "From: ".$_[HEAP]->{mserver_from},
                                                "To: ".$_[HEAP]->{curr_mrcpt_to},
                                                "Subject: ".$_[HEAP]->{curr_mmsg_subject},
                                                "",
                                                $_[HEAP]->{curr_mmsg_data},
                                                "."
                                        );

				}
			},


			stay_waiting => sub {
				print "\nSESSION ".$_[SESSION]->ID." waiting..." if DEBUG;

				if( $_[HEAP]->{waiting} ) {
					$_[KERNEL]->delay( "stay_waiting", 10 );
				}
				elsif( $_[HEAP]->{exiting} ) {
					$_[KERNEL]->yield( "shutdown" );
				}

			},
	

			stop_handler => sub {
	
				$_[KERNEL]->sig_handled();

			},

			_stop => sub {
	
				$_[HEAP]->{sess_state} = 0;		
		
				print "\nSESSION ".$_[SESSION]->ID." stopping...";
			

			}


	
	
		      }
		  );

	   }


	}


#	Running the kernel

	$poe_kernel->run;

	exit(0);


#
#       daemonizing
#

sub daemonize {
        if( -f '/var/log/mmaild/.pid' ) {
                die "\nmmaild is working already(PID file exists). Exiting...";
        }
        chdir '/' or die "Can't chdir to /: $!";
        open STDIN, '/dev/null' or die "Can't read /dev/null: $!";
        open STDOUT, ">/var/log/mmaild/log" or die "$!";
        open STDERR, ">/var/log/mmaild/errlog" or die "$!";
        defined( my $pid = fork) or die "Can't fork: $!";
        exit if $pid;
        setsid or die "Can't start a new session: $!";
        umask 0;
        open(PID, '>/var/log/mmaild/.pid');
        print PID $$;
        close PID;
}

sub SIGTERM_handler {


	print "\nSIGTERM recieved. Exiting...";
	print STDERR "\nSIGTERM recieved. Exiting...";

	$poe_kernel->post( "DBI_SESSION" => "check_smtp_sessions" ); 
	$poe_kernel->alias_resolve( "DBI_SESSION" )->get_heap()->{exiting} = 1;

#	unlink '/var/log/mmaild/.pid';
	
	foreach my $mserver_alias ( @mservers_aliases ) {

		if( $poe_kernel->alias_resolve( $mserver_alias )->get_heap()->{step} < 5 ) {
			$poe_kernel->alias_resolve( $mserver_alias )->get_heap()->{sess_state} = 0;
			$poe_kernel->alias_resolve( $mserver_alias )->get_heap()->{server}->put( "QUIT" );
			$poe_kernel->post( $mserver_alias => "shutdown" );
		}

	}


}


