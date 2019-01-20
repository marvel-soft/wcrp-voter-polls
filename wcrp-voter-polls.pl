use strict;
use warnings;
$| = 1;
use File::Basename;
use DBI;
use Data::Dumper;
use Getopt::Long qw(GetOptions);
use Switch;
use Time::Piece;

=head1 Function
=over
=heax		d2 Overview
	This program will analyze a washoe-county-pools file
	Input: county voter polls file.
	       
	Output: a csv file containing the extracted fields 
=cut
my $records;
my $inputFile = "../test-in/2018 4th Free List.csv";    #

#my $inputFile = "../test-in/2018-3rd Free List.csv";#

my $adPrecinctFile = "adall-precincts-jul.csv";

my $fileName       = "";
my $outputFile     = "../test-out/voters-polls.csv";
my @adPrecinctHash = ();
my %adPrecinctHash;
my $adPrecinctHeadings = "";
my $helpReq            = 0;
my $maxLines           = "300000";
my $voteCycle          = "";
my $fileCount          = 1;
my $csvHeadings        = "";
my @csvHeadings;
my $i;
my $line1Read    = '';
my $line2Read    = '';
my $linesRead    = 0;
my $linesWritten = 0;
my %newLine      = ();
my $generalCount;
my $party;
my $primaryCount;
my $pollCount;
my $absenteeCount   = 0;
my $leansRepCount   = 0;
my $leansDemCount   = 0;
my $leanRep         = 0;
my $leanDem         = 0;
my $leans           = "";
my $activeVOTERS    = 0;
my $activeREP       = 0;
my $activeDEM       = 0;
my $activeOTHR      = 0;
my $totalVOTERS     = 0;

#my $csvRowHash;
my @csvRowHash;
my %csvRowHash = ();
my @partyHash;
my %partyHash  = ();
my %schRowHash = ();
my @schRowHash;
my @values1;
my @values2;
my @date;
my $voterRank;
my @voterProfile;
my $voterHeading = "";
my @voterHeading = (
					 "Precinct",     "Voter ID",
					 "Voter Status", "Last Name",
					 "Sfx",          "First",
					 "Middle",       "Birthdate",
					 "Registerdate", "Address",
					 "Zip",          "Phone",
					 "Party",        "Gender",
					 "Military",     "Days Registered",
					 "Generals",     "Primaries",
					 "Polls",        "Absentee",
					 "LeansDEM",     "LeansREP",
					 "Leans",        "Rank"
);
my $precinct = "000000";
my @precinctSummary;
my @newLine;

my $newLine;
#
# main program controller
#
sub main {

	# Parse any parameters
	GetOptions(
				'infile=s'  => \$inputFile,
				'outile=s'  => \$outputFile,
				'lines=s'   => \$maxLines,
				'votecycle' => \$voteCycle,
				'help!'     => \$helpReq,
	) or die "Incorrect usage!\n";
	if ($helpReq) {
		print "Come on, it's really not that hard.\n";
	}
	else {
		print "My inputfile is: $inputFile.\n";
	}
	unless ( open( INPUT, $inputFile ) ) {
		die "Unable to open INPUT: $inputFile Reason: $!\n";
	}

	# pick out the heading line and hold it and remove end character
	$csvHeadings = <INPUT>;
	chomp $csvHeadings;
	chop $csvHeadings;

	# headings in an array to modify
	# @csvHeadings will be used to create the files
	@csvHeadings = split( /\s*,\s*/, $csvHeadings );

	# Build heading for new voter record
	$voterHeading = join( ",", @voterHeading );
	$voterHeading = $voterHeading . "\n";

	#
	# Initialize process loop
	$fileName = basename( $inputFile, ".csv" );
	$outputFile = "precinct-voters-" . $fileName . ".csv";
	print "Voter Profile file: $outputFile\n";
	open( OUTPUT, ">$outputFile" )
	  or die "Unable to open OUTPUT: $outputFile Reason: $!";
	print OUTPUT $voterHeading;

	# initialize the precinct-all table
	adPrecinctAll(@adPrecinctHash);
	$fileName = basename( $inputFile, ".csv" );
	$i = 0;

	# Process loop
	# Read the entire input and
	# 1) edit the input lines
	# 2) transform the data
	# 3) write out transformed line
  NEW:
	while ( $line1Read = <INPUT> ) {
		$linesRead++;
		#
		# Get the data into an array that matches the headers array
		chomp $line1Read;

		# replace commas from in between double quotes with a space
		$line1Read =~ s/(?:\G(?!\A)|[^"]*")[^",]*\K(?:,|"(*SKIP)(*FAIL))/ /g;

		# then create the values array
		@values1 = split( /\s*,\s*/, $line1Read, -1 );

		# Create hash of line for transformation
		@csvRowHash{@csvHeadings} = @values1;

		# determine in precinctSummary needs writing
		if ( $precinct eq "000000" ) {
			$precinct = substr $csvRowHash{"precinct"}, 0, 4 . "00";
		}
		elsif ( $csvRowHash{"precinct"} != $precinct ) {

			# write new precinctSummary
			print "At line: $linesRead - Precinct Summary for: $precinct\n";

			# Create Precinct Summary
			precinctSummary();
			$precinct = substr $csvRowHash{"precinct"}, 0, 4 . "00";
		}

		# Assemble Basic New Voter Line
		%newLine = ();
		$newLine{"Precinct"}     = substr $csvRowHash{"precinct"}, 0, 6;
		$newLine{"Voter Status"} = $csvRowHash{"status"};
		$newLine{"Voter ID"}     = $csvRowHash{"voter_id"};
		$newLine{"Last Name"}    = $csvRowHash{"name_last"};
		$newLine{"Sfx"}          = $csvRowHash{"name_suffix"};
		$newLine{"First"}        = $csvRowHash{"name_first"};
		$newLine{"Middle"}       = $csvRowHash{"name_middle"};
		$newLine{"Gender"}       = "";
		if ( $csvRowHash{"gender"} eq 'M' ) {
			$newLine{"Gender"} = "Male";
		}
		if ( $csvRowHash{"gender"} eq 'F' ) {
			$newLine{"Gender"} = "Female";
		}
		$newLine{"Military"} = "";
		if ( $csvRowHash{"military"} eq 'Y' ) {
			$newLine{"Military"} = "Y";
		}
		$newLine{"Party"} = $csvRowHash{"party"};
		countParty();
		$newLine{"Phone"} = $csvRowHash{"phone_1"};
		@date = split( /\s*\/\s*/, $csvRowHash{"birth_date"}, -1 );
		my $mm = sprintf( "%02d", $date[0] );
		my $dd = sprintf( "%02d", $date[1] );
		my $yy = sprintf( "%02d", $date[2] );
		$newLine{"Birthdate"} = "$mm/$dd/$yy";
		@date = split( /\s*\/\s*/, $csvRowHash{"reg_date"}, -1 );
		$mm = sprintf( "%02d", $date[0] );
		$dd = sprintf( "%02d", $date[1] );
		$yy = sprintf( "%02d", $date[2] );
		$newLine{"Registerdate"} = "$mm/$dd/$yy";
		my $before =
		  Time::Piece->strptime( $newLine{"Registerdate"}, "%m/%d/%y" );
		my $now            = localtime;
		my $daysRegistered = $now - $before;
		$daysRegistered = ( $daysRegistered / ( 1440 * 24 ) );
		$newLine{"Days Registered"} = int($daysRegistered);

		# Assemble Street Address
		$newLine{"Address"} = join( ' ',
									$csvRowHash{house_number},
									$csvRowHash{street}, $csvRowHash{type} );
		$newLine{"Zip"} = $csvRowHash{zip};
		evaluateVoter();
		$newLine{"Primaries"} = $primaryCount;
		$newLine{"Generals"}  = $generalCount;
		$newLine{"Polls"}     = $pollCount;
		$newLine{"Absentee"}  = $absenteeCount;
		$newLine{"LeansREP"}  = $leansRepCount;
		$newLine{"LeansDEM"}  = $leansDemCount;
		$newLine{"LeanREP"}   = $leanRep;
		$newLine{"LeanDEM"}   = $leanDem;
		if ($leanDem) {
			$leans = "DEM";
		}
		if ($leanRep) {
			$leans = "REP";
		}
		$newLine{"Leans"} = $leans;
		$leans            = "";
		$newLine{"Rank"}  = $voterRank;

		# Line processed- write it and go on....
		$i++;
		@voterProfile = ();
		foreach (@voterHeading) {
			push( @voterProfile, $newLine{$_} );
		}
		print OUTPUT join( ',', @voterProfile ), "\n";
		$linesWritten++;
		#
		# For now this is the in-elegant way I detect completion
		if ( eof(INPUT) ) {
			goto EXIT;
		}
		next;
	}
	#
	goto NEW;
}
#
# call main program controller
main();
#
# Common Exit
EXIT:

# write FINAL precinctSummary
precinctSummary();
close(INPUT);
close(OUTPUT);
print " <===> Completed conversion of: $inputFile \n";
print " <===> Output available in file: $outputFile \n";
print " <===> Total Records Read: $linesRead \n";
print " <===> Total Records written: $linesWritten \n";
exit;

sub preparefile {
	print "New output file: $outputFile\n";
	open( OUTPUT, ">$outputFile" )
	  or die "Unable to open OUTPUT: $outputFile Reason: $!";
	print OUTPUT $voterHeading;
}


# calculate percentage
sub percentage {
	my $val = $_;
	return ( sprintf( "%.2f", ( $- * 100 ) ) . "%" . $/ );
}

