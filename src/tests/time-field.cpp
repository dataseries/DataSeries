// -*-C++-*-
/*
   (c) Copyright 2008, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/
#include <limits>

#include <boost/random/mersenne_twister.hpp>

#include <Lintel/Clock.hpp>
#include <Lintel/HashTable.hpp>
#include <DataSeries/Int64TimeField.hpp>

using namespace std;
using boost::format;

static const int64_t min_i64 = numeric_limits<int64_t>::min();
static const int64_t max_i64 = numeric_limits<int64_t>::max();
static const int32_t min_i32 = numeric_limits<int32_t>::min();
static const int32_t max_i32 = numeric_limits<int32_t>::max();
static const uint32_t max_u32 = numeric_limits<uint32_t>::max();
static const uint32_t max_ns = 999999999;
static const int64_t two32 = static_cast<int64_t>(1) << 32;

typedef Int64TimeField::SecNano SecNano;

void check_one_frac32_convert(Int64TimeField &nsec, int64_t ifrac32, 
			      int64_t nsec_conv, int64_t ofrac32)
{
    int64_t a = nsec.frac32ToRaw(ifrac32);
    SINVARIANT(a == nsec_conv);
    int64_t b = nsec.rawToFrac32(a);
    SINVARIANT(b == ofrac32);
}

void check_many_conversion_static_frac32(Int64TimeField &nsec)
{
    // randomly generated using tests/time-field.pl which uses
    // multi-precision arithmetic to get exact results.
    check_one_frac32_convert(nsec, -1232098685620695424LL, -286870330018153280LL, -1232098685620695424LL);
    check_one_frac32_convert(nsec, 2892892729728298792LL, 673554076284239719LL, 2892892729728298793LL);
    check_one_frac32_convert(nsec, 340126467941169133LL, 79191864454459663LL, 340126467941169134LL);
    check_one_frac32_convert(nsec, -8312319374267399515LL, -1935362670167209468LL, -8312319374267399517LL);
    check_one_frac32_convert(nsec, 679360692413972114LL, 158175987287883673LL, 679360692413972113LL);
    check_one_frac32_convert(nsec, -5632872271122305717LL, -1311505276505440873LL, -5632872271122305716LL);
    check_one_frac32_convert(nsec, 7732858514995595345LL, 1800446425330730934LL, 7732858514995595345LL);
    check_one_frac32_convert(nsec, 578524256980605421LL, 134698175122171040LL, 578524256980605421LL);
    check_one_frac32_convert(nsec, -1359272646874840907LL, -316480325272968250LL, -1359272646874840907LL);
    check_one_frac32_convert(nsec, -238933312589025911LL, -55630996960454134LL, -238933312589025911LL);
    check_one_frac32_convert(nsec, 1088037765746079140LL, 253328533318377831LL, 1088037765746079140LL);
    check_one_frac32_convert(nsec, 1378927716064732548LL, 321056627683512063LL, 1378927716064732549LL);
    check_one_frac32_convert(nsec, 8786424457177501965LL, 2045748861780739847LL, 8786424457177501966LL);
    check_one_frac32_convert(nsec, 4763889951220168308LL, 1109179563638793376LL, 4763889951220168307LL);
    check_one_frac32_convert(nsec, -5526379810779288303LL, -1286710568419491943LL, -5526379810779288304LL);
    check_one_frac32_convert(nsec, -4454915084929841813LL, -1037240746647548353LL, -4454915084929841815LL);
    check_one_frac32_convert(nsec, 4614077189704639783LL, 1074298561947569201LL, 4614077189704639785LL);
    check_one_frac32_convert(nsec, -4302548033268547145LL, -1001765027937606709LL, -4302548033268547144LL);
    check_one_frac32_convert(nsec, 4593883118941753754LL, 1069596763453854656LL, 4593883118941753753LL);
    check_one_frac32_convert(nsec, -3282024266901528586LL, -764155822550302508LL, -3282024266901528587LL);
    check_one_frac32_convert(nsec, -5777499814191567139LL, -1345179000448334762LL, -5777499814191567140LL);
    check_one_frac32_convert(nsec, -354040165965951964LL, -82431399721175424LL, -354040165965951965LL);
    check_one_frac32_convert(nsec, 11411328073034916LL, 2656906860190191LL, 11411328073034915LL);
    check_one_frac32_convert(nsec, -1593942658213639182LL, -371118695059241537LL, -1593942658213639184LL);
    check_one_frac32_convert(nsec, -2871281379057680684LL, -668522291597370218LL, -2871281379057680686LL);
    check_one_frac32_convert(nsec, -7247206752302276223LL, -1687371812831209093LL, -7247206752302276223LL);
    check_one_frac32_convert(nsec, -4739037159778834053LL, -1103393072210912139LL, -4739037159778834051LL);
    check_one_frac32_convert(nsec, 4874570518225493942LL, 1134949391294618589LL, 4874570518225493941LL);
    check_one_frac32_convert(nsec, 2370095276711239073LL, 551830808797674038LL, 2370095276711239074LL);
    check_one_frac32_convert(nsec, 4634173757736974194LL, 1078977658817771402LL, 4634173757736974195LL);
    check_one_frac32_convert(nsec, 2574060579472972462LL, 599320181522744815LL, 2574060579472972461LL);
    check_one_frac32_convert(nsec, 4129461274453759134LL, 961465126474797524LL, 4129461274453759134LL);
    check_one_frac32_convert(nsec, 5298455090300335307LL, 1233642709045748996LL, 5298455090300335306LL);
    check_one_frac32_convert(nsec, 7244676037651398850LL, 1686782584910141036LL, 7244676037651398848LL);
    check_one_frac32_convert(nsec, 4350186735731745712LL, 1012856777694948416LL, 4350186735731745711LL);
    check_one_frac32_convert(nsec, -4749253303504229718LL, -1105771703530156454LL, -4749253303504229720LL);
    check_one_frac32_convert(nsec, -8604558193066752434LL, -2003404822448909384LL, -8604558193066752435LL);
    check_one_frac32_convert(nsec, 2656222848470343576LL, 618450075497465109LL, 2656222848470343574LL);
    check_one_frac32_convert(nsec, 3368675412947195652LL, 784330864657460631LL, 3368675412947195653LL);
    check_one_frac32_convert(nsec, -1102835234875115973LL, -256773837580139742LL, -1102835234875115971LL);
    check_one_frac32_convert(nsec, 199544322143480089LL, 46460032962141579LL, 199544322143480088LL);
    check_one_frac32_convert(nsec, 1278076789928811234LL, 297575441638196640LL, 1278076789928811233LL);
    check_one_frac32_convert(nsec, 6243133842904610685LL, 1453592871060737103LL, 6243133842904610687LL);
    check_one_frac32_convert(nsec, 5017735960712519779LL, 1168282693417863869LL, 5017735960712519780LL);
    check_one_frac32_convert(nsec, -6243067892568303009LL, -1453577515801485397LL, -6243067892568303008LL);
    check_one_frac32_convert(nsec, 4650708228846067559LL, 1082827390368577922LL, 4650708228846067561LL);
    check_one_frac32_convert(nsec, -6357013120617660246LL, -1480107456589504202LL, -6357013120617660244LL);
    check_one_frac32_convert(nsec, -1108187648102251869LL, -258020043396915279LL, -1108187648102251871LL);
    check_one_frac32_convert(nsec, -4840734795702803464LL, -1127071398241166832LL, -4840734795702803464LL);
    check_one_frac32_convert(nsec, 7125999214671718970LL, 1659150983828985824LL, 7125999214671718971LL);
    check_one_frac32_convert(nsec, -2324422299999691497LL, -541196740232336218LL, -2324422299999691498LL);
    check_one_frac32_convert(nsec, -1676601495571101855LL, -390364205364860095LL, -1676601495571101856LL);
    check_one_frac32_convert(nsec, -4542116055493607577LL, -1057543804751152074LL, -4542116055493607576LL);
    check_one_frac32_convert(nsec, -3051822214323370645LL, -710557730478088056LL, -3051822214323370645LL);
    check_one_frac32_convert(nsec, 2445504204917566732LL, 569388318089201751LL, 2445504204917566731LL);
    check_one_frac32_convert(nsec, 360098757715864508LL, 83842025537943586LL, 360098757715864509LL);
    check_one_frac32_convert(nsec, -345685621185590959LL, -80486205682528894LL, -345685621185590958LL);
    check_one_frac32_convert(nsec, 3850166090022644756LL, 896436648914274936LL, 3850166090022644758LL);
    check_one_frac32_convert(nsec, 6860470720183248395LL, 1597327813548792246LL, 6860470720183248397LL);
    check_one_frac32_convert(nsec, -6468534677508018366LL, -1506073092461567923LL, -6468534677508018366LL);
    check_one_frac32_convert(nsec, 4148635561019880300LL, 965929487957591261LL, 4148635561019880301LL);
    check_one_frac32_convert(nsec, 210924444064275032LL, 49109674073819777LL, 210924444064275032LL);
    check_one_frac32_convert(nsec, 630780380165109325LL, 146865001918074983LL, 630780380165109323LL);
    check_one_frac32_convert(nsec, -5309414237967741656LL, -1236194334451049021LL, -5309414237967741658LL);
    check_one_frac32_convert(nsec, 688222507814464649LL, 160239289471522125LL, 688222507814464650LL);
    check_one_frac32_convert(nsec, -2924623482283880201LL, -680941967825377407LL, -2924623482283880202LL);
    check_one_frac32_convert(nsec, -4792492106384274840LL, -1115839021835540151LL, -4792492106384274839LL);
    check_one_frac32_convert(nsec, -7993904552031777280LL, -1861225942157157063LL, -7993904552031777278LL);
    check_one_frac32_convert(nsec, 7475565526291014736LL, 1740540733163015623LL, 7475565526291014738LL);
    check_one_frac32_convert(nsec, 5340654446150990658LL, 1243468012230235771LL, 5340654446150990659LL);
    check_one_frac32_convert(nsec, -6470046856418859004LL, -1506425174050698756LL, -6470046856418859003LL);
    check_one_frac32_convert(nsec, -8349648837638330982LL, -1944054113151116991LL, -8349648837638330982LL);
    check_one_frac32_convert(nsec, -6753838082835162040LL, -1572500467960527642LL, -6753838082835162041LL);
    check_one_frac32_convert(nsec, -971895367537622368LL, -226287023987998806LL, -971895367537622368LL);
    check_one_frac32_convert(nsec, -8788434989869902179LL, -2046216975401598536LL, -8788434989869902178LL);
    check_one_frac32_convert(nsec, 1211355564057940184LL, 282040695673306515LL, 1211355564057940182LL);
    check_one_frac32_convert(nsec, -1766357884028564387LL, -411262243061457851LL, -1766357884028564388LL);
    check_one_frac32_convert(nsec, -692053508544050220LL, -161131263837229977LL, -692053508544050218LL);
    check_one_frac32_convert(nsec, -3220894115583625406LL, -749922850072296664LL, -3220894115583625407LL);
    check_one_frac32_convert(nsec, 8846157600296195327LL, 2059656567940533936LL, 8846157600296195328LL);
    check_one_frac32_convert(nsec, 2267183249451902534LL, 527869735251158135LL, 2267183249451902536LL);
    check_one_frac32_convert(nsec, -7801559375623495799LL, -1816442090930299787LL, -7801559375623495801LL);
    check_one_frac32_convert(nsec, -3664121127161091042LL, -853119680462659114LL, -3664121127161091044LL);
    check_one_frac32_convert(nsec, 2302562988457646732LL, 536107222656171474LL, 2302562988457646733LL);
    check_one_frac32_convert(nsec, -7083412429467713966LL, -1649235475218788247LL, -7083412429467713966LL);
    check_one_frac32_convert(nsec, -6088667493270066703LL, -1417628371452462557LL, -6088667493270066701LL);
    check_one_frac32_convert(nsec, -204442389992600827LL, -47600453252112685LL, -204442389992600825LL);
    check_one_frac32_convert(nsec, -5704053898472749632LL, -1328078540617774621LL, -5704053898472749633LL);
    check_one_frac32_convert(nsec, -807654907988614251LL, -188046812077195908LL, -807654907988614252LL);
    check_one_frac32_convert(nsec, 2174054042287947039LL, 506186402004199810LL, 2174054042287947039LL);
    check_one_frac32_convert(nsec, 8767591518256153262LL, 2041363976489788215LL, 8767591518256153261LL);
    check_one_frac32_convert(nsec, 2801040285471357914LL, 652168012566714993LL, 2801040285471357913LL);
    check_one_frac32_convert(nsec, 4753421904330341732LL, 1106742281543636166LL, 4753421904330341730LL);
    check_one_frac32_convert(nsec, 2014038947613123821LL, 468929984516725834LL, 2014038947613123822LL);
    check_one_frac32_convert(nsec, -830954975498183644LL, -193471781792627565LL, -830954975498183646LL);
    check_one_frac32_convert(nsec, -7589904842171202028LL, -1767162429674342747LL, -7589904842171202029LL);
    check_one_frac32_convert(nsec, 5564047662103809991LL, 1295480798488438593LL, 5564047662103809991LL);
    check_one_frac32_convert(nsec, -8981234063110195457LL, -2091106507720005572LL, -8981234063110195457LL);
    check_one_frac32_convert(nsec, -64503218942109702LL, -15018325984037878LL, -64503218942109704LL);
    check_one_frac32_convert(nsec, -1320238532545585299LL, -307391987309228931LL, -1320238532545585298LL);

}

void check_conversion_static()
{
    ExtentSeries s;
    Int64TimeField frac32(s, "");
    Int64TimeField nsec(s, "");

    frac32.setUnitsEpoch("2^-32 seconds", "unix");
    nsec.setUnitsEpoch("nanoseconds", "unix");

    // native -> native conversions
    SINVARIANT(frac32.rawToFrac32(min_i64) == min_i64);
    SINVARIANT(frac32.rawToFrac32(max_i64) == max_i64);
    SINVARIANT(nsec.rawToSecNano(1000*1000*1000) == SecNano(1,0));
    SINVARIANT(nsec.rawToSecNano(50*1000*1000) == SecNano(0,50*1000*1000));
    SINVARIANT(nsec.rawToSecNano(-50*1000*1000) == SecNano(-1,950*1000*1000));

    {
	int64_t a_ns = nsec.secNanoToRaw(max_i32, max_ns);
	int64_t a_frac32 = nsec.rawToFrac32(a_ns);
	int64_t a_ns_2 = nsec.frac32ToRaw(a_frac32);
	SINVARIANT(a_ns == a_ns_2);
    }

    { 
	int64_t b_ns = nsec.secNanoToRaw(min_i32, max_ns);
	int64_t b_frac32 = nsec.rawToFrac32(b_ns);
	int64_t b_ns_2 = nsec.frac32ToRaw(b_frac32);
	SINVARIANT(b_ns == b_ns_2);
    }

    { 
	int64_t c_ns = nsec.secNanoToRaw(min_i32, 0);
	int64_t c_frac32 = nsec.rawToFrac32(c_ns);
	int64_t c_ns_2 = nsec.frac32ToRaw(c_frac32);
	SINVARIANT(c_ns == c_ns_2);
    }

    { // special-a: subtle case found through randomized testing,
      // couldn't precalculate conversion constant in secNanoToFrac32.
	int32_t s = 1208544069;
	uint32_t ns = 460581886;

	int64_t a_ns = nsec.secNanoToRaw(s,ns);
	int64_t a_frac32 = nsec.rawToFrac32(a_ns);
	// calculated used multi-precision time-field.pl
	SINVARIANT(a_frac32 == 5190657254107951562LL);

	int64_t b_ns = nsec.secNanoToRaw(-s-1, 1000*1000*1000-ns);
	SINVARIANT(a_ns == -b_ns);
	int64_t b_frac32 = nsec.rawToFrac32(b_ns);
	SINVARIANT(b_frac32 == -5190657254107951562LL);
	SINVARIANT(a_frac32 == -b_frac32);
    }
    check_many_conversion_static_frac32(nsec);
    cout << "static time checks successful" << endl;
}

void check_conversion_tfrac_micro()
{
}
void check_conversion_tfrac_epoch_unknown_random()
{
    ExtentSeries s;
    Int64TimeField unknown_epoch(s,"");
    Int64TimeField unix_epoch(s,"");
    unknown_epoch.setUnitsEpoch("2^32 seconds", "unknown");
    unix_epoch.setUnitsEpoch("2^32 seconds", "unix");
    SecNano sn;
    sn.seconds = 2543452; // Random palindrome
    sn.nanoseconds = 875424578; // Random palindrome
    //unix.
}

void check_conversion_tfrac_nano_random()
{
}

void check_conversion_random()
{
    ExtentSeries s;
    Int64TimeField frac32(s, "");
    Int64TimeField nsec(s, "");

    frac32.setUnitsEpoch("2^-32 seconds", "unix");
    nsec.setUnitsEpoch("nanoseconds", "unix");

    boost::mt19937 rng;

    uint32_t seed =
	BobJenkinsHashMix3(getpid(), getppid(),
			   BobJenkinsHashMixULL(Clock::todTfrac()));

    cout << format("Randomized testing with seed %d\n") % seed;
    rng.seed(seed);

    SecNano sn;
    const unsigned random_rounds = 10 * 1000 * 1000;
    for(unsigned i = 0; i < random_rounds; ++i) {
	sn.seconds = rng() % max_i32;
	sn.nanoseconds = rng() % (1000*1000*1000);
	
	int64_t a_ns = nsec.secNanoToRaw(sn.seconds, sn.nanoseconds);
	int64_t a_frac32 = nsec.rawToFrac32(a_ns);
	int64_t a_ns_2 = nsec.frac32ToRaw(a_frac32);
	SINVARIANT(a_ns_2 == a_ns);
	SecNano tmp(frac32.rawToSecNano(a_frac32));
	SINVARIANT(tmp == sn);
	
	if (sn.nanoseconds == 0) {
	    sn.seconds = -sn.seconds;
	} else {
	    sn.seconds = -sn.seconds - 1;
	    sn.nanoseconds = 1000*1000*1000 - sn.nanoseconds;
	}
	int64_t b_ns = nsec.secNanoToRaw(sn.seconds, sn.nanoseconds);
	SINVARIANT(b_ns == -a_ns);
	int64_t b_frac32 = nsec.rawToFrac32(b_ns);
	INVARIANT(b_frac32 == -a_frac32,
		  format("(%d,%d) -> %d != -%d")
		  % sn.seconds % sn.nanoseconds % b_frac32 % a_frac32);
	int64_t b_ns_2 = nsec.frac32ToRaw(b_frac32);
	SINVARIANT(b_ns == b_ns_2);
    }

    unsigned negative_count = 0;
    vector<unsigned> diff_count;
    diff_count.resize(5);
    for(unsigned i = 0; i < random_rounds; ++i) {
	uint32_t lower = rng();
	int32_t upper = static_cast<int32_t>(rng());
	if (upper < 0) {
	    ++negative_count;
	}
	int64_t frac32 = static_cast<int64_t>(upper) << 32 | lower;
	int64_t ns = nsec.frac32ToRaw(frac32);
	int64_t frac32_b = nsec.rawToFrac32(ns);
	int64_t diff = frac32_b - frac32;
	// All we know is that when it comes back it should be within +-2 of
	// the original value. because frac32 has just over 4x the precision
	// of ns time.  (Not completely clear we couldn't get a case where
	// we are 3 off)
	INVARIANT(diff >= -2 && diff <= 2, 
		  format("?? %d %d %d %d") % diff % frac32 % ns % frac32_b);
	++diff_count[diff + 2];
    }
    INVARIANT(negative_count > random_rounds/3, "bad rng?");

    // Not clear if there is anything useful we can say about the
    // distribution; observationally with 100m rounds we're getting
    // 15m,23m,23m,23m,15m with a little extra distributed around.

    SINVARIANT(diff_count[0] + diff_count[1] + diff_count[2]
	       + diff_count[3] + diff_count[4] == random_rounds);
    cout << format("frac32->ns->frac32 diffs: %d,%d,%d,%d,%d\n")
	% diff_count[0] % diff_count[1] % diff_count[2] 
	% diff_count[3] % diff_count[4];
    cout << "random time checks successful" << endl;
}

// TODO: add an explicit test for the registerUnitsEpoch code; it's
// currently implicitly tested by running the nfsanalysis test.

int main(int argc, char **argv)
{
    check_conversion_static();
    check_conversion_random();
    //check_conversion_tfrac_nano_random();
    cout << "Time field checks successful" << endl;
}
