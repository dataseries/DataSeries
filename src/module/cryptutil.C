/* -*-C++-*-
   (c) Copyright 2003-2007, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details

   Description:  cryptographic utilities for dataseries
*/

#include <inttypes.h>
#include <cstring>

#include <boost/format.hpp>

#include <openssl/sha.h>
#include <openssl/aes.h>

#include <Lintel/LintelAssert.H>
#include <Lintel/AssertBoost.H>
#include <Lintel/HashMap.H>
#include <Lintel/Clock.H>
#include <Lintel/StringUtil.H>

#include <DataSeries/cryptutil.H>

using namespace std;

HashMap<string,string> encrypted_to_okstring; 

string
shastring(const string &in)
{
    unsigned char sha_out[SHA_DIGEST_LENGTH];
    SHA1((const unsigned char *)in.data(),in.size(),sha_out);
    return string((char *)sha_out,SHA_DIGEST_LENGTH);
}

static string hmac_key_1, hmac_key_2;
static AES_KEY encrypt_key, decrypt_key;

void
prepareEncrypt(const std::string &key_a, const std::string &key_b)
{
    hmac_key_1 = key_a;
    hmac_key_2 = key_b;
    INVARIANT(hmac_key_1.size() >= 16 && hmac_key_2.size() >= 16,
	      boost::format("no %d %d") % hmac_key_1.size() % hmac_key_2.size());
    AES_set_encrypt_key(reinterpret_cast<const unsigned char *>(hmac_key_1.data()),16*8,&encrypt_key);
    AES_set_decrypt_key(reinterpret_cast<const unsigned char *>(hmac_key_1.data()),16*8,&decrypt_key);
}

struct eokent {
    string encrypted;
    string ok;
};

static eokent eoklist[] = {
    // productions
    { "10ba0c97d572b18dcee3d6ba940f0dce", "honey" },
    { "35cdb4301171ed10b699f3a6fac30aaf", "training" },
    { "96cb43a41a53beef1fad90f58bc1e7e0", "molasses" },
    // teams
    { "482b421099353e7bb4dedf70b646d11828bcf6fa58ea55bd80e2d7ed4f181e25", "unaccounted" },

    { "5dd8e2867bfbd16831fcff149cc9eaaba920c3ffc58aa2fbbfb07e3425480a7b", "honey-layout" },
    { "b367b70c18545fffb81c6d2b6997adf3bfc9dbf18348ae8a82d36b328affbdf9", "honey-animation" },
    { "e91c596e5c5d8f2e026622af17552541127d83fd6f4fbeeab1dc7862ed1a3546", "honey-character-development" },
    { "1f6abc86fcc2fc5e3eb8d6436fa2eca68ff876ade9017626552446a440ac6685", "honey-effects" },
    { "ac007998eac68cfa288ad64b864def2d5ecb307ad9fad950279d2eca177b0ed3", "honey-effects-2" },
    { "03f29e35b81d9738bca41d33e72eb11207c49777dab1e61dcef3187647736bd2", "honey-surfacing" },
    { "5332a998856146b49c392563cfd5272d1eb2f3119d594cde0184ee7128fc3b45", "honey-finaling" },
    { "32d53e9fe78dec7bee6284f9bf402026c62aca31290973341388777fbd8cf9f1", "honey-high-priority" },
    { "25fb517d3e2e78c3aaef11c4460a952262ab3a38b96382b6571d9d82ec1ac5ab", "honey-lighting-1" },
    { "63bb7fd62cd7219ab5082ce224c69662afc153d799d9cf514f4d92cac642f674", "honey-lighting-2" },
    { "2e80c9017951b0ffe692b443edfb41e561a6a81994af7820777635471e61719d", "honey-lighting-2a" },
    { "39174e37d9d4fe7cb94e0109d7aea769fb712cba14cc5ff74fc6c52ee0ccc706", "honey-lighting-3" },
    { "66ef44d3224d6f643c7c4cd4329346b77e9885f42a0688caa49f8c4376946451", "honey-lighting-4" },
    { "75036c1cf93bcd0314b4bb58ea61c2caa46b0388a436b0583e5bfdfb3eb5becc", "honey-lighting-5" },
    { "f62c3f757fd13d884df168fc72e7d24755843ad387762d9db85216edd95fb73a", "honey-lighting-6" },
    { "4887743bce51b08944a83fcf2efa9a7ff186ca760fd6fe43f4c377893b661a07", "honey-lighting-7" },
    { "6ea83ba12a18e0ea58d94db20f47e467e847c05f27c35edd28c4242425fb73f8", "honey-consumer-products" },

    { "1cc2aacddfef4b164596bef234a9e028478264124f2088432952064e86b0c196", "molasses-visual-development" },
    { "1d37eb72ac94ec6d815be2305bd22fa7", "molasses-rough-layout" },
    { "486dac507b357e11f0975ce44917bc5b", "molasses-final-layout" },
    { "dc2b39e1b25d86543c669270fa0b49fd", "molasses-animation" },
    { "7c83ce595bc0c99701a2377077c8854e", "molasses-effects" },
    { "0f5e0904d6b7f300a751ff893ca73a66", "molasses-surfacing" },
    { "e2a109508bf3a39ac2704b909ac0342d31905dccb279a28b24e860995ee730b2", "molasses-finaling" },
    { "d3b282264bbca742c4c03b00ff6cc23a7572131a3149090392da8eea41ac4d67", "molasses-lighting-1" },
    { "293eabcc2332b51aee4446b41c8c516acf8ab7c7fcf99fd96d3a2970f484aa1d", "molasses-lighting-2" },
    { "e4f0ea98d659f490bff08b586c2546c3", "molasses-high-priority" },

    { "16ed0b8e13a689839e857a567dfb672f", "rj-motion" },
    { "932c90d17cbe596bb6beb6e2f1933ca9", "rj-light" },

    { "efaf0f701a7b9be409763aa4f10abf914e63002c9a7312a6cd863292c9bdd7bb", "rr-consumerprod" },
    { "1d108cc95d32e24ea95922df1c32416a", "rr-finaling" },
    { "1565725d7f63c6191de072ebe45f44ea", "rr-honey-lighting-7" },
    { "2f98d23c5215f23f122afd0c01a20bee", "rr-surfacing" },
    { "3ffc22710abd6c2c57d4a146e561e1dd", "rr-animation" },
    { "4c05ede1f6721e9600a2f9186b2fb6d5", "rr-honey-lighting-3" },
    { "4f84041753ca3c8ddb77c5a070a54006", "rr-honey-lighting-5" },
    { "61cee6ca72298760deb669e98f6224d1", "rr-molasses-lighting-1" },
    { "6470aa6a658afe75409f0bb230599d17", "rr-honey-lighting-6" },
    { "6dffcb375670432e3e38f0fb25a2054a", "rr-honey-lighting-1" },
    { "7a4be388b2c882d50de56d2229c4ff52", "rr-final-layout" },
    { "a7dc941cba17e45c7f2ddfd1c3649563", "rr-effects" },
    { "ac6a593c6a5dfa816708b7c86b73156d", "rr-cycles" },
    { "b717a07384d6ded47310cc8f1407afaa", "rr-honey-lighting-4" },
    { "c62d49a0a05050023d0480565e9cc5d7", "rr-molasses-lighting-2" },
    { "fc011b138e4cd3632eab5121b3716b0d", "rr-rough-layout" },

    { "9d92ec61b2c82ea2344fc94c84d2799991d173931e1d83183733c9ff032e3055", "java-service" },
    { "b58d7040ef99c5b55c4c233c5a4cba9d97c2de0e3194b66f8f17211a53d52763", "sstress-0.1.0" },
    { "cffd2847abdaac2b44b16935220370f58b5ece0a0db578deedd2a25b8e3e8224", "ers_host_traces" },
    { "b2020dd9561e7df2fd9c57136b1133612b7d0ab577f43ff221cc20f7d6a22722", "batch-parallel" },
    { "467dff5f6c54087341aa73735695cea3", "BPMake" },
    { "f8fb699a41677c34e9523f9b714e5298", "sstress" },
    { "87b2aec5841c83e96797f98c890a0106dbdf9cedf7a55cb20be6704458d68c07", "ers-trace-data" },
};

struct testlist {
    string in, out;
};

static testlist tests[] = {
    { "eric", "ed76debf248fbfc322ae42328a555125" },
    { "the quick brown fox jumps over the lazy dog", "0b4590d4d762ad91cce4144a30c1fbf6b26801121cebbd81484300367db6aebffdfb2442477cb230f4168035cc68e09e6168d660a82ccc75c35778584b4fee40" },
    { "1" , "61933f64b9227ca12c6d4d217c2545d3" },
    { "12" , "6990a16d2324cd8c85af99f1111fbb65" },
    { "123" , "b5ad14513da62197e33c263b70b4605e" },
    { "1234" , "c5fcd3b60049df3275ef4b78215e5e24" },
    { "12345" , "83fe893ea01c6ef28d0c833342a6b573" },
    { "123456" , "e399f7482967b2843225a22051bcb529" },
    { "1234567" , "f4ba85e28e9fcd9f3fa552100e25ac01" },
    { "12345678" , "8f4483c3b55eada20ec50d5ec2295857" },
    { "123456789" , "49a1656523fd8c62842d7c74057267c95d5f5f8cc969f372cfcccda8bec0cfeb" },
    { "123456789a" , "0de6a973de914369453a2079ed7e88995b5e90a989138a9074c9373c7e0b7490" },
    { "123456789ab" , "d0b19e6b94a7ad745654c246d37cfbe14444a148caacd1128162dc0c449cb29a" },
    { "123456789abc" , "cfd3d8a04a83b04d50d3bd1412b14a6c61ef046387a24aca30d799c9b13df1d3" },
    { "123456789abcd" , "f22f98d6b9b3c94091468f364aab025d5cb61a8e9a575e3434a48cf58295ab32" },
    { "123456789abcde" , "d510d62192bc0a1712d6d4623ddbfb8e9d4e1b00ada993a39f89826853cdfda6" },
    { "123456789abcdef" , "1f37c76f92c54d68a617f7e8c3380dff5bda5e4cf4f84a528d0c7c8ea3a01bab" },
    { "0123456789abcdef" , "7d58830ebf0c8a07a7033e03ea06e795023ffa5c3bd8fcdf19a7b73561324e3e" },
    { "", "" } // end of list
};

void
runCryptUtilChecks()
{
    prepareEncrypt("abcdefghijklmnop","0123456789qrstuv");

    for(unsigned i = 0;!tests[i].in.empty(); ++i) {
	string t_out = encryptString(tests[i].in);
	string t_in = decryptString(t_out);
	INVARIANT(hexstring(t_out) == tests[i].out, 
		  boost::format("mismatch %s -> %s != %s") % tests[i].in % hexstring(t_out) % tests[i].out);
	INVARIANT(t_in == tests[i].in, 
		  boost::format("mismatch %s did not decrypt back to original") % tests[i].in);
    }

    string in;
    for(int i = 0;i<4096;++i) {
	string enc = encryptString(in);
	string dec = decryptString(enc);
	AssertAlways(enc != dec && dec == in,("bad"));
	in.append(" ");
    }
    if (false) cout << "CryptUtilChecks passed." << endl;
}

void
prepareEncryptEnvOrRandom(bool random_ok)
{
    runCryptUtilChecks();

    if (getenv("NAME_KEY_1") != NULL && getenv("NAME_KEY_2") != NULL) {
	prepareEncrypt(hex2raw(getenv("NAME_KEY_1")),
		       hex2raw(getenv("NAME_KEY_2")));
    } else {
	INVARIANT(random_ok, "Missing environment variables NAME_KEY_1 and NAME_KEY_2; random choice not ok");
	fprintf(stderr,"Warning; no NAME_KEY_[12] env variables, using random hmac\n");
	FILE *f = fopen("/dev/urandom","r");
	AssertAlways(f != NULL,("bad"));
	const int randombytes = 32;
	char buf[randombytes + 100];
	int i = fread(buf,1,randombytes,f);
	AssertAlways(i == randombytes,("bad %d",i));
	string key_a;
	key_a.assign(buf,randombytes);
	i = fread(buf,1,randombytes,f);
	AssertAlways(i==randombytes,("bad"));
	string key_b;
	key_b.assign(buf,randombytes);
	prepareEncrypt(key_a,key_b);
	fclose(f);
    }
    int neoklist = sizeof(eoklist)/sizeof(eokent);
 
    for(int i = 0;i<neoklist;++i) {
	string f = hex2raw(eoklist[i].encrypted);
	encrypted_to_okstring[f] = eoklist[i].ok;
    }
}

// This is just cipher block chaining implemented inplace without the
// need to pass the all zero's IV and getting to assume multiple of 16
// byte (AES block size) size.

void
aesEncryptFast(AES_KEY *key,unsigned char *buf, int bufsize)
{
    AssertAlways((bufsize % 16) == 0,("bad %d",bufsize));
    uint32_t *v = (uint32_t *)buf;
    uint32_t *vend = v + bufsize/4;
    AES_encrypt((const unsigned char *)v,(unsigned char *)v, key);
    for(v += 4;v < vend;v += 4) {
        v[0] ^= v[-4];
        v[1] ^= v[-3];
        v[2] ^= v[-2];
        v[3] ^= v[-1];
	AES_encrypt((const unsigned char *)v, (unsigned char *)v, key);
    }
}

void
aesDecryptFast(AES_KEY *key,unsigned char *buf, int bufsize)
{
    AssertAlways((bufsize % 16) == 0,("bad"));
    uint32_t *vbegin = (uint32_t *)buf;
    uint32_t *v = vbegin + bufsize/4;
    for(v -= 4; v > vbegin; v -= 4) {
	AES_decrypt((const unsigned char *)v, (unsigned char *)v, key);
	v[0] ^= v[-4];
	v[1] ^= v[-3];
	v[2] ^= v[-2];
	v[3] ^= v[-1];
    }
    AES_decrypt((const unsigned char *)v,(unsigned char *)v, key);
}

static HashMap<string,string> raw2encrypted;

static uint32_t encrypt_memoize_entries = 1000000;

void
encryptMemoizeMaxents(uint32_t nentries) 
{
    encrypt_memoize_entries = nentries;
}

string 
encryptString(string in)
{
    if (encrypt_memoize_entries > 0) {
	string *v = raw2encrypted.lookup(in);
	if (v != NULL) {
	    return *v;
	}
    }

    INVARIANT(hmac_key_1.size() >= 16 && hmac_key_2.size() >= 16,
	      boost::format("no %d %d") % hmac_key_1.size() % hmac_key_2.size());
    // partial HMAC construction
    string tmp = hmac_key_2;
    tmp.append(in);
    unsigned char sha_out[SHA_DIGEST_LENGTH];
    SHA1((const unsigned char *)tmp.data(),tmp.size(),sha_out);
    tmp = " ";
    int minlen = in.size() + 8; // at least 7 bytes of hmac (1 byte of hmac size)
    int totallen = minlen + (16 - (minlen % 16)) % 16;
    int hmaclen = totallen - (in.size() + 1);

    // Worst case is 8 bytes required + 15 bytes roundup -> 23 bytes -
    // 1 of hmacsize = 22 hmac

    AssertAlways(hmaclen >= 7 && hmaclen <= 22,
		 ("should have at least 7-22 bytes of hmac!\n"));
    tmp[0] = hmaclen;
    for(;hmaclen > SHA_DIGEST_LENGTH;--hmaclen) {
	tmp.append(" ");
    }
    tmp.append((char *)sha_out,hmaclen);
    tmp.append(in);
    aesEncryptFast(&encrypt_key,(unsigned char *)&*tmp.begin(),tmp.size());
    if (encrypt_memoize_entries > 0) {
	if (raw2encrypted.size() >= encrypt_memoize_entries) {
	    raw2encrypted.clear();
	}
	raw2encrypted[in] = tmp;
    }
    return tmp;
}

string
decryptString(string in)
{
    INVARIANT(hmac_key_1.size() >= 16 && hmac_key_2.size() >= 16,
	      boost::format("no %d %d") % hmac_key_1.size() % hmac_key_2.size());
    aesDecryptFast(&decrypt_key,(unsigned char *)&*in.begin(),in.size());
    int hmaclen = in[0];
    AssertAlways(hmaclen >= 7 && hmaclen <= 22,("bad decrypt"));
    AssertAlways((int)in.size() >= (hmaclen + 1),("bad decrypt"));
    string ret = in.substr(hmaclen + 1,in.size() - (hmaclen + 1));
    string tmp = hmac_key_2;
    tmp.append(ret);
    unsigned char sha_out[SHA_DIGEST_LENGTH];
    SHA1((unsigned char *)&*tmp.begin(),tmp.size(),sha_out);
    char *cmpto = &in[1];
    for(;hmaclen > SHA_DIGEST_LENGTH;--hmaclen) {
	AssertAlways(*cmpto == ' ',("bad decrypt"));
	++cmpto;
    }
    AssertAlways(memcmp(sha_out,cmpto,hmaclen)==0,("bad decrypt %d",hmaclen));
    return ret;
}
    
// static Clock::Tll encrypt_cycles, hex_cycles, lookup_cycles;
static int hexcount;
static string quote_string("'");
static string null_string("NULL");
static string empty_string("");

string
dsstring(string &instr, bool do_encrypt)
{
    if (instr == empty_string) {
	return empty_string;
    }
    //    Clock::Tll a = Clock::now();
    string encrypt;
    if (do_encrypt) {
	encrypt = encryptString(instr);
    } else {
	encrypt = instr;
    }
    //    Clock::Tll b = Clock::now();
    //    encrypt_cycles += b - a;
    string *ok = encrypted_to_okstring.lookup(encrypt);
    //    Clock::Tll c = Clock::now();
    //    lookup_cycles += c - b;
    if (ok) {
	return *ok;
    } 
    return encrypt;
}

string
sqlstring(string &instr, bool do_encrypt)
{
    if (instr == empty_string) {
	return null_string;
    }
    string ret(quote_string);
    //    Clock::Tll a = Clock::now();
    string encrypt;
    if (do_encrypt) {
	encrypt = encryptString(instr);
	//	Clock::Tll b = Clock::now();
	//	encrypt_cycles += b - a;
	string *ok = encrypted_to_okstring.lookup(encrypt);
	//	Clock::Tll c = Clock::now();
	//	lookup_cycles += c - b;
	if (ok) {
	    ret.append(*ok);
	} else {
	    ++hexcount;
	    ret.append(hexstring(encrypt));
	    //	    Clock::Tll d = Clock::now();
	    //	    hex_cycles += d - c;
	}
    } else {
	ret.append(instr);
    }
    ret.append(quote_string);
    if (false) printf("sqlstringret: %s\n",ret.c_str());
    return ret;
}

void
printEncodeStats()
{
//    Clock::calibrateClock();
//    Clock myclock;
//    fprintf(stderr,"encrypt %.3f; hex(%d) %.3f; lookup %.3f\n",
//	    1.0e-6 * encrypt_cycles * myclock.inverse_clock_rate, 
//	    hexcount,1.0e-6 * hex_cycles * myclock.inverse_clock_rate, 
//	    1.0e-6 * lookup_cycles * myclock.inverse_clock_rate);
}

unsigned
uintfield(const string &str)
{
    for(unsigned i = 0;i<str.size();++i) {
	AssertAlways(isdigit(str[i]),("parse error (not uint) on '%s'",str.c_str()));
    }
    return atoi(str.c_str());
}

int
intfield(const string &str)
{
    if (str.size() > 0 && str[0] == '-') {
	return - uintfield(str.substr(1,str.size()-1));
    } else {
	return uintfield(str);
    }
}

double
dblfield(const string &str)
{
    for(unsigned i = 0;i<str.size();++i) {
	AssertAlways(isdigit(str[i]) || str[i] == '.' || str[i] == '-',("parse error on %s",str.c_str()));
    }
    return atof(str.c_str());
}

#if 0
// Not in use any more, but useful to keep around in case we want to bring
// it back.

void
hexhmac(string &in)
{
    // H-Mac construction from Applied Cryptography:
    // H(K_1, H(K_2,M))
    AssertAlways(hmac_key_1.size() >= 16 && hmac_key_2.size() >= 16,
		 ("no %d %d\n",hmac_key_1.size(),hmac_key_2.size()));
    string tmp = hmac_key_2;
    tmp.append(in);
    unsigned char sha_out[SHA_DIGEST_LENGTH];
    SHA1((const unsigned char *)tmp.data(),tmp.size(),sha_out);
    tmp = hmac_key_1;
    tmp.append((char *)sha_out,SHA_DIGEST_LENGTH);
    SHA1((const unsigned char *)tmp.data(),tmp.size(),sha_out);
    tmp.assign((char *)sha_out,SHA_DIGEST_LENGTH);
    in = hexstring(tmp);
}

#endif

