// $ANTLR 3.5.1 CosmosSQL.g 2013-10-04 21:59:08

package cosmos.sql.parser;


import org.antlr.runtime.*;
import java.util.Stack;
import java.util.List;
import java.util.ArrayList;

@SuppressWarnings("all")
public class CosmosSQLLexer extends Lexer {
	public static final int EOF=-1;
	public static final int ALL_FIELDS=4;
	public static final int AND_SYM=5;
	public static final int ARROW=6;
	public static final int ASTERISK=7;
	public static final int A_=8;
	public static final int BITAND=9;
	public static final int BIT_NUM=10;
	public static final int B_=11;
	public static final int COLON=12;
	public static final int COMMA=13;
	public static final int C_=14;
	public static final int DIVIDE=15;
	public static final int DOT=16;
	public static final int D_=17;
	public static final int EQ_SYM=18;
	public static final int E_=19;
	public static final int F_=20;
	public static final int GET=21;
	public static final int GTH=22;
	public static final int G_=23;
	public static final int HEX_DIGIT=24;
	public static final int HEX_DIGIT_FRAGMENT=25;
	public static final int H_=26;
	public static final int ID=27;
	public static final int INTEGER_NUM=28;
	public static final int I_=29;
	public static final int J_=30;
	public static final int K_=31;
	public static final int LBRACK=32;
	public static final int LET=33;
	public static final int LPAREN=34;
	public static final int LTH=35;
	public static final int L_=36;
	public static final int MINUS=37;
	public static final int MOD_SYM=38;
	public static final int M_=39;
	public static final int NEGATION=40;
	public static final int NOT_EQ=41;
	public static final int N_=42;
	public static final int OR_SYM=43;
	public static final int O_=44;
	public static final int PLUS=45;
	public static final int POWER_OP=46;
	public static final int P_=47;
	public static final int Q_=48;
	public static final int RBRACK=49;
	public static final int REAL_NUMBER=50;
	public static final int RPAREN=51;
	public static final int R_=52;
	public static final int SEMI=53;
	public static final int SET_VAR=54;
	public static final int SHIFT_LEFT=55;
	public static final int SHIFT_RIGHT=56;
	public static final int SHOW=57;
	public static final int S_=58;
	public static final int Space=59;
	public static final int TABLES=60;
	public static final int TEXT_STRING=61;
	public static final int T_=62;
	public static final int U_=63;
	public static final int VERTBAR=64;
	public static final int V_=65;
	public static final int W_=66;
	public static final int X_=67;
	public static final int Y_=68;
	public static final int Z_=69;

		@Override
		public void reportError(RecognitionException e)
		{
			throw new RuntimeException(e);
		}


	// delegates
	// delegators
	public Lexer[] getDelegates() {
		return new Lexer[] {};
	}

	public CosmosSQLLexer() {} 
	public CosmosSQLLexer(CharStream input) {
		this(input, new RecognizerSharedState());
	}
	public CosmosSQLLexer(CharStream input, RecognizerSharedState state) {
		super(input,state);
	}
	@Override public String getGrammarFileName() { return "CosmosSQL.g"; }

	// $ANTLR start "A_"
	public final void mA_() throws RecognitionException {
		try {
			// CosmosSQL.g:47:13: ( 'a' | 'A' )
			// CosmosSQL.g:
			{
			if ( input.LA(1)=='A'||input.LA(1)=='a' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "A_"

	// $ANTLR start "B_"
	public final void mB_() throws RecognitionException {
		try {
			// CosmosSQL.g:48:13: ( 'b' | 'B' )
			// CosmosSQL.g:
			{
			if ( input.LA(1)=='B'||input.LA(1)=='b' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "B_"

	// $ANTLR start "C_"
	public final void mC_() throws RecognitionException {
		try {
			// CosmosSQL.g:49:13: ( 'c' | 'C' )
			// CosmosSQL.g:
			{
			if ( input.LA(1)=='C'||input.LA(1)=='c' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "C_"

	// $ANTLR start "D_"
	public final void mD_() throws RecognitionException {
		try {
			// CosmosSQL.g:50:13: ( 'd' | 'D' )
			// CosmosSQL.g:
			{
			if ( input.LA(1)=='D'||input.LA(1)=='d' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "D_"

	// $ANTLR start "E_"
	public final void mE_() throws RecognitionException {
		try {
			// CosmosSQL.g:51:13: ( 'e' | 'E' )
			// CosmosSQL.g:
			{
			if ( input.LA(1)=='E'||input.LA(1)=='e' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "E_"

	// $ANTLR start "F_"
	public final void mF_() throws RecognitionException {
		try {
			// CosmosSQL.g:52:13: ( 'f' | 'F' )
			// CosmosSQL.g:
			{
			if ( input.LA(1)=='F'||input.LA(1)=='f' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "F_"

	// $ANTLR start "G_"
	public final void mG_() throws RecognitionException {
		try {
			// CosmosSQL.g:53:13: ( 'g' | 'G' )
			// CosmosSQL.g:
			{
			if ( input.LA(1)=='G'||input.LA(1)=='g' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "G_"

	// $ANTLR start "H_"
	public final void mH_() throws RecognitionException {
		try {
			// CosmosSQL.g:54:13: ( 'h' | 'H' )
			// CosmosSQL.g:
			{
			if ( input.LA(1)=='H'||input.LA(1)=='h' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "H_"

	// $ANTLR start "I_"
	public final void mI_() throws RecognitionException {
		try {
			// CosmosSQL.g:55:13: ( 'i' | 'I' )
			// CosmosSQL.g:
			{
			if ( input.LA(1)=='I'||input.LA(1)=='i' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "I_"

	// $ANTLR start "J_"
	public final void mJ_() throws RecognitionException {
		try {
			// CosmosSQL.g:56:13: ( 'j' | 'J' )
			// CosmosSQL.g:
			{
			if ( input.LA(1)=='J'||input.LA(1)=='j' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "J_"

	// $ANTLR start "K_"
	public final void mK_() throws RecognitionException {
		try {
			// CosmosSQL.g:57:13: ( 'k' | 'K' )
			// CosmosSQL.g:
			{
			if ( input.LA(1)=='K'||input.LA(1)=='k' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_"

	// $ANTLR start "L_"
	public final void mL_() throws RecognitionException {
		try {
			// CosmosSQL.g:58:13: ( 'l' | 'L' )
			// CosmosSQL.g:
			{
			if ( input.LA(1)=='L'||input.LA(1)=='l' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "L_"

	// $ANTLR start "M_"
	public final void mM_() throws RecognitionException {
		try {
			// CosmosSQL.g:59:13: ( 'm' | 'M' )
			// CosmosSQL.g:
			{
			if ( input.LA(1)=='M'||input.LA(1)=='m' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "M_"

	// $ANTLR start "N_"
	public final void mN_() throws RecognitionException {
		try {
			// CosmosSQL.g:60:13: ( 'n' | 'N' )
			// CosmosSQL.g:
			{
			if ( input.LA(1)=='N'||input.LA(1)=='n' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "N_"

	// $ANTLR start "O_"
	public final void mO_() throws RecognitionException {
		try {
			// CosmosSQL.g:61:13: ( 'o' | 'O' )
			// CosmosSQL.g:
			{
			if ( input.LA(1)=='O'||input.LA(1)=='o' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "O_"

	// $ANTLR start "P_"
	public final void mP_() throws RecognitionException {
		try {
			// CosmosSQL.g:62:13: ( 'p' | 'P' )
			// CosmosSQL.g:
			{
			if ( input.LA(1)=='P'||input.LA(1)=='p' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "P_"

	// $ANTLR start "Q_"
	public final void mQ_() throws RecognitionException {
		try {
			// CosmosSQL.g:63:13: ( 'q' | 'Q' )
			// CosmosSQL.g:
			{
			if ( input.LA(1)=='Q'||input.LA(1)=='q' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "Q_"

	// $ANTLR start "R_"
	public final void mR_() throws RecognitionException {
		try {
			// CosmosSQL.g:64:13: ( 'r' | 'R' )
			// CosmosSQL.g:
			{
			if ( input.LA(1)=='R'||input.LA(1)=='r' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "R_"

	// $ANTLR start "S_"
	public final void mS_() throws RecognitionException {
		try {
			// CosmosSQL.g:65:13: ( 's' | 'S' )
			// CosmosSQL.g:
			{
			if ( input.LA(1)=='S'||input.LA(1)=='s' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "S_"

	// $ANTLR start "T_"
	public final void mT_() throws RecognitionException {
		try {
			// CosmosSQL.g:66:13: ( 't' | 'T' )
			// CosmosSQL.g:
			{
			if ( input.LA(1)=='T'||input.LA(1)=='t' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "T_"

	// $ANTLR start "U_"
	public final void mU_() throws RecognitionException {
		try {
			// CosmosSQL.g:67:13: ( 'u' | 'U' )
			// CosmosSQL.g:
			{
			if ( input.LA(1)=='U'||input.LA(1)=='u' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "U_"

	// $ANTLR start "V_"
	public final void mV_() throws RecognitionException {
		try {
			// CosmosSQL.g:68:13: ( 'v' | 'V' )
			// CosmosSQL.g:
			{
			if ( input.LA(1)=='V'||input.LA(1)=='v' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "V_"

	// $ANTLR start "W_"
	public final void mW_() throws RecognitionException {
		try {
			// CosmosSQL.g:69:13: ( 'w' | 'W' )
			// CosmosSQL.g:
			{
			if ( input.LA(1)=='W'||input.LA(1)=='w' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "W_"

	// $ANTLR start "X_"
	public final void mX_() throws RecognitionException {
		try {
			// CosmosSQL.g:70:13: ( 'x' | 'X' )
			// CosmosSQL.g:
			{
			if ( input.LA(1)=='X'||input.LA(1)=='x' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "X_"

	// $ANTLR start "Y_"
	public final void mY_() throws RecognitionException {
		try {
			// CosmosSQL.g:71:13: ( 'y' | 'Y' )
			// CosmosSQL.g:
			{
			if ( input.LA(1)=='Y'||input.LA(1)=='y' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "Y_"

	// $ANTLR start "Z_"
	public final void mZ_() throws RecognitionException {
		try {
			// CosmosSQL.g:72:13: ( 'z' | 'Z' )
			// CosmosSQL.g:
			{
			if ( input.LA(1)=='Z'||input.LA(1)=='z' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "Z_"

	// $ANTLR start "SHOW"
	public final void mSHOW() throws RecognitionException {
		try {
			int _type = SHOW;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// CosmosSQL.g:77:9: ( S_ H_ O_ W_ )
			// CosmosSQL.g:77:11: S_ H_ O_ W_
			{
			mS_(); 

			mH_(); 

			mO_(); 

			mW_(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "SHOW"

	// $ANTLR start "TABLES"
	public final void mTABLES() throws RecognitionException {
		try {
			int _type = TABLES;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// CosmosSQL.g:78:11: ( T_ A_ B_ L_ E_ S_ )
			// CosmosSQL.g:78:13: T_ A_ B_ L_ E_ S_
			{
			mT_(); 

			mA_(); 

			mB_(); 

			mL_(); 

			mE_(); 

			mS_(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "TABLES"

	// $ANTLR start "DIVIDE"
	public final void mDIVIDE() throws RecognitionException {
		try {
			int _type = DIVIDE;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// CosmosSQL.g:86:8: ( ( D_ I_ V_ ) | '/' )
			int alt1=2;
			int LA1_0 = input.LA(1);
			if ( (LA1_0=='D'||LA1_0=='d') ) {
				alt1=1;
			}
			else if ( (LA1_0=='/') ) {
				alt1=2;
			}

			else {
				NoViableAltException nvae =
					new NoViableAltException("", 1, 0, input);
				throw nvae;
			}

			switch (alt1) {
				case 1 :
					// CosmosSQL.g:86:10: ( D_ I_ V_ )
					{
					// CosmosSQL.g:86:10: ( D_ I_ V_ )
					// CosmosSQL.g:86:13: D_ I_ V_
					{
					mD_(); 

					mI_(); 

					mV_(); 

					}

					}
					break;
				case 2 :
					// CosmosSQL.g:86:26: '/'
					{
					match('/'); 
					}
					break;

			}
			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "DIVIDE"

	// $ANTLR start "MOD_SYM"
	public final void mMOD_SYM() throws RecognitionException {
		try {
			int _type = MOD_SYM;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// CosmosSQL.g:87:9: ( ( M_ O_ D_ ) | '%' )
			int alt2=2;
			int LA2_0 = input.LA(1);
			if ( (LA2_0=='M'||LA2_0=='m') ) {
				alt2=1;
			}
			else if ( (LA2_0=='%') ) {
				alt2=2;
			}

			else {
				NoViableAltException nvae =
					new NoViableAltException("", 2, 0, input);
				throw nvae;
			}

			switch (alt2) {
				case 1 :
					// CosmosSQL.g:87:11: ( M_ O_ D_ )
					{
					// CosmosSQL.g:87:11: ( M_ O_ D_ )
					// CosmosSQL.g:87:14: M_ O_ D_
					{
					mM_(); 

					mO_(); 

					mD_(); 

					}

					}
					break;
				case 2 :
					// CosmosSQL.g:87:27: '%'
					{
					match('%'); 
					}
					break;

			}
			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "MOD_SYM"

	// $ANTLR start "OR_SYM"
	public final void mOR_SYM() throws RecognitionException {
		try {
			int _type = OR_SYM;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// CosmosSQL.g:88:8: ( ( O_ R_ ) | '||' )
			int alt3=2;
			int LA3_0 = input.LA(1);
			if ( (LA3_0=='O'||LA3_0=='o') ) {
				alt3=1;
			}
			else if ( (LA3_0=='|') ) {
				alt3=2;
			}

			else {
				NoViableAltException nvae =
					new NoViableAltException("", 3, 0, input);
				throw nvae;
			}

			switch (alt3) {
				case 1 :
					// CosmosSQL.g:88:10: ( O_ R_ )
					{
					// CosmosSQL.g:88:10: ( O_ R_ )
					// CosmosSQL.g:88:13: O_ R_
					{
					mO_(); 

					mR_(); 

					}

					}
					break;
				case 2 :
					// CosmosSQL.g:88:23: '||'
					{
					match("||"); 

					}
					break;

			}
			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "OR_SYM"

	// $ANTLR start "AND_SYM"
	public final void mAND_SYM() throws RecognitionException {
		try {
			int _type = AND_SYM;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// CosmosSQL.g:89:9: ( ( A_ N_ D_ ) | '&&' )
			int alt4=2;
			int LA4_0 = input.LA(1);
			if ( (LA4_0=='A'||LA4_0=='a') ) {
				alt4=1;
			}
			else if ( (LA4_0=='&') ) {
				alt4=2;
			}

			else {
				NoViableAltException nvae =
					new NoViableAltException("", 4, 0, input);
				throw nvae;
			}

			switch (alt4) {
				case 1 :
					// CosmosSQL.g:89:11: ( A_ N_ D_ )
					{
					// CosmosSQL.g:89:11: ( A_ N_ D_ )
					// CosmosSQL.g:89:14: A_ N_ D_
					{
					mA_(); 

					mN_(); 

					mD_(); 

					}

					}
					break;
				case 2 :
					// CosmosSQL.g:89:27: '&&'
					{
					match("&&"); 

					}
					break;

			}
			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "AND_SYM"

	// $ANTLR start "ARROW"
	public final void mARROW() throws RecognitionException {
		try {
			int _type = ARROW;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// CosmosSQL.g:91:7: ( '=>' )
			// CosmosSQL.g:91:9: '=>'
			{
			match("=>"); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "ARROW"

	// $ANTLR start "EQ_SYM"
	public final void mEQ_SYM() throws RecognitionException {
		try {
			int _type = EQ_SYM;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// CosmosSQL.g:92:8: ( '=' | '<=>' )
			int alt5=2;
			int LA5_0 = input.LA(1);
			if ( (LA5_0=='=') ) {
				alt5=1;
			}
			else if ( (LA5_0=='<') ) {
				alt5=2;
			}

			else {
				NoViableAltException nvae =
					new NoViableAltException("", 5, 0, input);
				throw nvae;
			}

			switch (alt5) {
				case 1 :
					// CosmosSQL.g:92:10: '='
					{
					match('='); 
					}
					break;
				case 2 :
					// CosmosSQL.g:92:16: '<=>'
					{
					match("<=>"); 

					}
					break;

			}
			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "EQ_SYM"

	// $ANTLR start "NOT_EQ"
	public final void mNOT_EQ() throws RecognitionException {
		try {
			int _type = NOT_EQ;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// CosmosSQL.g:93:8: ( '<>' | '!=' | '~=' | '^=' )
			int alt6=4;
			switch ( input.LA(1) ) {
			case '<':
				{
				alt6=1;
				}
				break;
			case '!':
				{
				alt6=2;
				}
				break;
			case '~':
				{
				alt6=3;
				}
				break;
			case '^':
				{
				alt6=4;
				}
				break;
			default:
				NoViableAltException nvae =
					new NoViableAltException("", 6, 0, input);
				throw nvae;
			}
			switch (alt6) {
				case 1 :
					// CosmosSQL.g:93:10: '<>'
					{
					match("<>"); 

					}
					break;
				case 2 :
					// CosmosSQL.g:93:17: '!='
					{
					match("!="); 

					}
					break;
				case 3 :
					// CosmosSQL.g:93:24: '~='
					{
					match("~="); 

					}
					break;
				case 4 :
					// CosmosSQL.g:93:30: '^='
					{
					match("^="); 

					}
					break;

			}
			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "NOT_EQ"

	// $ANTLR start "LET"
	public final void mLET() throws RecognitionException {
		try {
			int _type = LET;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// CosmosSQL.g:94:5: ( '<=' )
			// CosmosSQL.g:94:7: '<='
			{
			match("<="); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "LET"

	// $ANTLR start "GET"
	public final void mGET() throws RecognitionException {
		try {
			int _type = GET;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// CosmosSQL.g:95:5: ( '>=' )
			// CosmosSQL.g:95:7: '>='
			{
			match(">="); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "GET"

	// $ANTLR start "SET_VAR"
	public final void mSET_VAR() throws RecognitionException {
		try {
			int _type = SET_VAR;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// CosmosSQL.g:96:9: ( ':=' )
			// CosmosSQL.g:96:11: ':='
			{
			match(":="); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "SET_VAR"

	// $ANTLR start "SHIFT_LEFT"
	public final void mSHIFT_LEFT() throws RecognitionException {
		try {
			int _type = SHIFT_LEFT;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// CosmosSQL.g:97:12: ( '<<' )
			// CosmosSQL.g:97:14: '<<'
			{
			match("<<"); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "SHIFT_LEFT"

	// $ANTLR start "SHIFT_RIGHT"
	public final void mSHIFT_RIGHT() throws RecognitionException {
		try {
			int _type = SHIFT_RIGHT;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// CosmosSQL.g:98:13: ( '>>' )
			// CosmosSQL.g:98:15: '>>'
			{
			match(">>"); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "SHIFT_RIGHT"

	// $ANTLR start "ALL_FIELDS"
	public final void mALL_FIELDS() throws RecognitionException {
		try {
			int _type = ALL_FIELDS;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// CosmosSQL.g:99:12: ( '.*' )
			// CosmosSQL.g:99:14: '.*'
			{
			match(".*"); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "ALL_FIELDS"

	// $ANTLR start "SEMI"
	public final void mSEMI() throws RecognitionException {
		try {
			int _type = SEMI;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// CosmosSQL.g:101:6: ( ';' )
			// CosmosSQL.g:101:8: ';'
			{
			match(';'); 
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "SEMI"

	// $ANTLR start "COLON"
	public final void mCOLON() throws RecognitionException {
		try {
			int _type = COLON;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// CosmosSQL.g:102:7: ( ':' )
			// CosmosSQL.g:102:9: ':'
			{
			match(':'); 
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "COLON"

	// $ANTLR start "DOT"
	public final void mDOT() throws RecognitionException {
		try {
			int _type = DOT;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// CosmosSQL.g:103:5: ( '.' )
			// CosmosSQL.g:103:7: '.'
			{
			match('.'); 
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "DOT"

	// $ANTLR start "COMMA"
	public final void mCOMMA() throws RecognitionException {
		try {
			int _type = COMMA;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// CosmosSQL.g:104:7: ( ',' )
			// CosmosSQL.g:104:9: ','
			{
			match(','); 
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "COMMA"

	// $ANTLR start "ASTERISK"
	public final void mASTERISK() throws RecognitionException {
		try {
			int _type = ASTERISK;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// CosmosSQL.g:105:9: ( '*' )
			// CosmosSQL.g:105:11: '*'
			{
			match('*'); 
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "ASTERISK"

	// $ANTLR start "RPAREN"
	public final void mRPAREN() throws RecognitionException {
		try {
			int _type = RPAREN;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// CosmosSQL.g:106:8: ( ')' )
			// CosmosSQL.g:106:10: ')'
			{
			match(')'); 
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "RPAREN"

	// $ANTLR start "LPAREN"
	public final void mLPAREN() throws RecognitionException {
		try {
			int _type = LPAREN;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// CosmosSQL.g:107:8: ( '(' )
			// CosmosSQL.g:107:10: '('
			{
			match('('); 
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "LPAREN"

	// $ANTLR start "RBRACK"
	public final void mRBRACK() throws RecognitionException {
		try {
			int _type = RBRACK;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// CosmosSQL.g:108:8: ( ']' )
			// CosmosSQL.g:108:10: ']'
			{
			match(']'); 
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "RBRACK"

	// $ANTLR start "LBRACK"
	public final void mLBRACK() throws RecognitionException {
		try {
			int _type = LBRACK;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// CosmosSQL.g:109:8: ( '[' )
			// CosmosSQL.g:109:10: '['
			{
			match('['); 
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "LBRACK"

	// $ANTLR start "PLUS"
	public final void mPLUS() throws RecognitionException {
		try {
			int _type = PLUS;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// CosmosSQL.g:110:6: ( '+' )
			// CosmosSQL.g:110:8: '+'
			{
			match('+'); 
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "PLUS"

	// $ANTLR start "MINUS"
	public final void mMINUS() throws RecognitionException {
		try {
			int _type = MINUS;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// CosmosSQL.g:111:7: ( '-' )
			// CosmosSQL.g:111:9: '-'
			{
			match('-'); 
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "MINUS"

	// $ANTLR start "NEGATION"
	public final void mNEGATION() throws RecognitionException {
		try {
			int _type = NEGATION;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// CosmosSQL.g:112:9: ( '~' )
			// CosmosSQL.g:112:11: '~'
			{
			match('~'); 
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "NEGATION"

	// $ANTLR start "VERTBAR"
	public final void mVERTBAR() throws RecognitionException {
		try {
			int _type = VERTBAR;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// CosmosSQL.g:113:9: ( '|' )
			// CosmosSQL.g:113:11: '|'
			{
			match('|'); 
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "VERTBAR"

	// $ANTLR start "BITAND"
	public final void mBITAND() throws RecognitionException {
		try {
			int _type = BITAND;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// CosmosSQL.g:114:8: ( '&' )
			// CosmosSQL.g:114:10: '&'
			{
			match('&'); 
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "BITAND"

	// $ANTLR start "POWER_OP"
	public final void mPOWER_OP() throws RecognitionException {
		try {
			int _type = POWER_OP;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// CosmosSQL.g:115:9: ( '^' )
			// CosmosSQL.g:115:11: '^'
			{
			match('^'); 
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "POWER_OP"

	// $ANTLR start "GTH"
	public final void mGTH() throws RecognitionException {
		try {
			int _type = GTH;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// CosmosSQL.g:116:5: ( '>' )
			// CosmosSQL.g:116:7: '>'
			{
			match('>'); 
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "GTH"

	// $ANTLR start "LTH"
	public final void mLTH() throws RecognitionException {
		try {
			int _type = LTH;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// CosmosSQL.g:117:5: ( '<' )
			// CosmosSQL.g:117:7: '<'
			{
			match('<'); 
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "LTH"

	// $ANTLR start "INTEGER_NUM"
	public final void mINTEGER_NUM() throws RecognitionException {
		try {
			int _type = INTEGER_NUM;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// CosmosSQL.g:121:14: ( ( '0' .. '9' )+ )
			// CosmosSQL.g:121:16: ( '0' .. '9' )+
			{
			// CosmosSQL.g:121:16: ( '0' .. '9' )+
			int cnt7=0;
			loop7:
			while (true) {
				int alt7=2;
				int LA7_0 = input.LA(1);
				if ( ((LA7_0 >= '0' && LA7_0 <= '9')) ) {
					alt7=1;
				}

				switch (alt7) {
				case 1 :
					// CosmosSQL.g:
					{
					if ( (input.LA(1) >= '0' && input.LA(1) <= '9') ) {
						input.consume();
					}
					else {
						MismatchedSetException mse = new MismatchedSetException(null,input);
						recover(mse);
						throw mse;
					}
					}
					break;

				default :
					if ( cnt7 >= 1 ) break loop7;
					EarlyExitException eee = new EarlyExitException(7, input);
					throw eee;
				}
				cnt7++;
			}

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "INTEGER_NUM"

	// $ANTLR start "HEX_DIGIT_FRAGMENT"
	public final void mHEX_DIGIT_FRAGMENT() throws RecognitionException {
		try {
			// CosmosSQL.g:123:28: ( ( 'a' .. 'f' | 'A' .. 'F' | '0' .. '9' ) )
			// CosmosSQL.g:
			{
			if ( (input.LA(1) >= '0' && input.LA(1) <= '9')||(input.LA(1) >= 'A' && input.LA(1) <= 'F')||(input.LA(1) >= 'a' && input.LA(1) <= 'f') ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "HEX_DIGIT_FRAGMENT"

	// $ANTLR start "HEX_DIGIT"
	public final void mHEX_DIGIT() throws RecognitionException {
		try {
			int _type = HEX_DIGIT;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// CosmosSQL.g:124:10: ( ( '0x' ( HEX_DIGIT_FRAGMENT )+ ) | ( 'X' '\\'' ( HEX_DIGIT_FRAGMENT )+ '\\'' ) )
			int alt10=2;
			int LA10_0 = input.LA(1);
			if ( (LA10_0=='0') ) {
				alt10=1;
			}
			else if ( (LA10_0=='X') ) {
				alt10=2;
			}

			else {
				NoViableAltException nvae =
					new NoViableAltException("", 10, 0, input);
				throw nvae;
			}

			switch (alt10) {
				case 1 :
					// CosmosSQL.g:125:2: ( '0x' ( HEX_DIGIT_FRAGMENT )+ )
					{
					// CosmosSQL.g:125:2: ( '0x' ( HEX_DIGIT_FRAGMENT )+ )
					// CosmosSQL.g:125:5: '0x' ( HEX_DIGIT_FRAGMENT )+
					{
					match("0x"); 

					// CosmosSQL.g:125:14: ( HEX_DIGIT_FRAGMENT )+
					int cnt8=0;
					loop8:
					while (true) {
						int alt8=2;
						int LA8_0 = input.LA(1);
						if ( ((LA8_0 >= '0' && LA8_0 <= '9')||(LA8_0 >= 'A' && LA8_0 <= 'F')||(LA8_0 >= 'a' && LA8_0 <= 'f')) ) {
							alt8=1;
						}

						switch (alt8) {
						case 1 :
							// CosmosSQL.g:
							{
							if ( (input.LA(1) >= '0' && input.LA(1) <= '9')||(input.LA(1) >= 'A' && input.LA(1) <= 'F')||(input.LA(1) >= 'a' && input.LA(1) <= 'f') ) {
								input.consume();
							}
							else {
								MismatchedSetException mse = new MismatchedSetException(null,input);
								recover(mse);
								throw mse;
							}
							}
							break;

						default :
							if ( cnt8 >= 1 ) break loop8;
							EarlyExitException eee = new EarlyExitException(8, input);
							throw eee;
						}
						cnt8++;
					}

					}

					}
					break;
				case 2 :
					// CosmosSQL.g:127:2: ( 'X' '\\'' ( HEX_DIGIT_FRAGMENT )+ '\\'' )
					{
					// CosmosSQL.g:127:2: ( 'X' '\\'' ( HEX_DIGIT_FRAGMENT )+ '\\'' )
					// CosmosSQL.g:127:5: 'X' '\\'' ( HEX_DIGIT_FRAGMENT )+ '\\''
					{
					match('X'); 
					match('\''); 
					// CosmosSQL.g:127:14: ( HEX_DIGIT_FRAGMENT )+
					int cnt9=0;
					loop9:
					while (true) {
						int alt9=2;
						int LA9_0 = input.LA(1);
						if ( ((LA9_0 >= '0' && LA9_0 <= '9')||(LA9_0 >= 'A' && LA9_0 <= 'F')||(LA9_0 >= 'a' && LA9_0 <= 'f')) ) {
							alt9=1;
						}

						switch (alt9) {
						case 1 :
							// CosmosSQL.g:
							{
							if ( (input.LA(1) >= '0' && input.LA(1) <= '9')||(input.LA(1) >= 'A' && input.LA(1) <= 'F')||(input.LA(1) >= 'a' && input.LA(1) <= 'f') ) {
								input.consume();
							}
							else {
								MismatchedSetException mse = new MismatchedSetException(null,input);
								recover(mse);
								throw mse;
							}
							}
							break;

						default :
							if ( cnt9 >= 1 ) break loop9;
							EarlyExitException eee = new EarlyExitException(9, input);
							throw eee;
						}
						cnt9++;
					}

					match('\''); 
					}

					}
					break;

			}
			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "HEX_DIGIT"

	// $ANTLR start "BIT_NUM"
	public final void mBIT_NUM() throws RecognitionException {
		try {
			int _type = BIT_NUM;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// CosmosSQL.g:130:8: ( ( '0b' ( '0' | '1' )+ ) | ( B_ '\\'' ( '0' | '1' )+ '\\'' ) )
			int alt13=2;
			int LA13_0 = input.LA(1);
			if ( (LA13_0=='0') ) {
				alt13=1;
			}
			else if ( (LA13_0=='B'||LA13_0=='b') ) {
				alt13=2;
			}

			else {
				NoViableAltException nvae =
					new NoViableAltException("", 13, 0, input);
				throw nvae;
			}

			switch (alt13) {
				case 1 :
					// CosmosSQL.g:131:2: ( '0b' ( '0' | '1' )+ )
					{
					// CosmosSQL.g:131:2: ( '0b' ( '0' | '1' )+ )
					// CosmosSQL.g:131:5: '0b' ( '0' | '1' )+
					{
					match("0b"); 

					// CosmosSQL.g:131:13: ( '0' | '1' )+
					int cnt11=0;
					loop11:
					while (true) {
						int alt11=2;
						int LA11_0 = input.LA(1);
						if ( ((LA11_0 >= '0' && LA11_0 <= '1')) ) {
							alt11=1;
						}

						switch (alt11) {
						case 1 :
							// CosmosSQL.g:
							{
							if ( (input.LA(1) >= '0' && input.LA(1) <= '1') ) {
								input.consume();
							}
							else {
								MismatchedSetException mse = new MismatchedSetException(null,input);
								recover(mse);
								throw mse;
							}
							}
							break;

						default :
							if ( cnt11 >= 1 ) break loop11;
							EarlyExitException eee = new EarlyExitException(11, input);
							throw eee;
						}
						cnt11++;
					}

					}

					}
					break;
				case 2 :
					// CosmosSQL.g:133:2: ( B_ '\\'' ( '0' | '1' )+ '\\'' )
					{
					// CosmosSQL.g:133:2: ( B_ '\\'' ( '0' | '1' )+ '\\'' )
					// CosmosSQL.g:133:5: B_ '\\'' ( '0' | '1' )+ '\\''
					{
					mB_(); 

					match('\''); 
					// CosmosSQL.g:133:13: ( '0' | '1' )+
					int cnt12=0;
					loop12:
					while (true) {
						int alt12=2;
						int LA12_0 = input.LA(1);
						if ( ((LA12_0 >= '0' && LA12_0 <= '1')) ) {
							alt12=1;
						}

						switch (alt12) {
						case 1 :
							// CosmosSQL.g:
							{
							if ( (input.LA(1) >= '0' && input.LA(1) <= '1') ) {
								input.consume();
							}
							else {
								MismatchedSetException mse = new MismatchedSetException(null,input);
								recover(mse);
								throw mse;
							}
							}
							break;

						default :
							if ( cnt12 >= 1 ) break loop12;
							EarlyExitException eee = new EarlyExitException(12, input);
							throw eee;
						}
						cnt12++;
					}

					match('\''); 
					}

					}
					break;

			}
			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "BIT_NUM"

	// $ANTLR start "REAL_NUMBER"
	public final void mREAL_NUMBER() throws RecognitionException {
		try {
			int _type = REAL_NUMBER;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// CosmosSQL.g:136:12: ( ( INTEGER_NUM DOT INTEGER_NUM | INTEGER_NUM DOT | DOT INTEGER_NUM | INTEGER_NUM ) ( ( 'E' | 'e' ) ( PLUS | MINUS )? INTEGER_NUM )? )
			// CosmosSQL.g:137:2: ( INTEGER_NUM DOT INTEGER_NUM | INTEGER_NUM DOT | DOT INTEGER_NUM | INTEGER_NUM ) ( ( 'E' | 'e' ) ( PLUS | MINUS )? INTEGER_NUM )?
			{
			// CosmosSQL.g:137:2: ( INTEGER_NUM DOT INTEGER_NUM | INTEGER_NUM DOT | DOT INTEGER_NUM | INTEGER_NUM )
			int alt14=4;
			alt14 = dfa14.predict(input);
			switch (alt14) {
				case 1 :
					// CosmosSQL.g:137:5: INTEGER_NUM DOT INTEGER_NUM
					{
					mINTEGER_NUM(); 

					mDOT(); 

					mINTEGER_NUM(); 

					}
					break;
				case 2 :
					// CosmosSQL.g:137:35: INTEGER_NUM DOT
					{
					mINTEGER_NUM(); 

					mDOT(); 

					}
					break;
				case 3 :
					// CosmosSQL.g:137:53: DOT INTEGER_NUM
					{
					mDOT(); 

					mINTEGER_NUM(); 

					}
					break;
				case 4 :
					// CosmosSQL.g:137:71: INTEGER_NUM
					{
					mINTEGER_NUM(); 

					}
					break;

			}

			// CosmosSQL.g:138:2: ( ( 'E' | 'e' ) ( PLUS | MINUS )? INTEGER_NUM )?
			int alt16=2;
			int LA16_0 = input.LA(1);
			if ( (LA16_0=='E'||LA16_0=='e') ) {
				alt16=1;
			}
			switch (alt16) {
				case 1 :
					// CosmosSQL.g:138:5: ( 'E' | 'e' ) ( PLUS | MINUS )? INTEGER_NUM
					{
					if ( input.LA(1)=='E'||input.LA(1)=='e' ) {
						input.consume();
					}
					else {
						MismatchedSetException mse = new MismatchedSetException(null,input);
						recover(mse);
						throw mse;
					}
					// CosmosSQL.g:138:15: ( PLUS | MINUS )?
					int alt15=2;
					int LA15_0 = input.LA(1);
					if ( (LA15_0=='+'||LA15_0=='-') ) {
						alt15=1;
					}
					switch (alt15) {
						case 1 :
							// CosmosSQL.g:
							{
							if ( input.LA(1)=='+'||input.LA(1)=='-' ) {
								input.consume();
							}
							else {
								MismatchedSetException mse = new MismatchedSetException(null,input);
								recover(mse);
								throw mse;
							}
							}
							break;

					}

					mINTEGER_NUM(); 

					}
					break;

			}

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "REAL_NUMBER"

	// $ANTLR start "TEXT_STRING"
	public final void mTEXT_STRING() throws RecognitionException {
		try {
			int _type = TEXT_STRING;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// CosmosSQL.g:141:12: ( ( N_ | ( '_' U_ T_ F_ '8' ) )? ( ( '\\'' ( ( '\\\\' '\\\\' ) | ( '\\'' '\\'' ) | ( '\\\\' '\\'' ) |~ ( '\\'' ) )* '\\'' ) | ( '\\\"' ( ( '\\\\' '\\\\' ) | ( '\\\"' '\\\"' ) | ( '\\\\' '\\\"' ) |~ ( '\\\"' ) )* '\\\"' ) ) )
			// CosmosSQL.g:142:2: ( N_ | ( '_' U_ T_ F_ '8' ) )? ( ( '\\'' ( ( '\\\\' '\\\\' ) | ( '\\'' '\\'' ) | ( '\\\\' '\\'' ) |~ ( '\\'' ) )* '\\'' ) | ( '\\\"' ( ( '\\\\' '\\\\' ) | ( '\\\"' '\\\"' ) | ( '\\\\' '\\\"' ) |~ ( '\\\"' ) )* '\\\"' ) )
			{
			// CosmosSQL.g:142:2: ( N_ | ( '_' U_ T_ F_ '8' ) )?
			int alt17=3;
			int LA17_0 = input.LA(1);
			if ( (LA17_0=='N'||LA17_0=='n') ) {
				alt17=1;
			}
			else if ( (LA17_0=='_') ) {
				alt17=2;
			}
			switch (alt17) {
				case 1 :
					// CosmosSQL.g:142:4: N_
					{
					mN_(); 

					}
					break;
				case 2 :
					// CosmosSQL.g:142:9: ( '_' U_ T_ F_ '8' )
					{
					// CosmosSQL.g:142:9: ( '_' U_ T_ F_ '8' )
					// CosmosSQL.g:142:10: '_' U_ T_ F_ '8'
					{
					match('_'); 
					mU_(); 

					mT_(); 

					mF_(); 

					match('8'); 
					}

					}
					break;

			}

			// CosmosSQL.g:143:2: ( ( '\\'' ( ( '\\\\' '\\\\' ) | ( '\\'' '\\'' ) | ( '\\\\' '\\'' ) |~ ( '\\'' ) )* '\\'' ) | ( '\\\"' ( ( '\\\\' '\\\\' ) | ( '\\\"' '\\\"' ) | ( '\\\\' '\\\"' ) |~ ( '\\\"' ) )* '\\\"' ) )
			int alt20=2;
			int LA20_0 = input.LA(1);
			if ( (LA20_0=='\'') ) {
				alt20=1;
			}
			else if ( (LA20_0=='\"') ) {
				alt20=2;
			}

			else {
				NoViableAltException nvae =
					new NoViableAltException("", 20, 0, input);
				throw nvae;
			}

			switch (alt20) {
				case 1 :
					// CosmosSQL.g:144:3: ( '\\'' ( ( '\\\\' '\\\\' ) | ( '\\'' '\\'' ) | ( '\\\\' '\\'' ) |~ ( '\\'' ) )* '\\'' )
					{
					// CosmosSQL.g:144:3: ( '\\'' ( ( '\\\\' '\\\\' ) | ( '\\'' '\\'' ) | ( '\\\\' '\\'' ) |~ ( '\\'' ) )* '\\'' )
					// CosmosSQL.g:144:6: '\\'' ( ( '\\\\' '\\\\' ) | ( '\\'' '\\'' ) | ( '\\\\' '\\'' ) |~ ( '\\'' ) )* '\\''
					{
					match('\''); 
					// CosmosSQL.g:144:11: ( ( '\\\\' '\\\\' ) | ( '\\'' '\\'' ) | ( '\\\\' '\\'' ) |~ ( '\\'' ) )*
					loop18:
					while (true) {
						int alt18=5;
						alt18 = dfa18.predict(input);
						switch (alt18) {
						case 1 :
							// CosmosSQL.g:144:13: ( '\\\\' '\\\\' )
							{
							// CosmosSQL.g:144:13: ( '\\\\' '\\\\' )
							// CosmosSQL.g:144:14: '\\\\' '\\\\'
							{
							match('\\'); 
							match('\\'); 
							}

							}
							break;
						case 2 :
							// CosmosSQL.g:144:27: ( '\\'' '\\'' )
							{
							// CosmosSQL.g:144:27: ( '\\'' '\\'' )
							// CosmosSQL.g:144:28: '\\'' '\\''
							{
							match('\''); 
							match('\''); 
							}

							}
							break;
						case 3 :
							// CosmosSQL.g:144:41: ( '\\\\' '\\'' )
							{
							// CosmosSQL.g:144:41: ( '\\\\' '\\'' )
							// CosmosSQL.g:144:42: '\\\\' '\\''
							{
							match('\\'); 
							match('\''); 
							}

							}
							break;
						case 4 :
							// CosmosSQL.g:144:55: ~ ( '\\'' )
							{
							if ( (input.LA(1) >= '\u0000' && input.LA(1) <= '&')||(input.LA(1) >= '(' && input.LA(1) <= '\uFFFF') ) {
								input.consume();
							}
							else {
								MismatchedSetException mse = new MismatchedSetException(null,input);
								recover(mse);
								throw mse;
							}
							}
							break;

						default :
							break loop18;
						}
					}

					match('\''); 
					}

					}
					break;
				case 2 :
					// CosmosSQL.g:146:3: ( '\\\"' ( ( '\\\\' '\\\\' ) | ( '\\\"' '\\\"' ) | ( '\\\\' '\\\"' ) |~ ( '\\\"' ) )* '\\\"' )
					{
					// CosmosSQL.g:146:3: ( '\\\"' ( ( '\\\\' '\\\\' ) | ( '\\\"' '\\\"' ) | ( '\\\\' '\\\"' ) |~ ( '\\\"' ) )* '\\\"' )
					// CosmosSQL.g:146:6: '\\\"' ( ( '\\\\' '\\\\' ) | ( '\\\"' '\\\"' ) | ( '\\\\' '\\\"' ) |~ ( '\\\"' ) )* '\\\"'
					{
					match('\"'); 
					// CosmosSQL.g:146:11: ( ( '\\\\' '\\\\' ) | ( '\\\"' '\\\"' ) | ( '\\\\' '\\\"' ) |~ ( '\\\"' ) )*
					loop19:
					while (true) {
						int alt19=5;
						alt19 = dfa19.predict(input);
						switch (alt19) {
						case 1 :
							// CosmosSQL.g:146:13: ( '\\\\' '\\\\' )
							{
							// CosmosSQL.g:146:13: ( '\\\\' '\\\\' )
							// CosmosSQL.g:146:14: '\\\\' '\\\\'
							{
							match('\\'); 
							match('\\'); 
							}

							}
							break;
						case 2 :
							// CosmosSQL.g:146:27: ( '\\\"' '\\\"' )
							{
							// CosmosSQL.g:146:27: ( '\\\"' '\\\"' )
							// CosmosSQL.g:146:28: '\\\"' '\\\"'
							{
							match('\"'); 
							match('\"'); 
							}

							}
							break;
						case 3 :
							// CosmosSQL.g:146:41: ( '\\\\' '\\\"' )
							{
							// CosmosSQL.g:146:41: ( '\\\\' '\\\"' )
							// CosmosSQL.g:146:42: '\\\\' '\\\"'
							{
							match('\\'); 
							match('\"'); 
							}

							}
							break;
						case 4 :
							// CosmosSQL.g:146:55: ~ ( '\\\"' )
							{
							if ( (input.LA(1) >= '\u0000' && input.LA(1) <= '!')||(input.LA(1) >= '#' && input.LA(1) <= '\uFFFF') ) {
								input.consume();
							}
							else {
								MismatchedSetException mse = new MismatchedSetException(null,input);
								recover(mse);
								throw mse;
							}
							}
							break;

						default :
							break loop19;
						}
					}

					match('\"'); 
					}

					}
					break;

			}

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "TEXT_STRING"

	// $ANTLR start "ID"
	public final void mID() throws RecognitionException {
		try {
			int _type = ID;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// CosmosSQL.g:150:3: ( ( 'A' .. 'Z' | 'a' .. 'z' | '_' | '$' ) ( 'A' .. 'Z' | 'a' .. 'z' | '_' | '$' | '0' .. '9' )* )
			// CosmosSQL.g:151:2: ( 'A' .. 'Z' | 'a' .. 'z' | '_' | '$' ) ( 'A' .. 'Z' | 'a' .. 'z' | '_' | '$' | '0' .. '9' )*
			{
			if ( input.LA(1)=='$'||(input.LA(1) >= 'A' && input.LA(1) <= 'Z')||input.LA(1)=='_'||(input.LA(1) >= 'a' && input.LA(1) <= 'z') ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			// CosmosSQL.g:151:37: ( 'A' .. 'Z' | 'a' .. 'z' | '_' | '$' | '0' .. '9' )*
			loop21:
			while (true) {
				int alt21=2;
				int LA21_0 = input.LA(1);
				if ( (LA21_0=='$'||(LA21_0 >= '0' && LA21_0 <= '9')||(LA21_0 >= 'A' && LA21_0 <= 'Z')||LA21_0=='_'||(LA21_0 >= 'a' && LA21_0 <= 'z')) ) {
					alt21=1;
				}

				switch (alt21) {
				case 1 :
					// CosmosSQL.g:
					{
					if ( input.LA(1)=='$'||(input.LA(1) >= '0' && input.LA(1) <= '9')||(input.LA(1) >= 'A' && input.LA(1) <= 'Z')||input.LA(1)=='_'||(input.LA(1) >= 'a' && input.LA(1) <= 'z') ) {
						input.consume();
					}
					else {
						MismatchedSetException mse = new MismatchedSetException(null,input);
						recover(mse);
						throw mse;
					}
					}
					break;

				default :
					break loop21;
				}
			}

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "ID"

	// $ANTLR start "Space"
	public final void mSpace() throws RecognitionException {
		try {
			int _type = Space;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// CosmosSQL.g:166:7: ( ' ' )
			// CosmosSQL.g:166:10: ' '
			{
			match(' '); 
			skip();
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "Space"

	@Override
	public void mTokens() throws RecognitionException {
		// CosmosSQL.g:1:8: ( SHOW | TABLES | DIVIDE | MOD_SYM | OR_SYM | AND_SYM | ARROW | EQ_SYM | NOT_EQ | LET | GET | SET_VAR | SHIFT_LEFT | SHIFT_RIGHT | ALL_FIELDS | SEMI | COLON | DOT | COMMA | ASTERISK | RPAREN | LPAREN | RBRACK | LBRACK | PLUS | MINUS | NEGATION | VERTBAR | BITAND | POWER_OP | GTH | LTH | INTEGER_NUM | HEX_DIGIT | BIT_NUM | REAL_NUMBER | TEXT_STRING | ID | Space )
		int alt22=39;
		alt22 = dfa22.predict(input);
		switch (alt22) {
			case 1 :
				// CosmosSQL.g:1:10: SHOW
				{
				mSHOW(); 

				}
				break;
			case 2 :
				// CosmosSQL.g:1:15: TABLES
				{
				mTABLES(); 

				}
				break;
			case 3 :
				// CosmosSQL.g:1:22: DIVIDE
				{
				mDIVIDE(); 

				}
				break;
			case 4 :
				// CosmosSQL.g:1:29: MOD_SYM
				{
				mMOD_SYM(); 

				}
				break;
			case 5 :
				// CosmosSQL.g:1:37: OR_SYM
				{
				mOR_SYM(); 

				}
				break;
			case 6 :
				// CosmosSQL.g:1:44: AND_SYM
				{
				mAND_SYM(); 

				}
				break;
			case 7 :
				// CosmosSQL.g:1:52: ARROW
				{
				mARROW(); 

				}
				break;
			case 8 :
				// CosmosSQL.g:1:58: EQ_SYM
				{
				mEQ_SYM(); 

				}
				break;
			case 9 :
				// CosmosSQL.g:1:65: NOT_EQ
				{
				mNOT_EQ(); 

				}
				break;
			case 10 :
				// CosmosSQL.g:1:72: LET
				{
				mLET(); 

				}
				break;
			case 11 :
				// CosmosSQL.g:1:76: GET
				{
				mGET(); 

				}
				break;
			case 12 :
				// CosmosSQL.g:1:80: SET_VAR
				{
				mSET_VAR(); 

				}
				break;
			case 13 :
				// CosmosSQL.g:1:88: SHIFT_LEFT
				{
				mSHIFT_LEFT(); 

				}
				break;
			case 14 :
				// CosmosSQL.g:1:99: SHIFT_RIGHT
				{
				mSHIFT_RIGHT(); 

				}
				break;
			case 15 :
				// CosmosSQL.g:1:111: ALL_FIELDS
				{
				mALL_FIELDS(); 

				}
				break;
			case 16 :
				// CosmosSQL.g:1:122: SEMI
				{
				mSEMI(); 

				}
				break;
			case 17 :
				// CosmosSQL.g:1:127: COLON
				{
				mCOLON(); 

				}
				break;
			case 18 :
				// CosmosSQL.g:1:133: DOT
				{
				mDOT(); 

				}
				break;
			case 19 :
				// CosmosSQL.g:1:137: COMMA
				{
				mCOMMA(); 

				}
				break;
			case 20 :
				// CosmosSQL.g:1:143: ASTERISK
				{
				mASTERISK(); 

				}
				break;
			case 21 :
				// CosmosSQL.g:1:152: RPAREN
				{
				mRPAREN(); 

				}
				break;
			case 22 :
				// CosmosSQL.g:1:159: LPAREN
				{
				mLPAREN(); 

				}
				break;
			case 23 :
				// CosmosSQL.g:1:166: RBRACK
				{
				mRBRACK(); 

				}
				break;
			case 24 :
				// CosmosSQL.g:1:173: LBRACK
				{
				mLBRACK(); 

				}
				break;
			case 25 :
				// CosmosSQL.g:1:180: PLUS
				{
				mPLUS(); 

				}
				break;
			case 26 :
				// CosmosSQL.g:1:185: MINUS
				{
				mMINUS(); 

				}
				break;
			case 27 :
				// CosmosSQL.g:1:191: NEGATION
				{
				mNEGATION(); 

				}
				break;
			case 28 :
				// CosmosSQL.g:1:200: VERTBAR
				{
				mVERTBAR(); 

				}
				break;
			case 29 :
				// CosmosSQL.g:1:208: BITAND
				{
				mBITAND(); 

				}
				break;
			case 30 :
				// CosmosSQL.g:1:215: POWER_OP
				{
				mPOWER_OP(); 

				}
				break;
			case 31 :
				// CosmosSQL.g:1:224: GTH
				{
				mGTH(); 

				}
				break;
			case 32 :
				// CosmosSQL.g:1:228: LTH
				{
				mLTH(); 

				}
				break;
			case 33 :
				// CosmosSQL.g:1:232: INTEGER_NUM
				{
				mINTEGER_NUM(); 

				}
				break;
			case 34 :
				// CosmosSQL.g:1:244: HEX_DIGIT
				{
				mHEX_DIGIT(); 

				}
				break;
			case 35 :
				// CosmosSQL.g:1:254: BIT_NUM
				{
				mBIT_NUM(); 

				}
				break;
			case 36 :
				// CosmosSQL.g:1:262: REAL_NUMBER
				{
				mREAL_NUMBER(); 

				}
				break;
			case 37 :
				// CosmosSQL.g:1:274: TEXT_STRING
				{
				mTEXT_STRING(); 

				}
				break;
			case 38 :
				// CosmosSQL.g:1:286: ID
				{
				mID(); 

				}
				break;
			case 39 :
				// CosmosSQL.g:1:289: Space
				{
				mSpace(); 

				}
				break;

		}
	}


	protected DFA14 dfa14 = new DFA14(this);
	protected DFA18 dfa18 = new DFA18(this);
	protected DFA19 dfa19 = new DFA19(this);
	protected DFA22 dfa22 = new DFA22(this);
	static final String DFA14_eotS =
		"\1\uffff\1\3\2\uffff\1\5\2\uffff";
	static final String DFA14_eofS =
		"\7\uffff";
	static final String DFA14_minS =
		"\2\56\2\uffff\1\60\2\uffff";
	static final String DFA14_maxS =
		"\2\71\2\uffff\1\71\2\uffff";
	static final String DFA14_acceptS =
		"\2\uffff\1\3\1\4\1\uffff\1\2\1\1";
	static final String DFA14_specialS =
		"\7\uffff}>";
	static final String[] DFA14_transitionS = {
			"\1\2\1\uffff\12\1",
			"\1\4\1\uffff\12\1",
			"",
			"",
			"\12\6",
			"",
			""
	};

	static final short[] DFA14_eot = DFA.unpackEncodedString(DFA14_eotS);
	static final short[] DFA14_eof = DFA.unpackEncodedString(DFA14_eofS);
	static final char[] DFA14_min = DFA.unpackEncodedStringToUnsignedChars(DFA14_minS);
	static final char[] DFA14_max = DFA.unpackEncodedStringToUnsignedChars(DFA14_maxS);
	static final short[] DFA14_accept = DFA.unpackEncodedString(DFA14_acceptS);
	static final short[] DFA14_special = DFA.unpackEncodedString(DFA14_specialS);
	static final short[][] DFA14_transition;

	static {
		int numStates = DFA14_transitionS.length;
		DFA14_transition = new short[numStates][];
		for (int i=0; i<numStates; i++) {
			DFA14_transition[i] = DFA.unpackEncodedString(DFA14_transitionS[i]);
		}
	}

	protected class DFA14 extends DFA {

		public DFA14(BaseRecognizer recognizer) {
			this.recognizer = recognizer;
			this.decisionNumber = 14;
			this.eot = DFA14_eot;
			this.eof = DFA14_eof;
			this.min = DFA14_min;
			this.max = DFA14_max;
			this.accept = DFA14_accept;
			this.special = DFA14_special;
			this.transition = DFA14_transition;
		}
		@Override
		public String getDescription() {
			return "137:2: ( INTEGER_NUM DOT INTEGER_NUM | INTEGER_NUM DOT | DOT INTEGER_NUM | INTEGER_NUM )";
		}
	}

	static final String DFA18_eotS =
		"\1\uffff\1\5\5\uffff\1\3\1\11\1\uffff\1\3";
	static final String DFA18_eofS =
		"\13\uffff";
	static final String DFA18_minS =
		"\1\0\1\47\1\0\4\uffff\2\0\1\uffff\1\0";
	static final String DFA18_maxS =
		"\1\uffff\1\47\1\uffff\4\uffff\2\uffff\1\uffff\1\uffff";
	static final String DFA18_acceptS =
		"\3\uffff\1\4\1\2\1\5\1\1\2\uffff\1\3\1\uffff";
	static final String DFA18_specialS =
		"\1\4\1\uffff\1\1\4\uffff\1\0\1\3\1\uffff\1\2}>";
	static final String[] DFA18_transitionS = {
			"\47\3\1\1\64\3\1\2\uffa3\3",
			"\1\4",
			"\47\3\1\7\64\3\1\6\uffa3\3",
			"",
			"",
			"",
			"",
			"\47\11\1\10\uffd8\11",
			"\47\3\1\12\uffd8\3",
			"",
			"\47\11\1\10\uffd8\11"
	};

	static final short[] DFA18_eot = DFA.unpackEncodedString(DFA18_eotS);
	static final short[] DFA18_eof = DFA.unpackEncodedString(DFA18_eofS);
	static final char[] DFA18_min = DFA.unpackEncodedStringToUnsignedChars(DFA18_minS);
	static final char[] DFA18_max = DFA.unpackEncodedStringToUnsignedChars(DFA18_maxS);
	static final short[] DFA18_accept = DFA.unpackEncodedString(DFA18_acceptS);
	static final short[] DFA18_special = DFA.unpackEncodedString(DFA18_specialS);
	static final short[][] DFA18_transition;

	static {
		int numStates = DFA18_transitionS.length;
		DFA18_transition = new short[numStates][];
		for (int i=0; i<numStates; i++) {
			DFA18_transition[i] = DFA.unpackEncodedString(DFA18_transitionS[i]);
		}
	}

	protected class DFA18 extends DFA {

		public DFA18(BaseRecognizer recognizer) {
			this.recognizer = recognizer;
			this.decisionNumber = 18;
			this.eot = DFA18_eot;
			this.eof = DFA18_eof;
			this.min = DFA18_min;
			this.max = DFA18_max;
			this.accept = DFA18_accept;
			this.special = DFA18_special;
			this.transition = DFA18_transition;
		}
		@Override
		public String getDescription() {
			return "()* loopback of 144:11: ( ( '\\\\' '\\\\' ) | ( '\\'' '\\'' ) | ( '\\\\' '\\'' ) |~ ( '\\'' ) )*";
		}
		@Override
		public int specialStateTransition(int s, IntStream _input) throws NoViableAltException {
			IntStream input = _input;
			int _s = s;
			switch ( s ) {
					case 0 : 
						int LA18_7 = input.LA(1);
						s = -1;
						if ( (LA18_7=='\'') ) {s = 8;}
						else if ( ((LA18_7 >= '\u0000' && LA18_7 <= '&')||(LA18_7 >= '(' && LA18_7 <= '\uFFFF')) ) {s = 9;}
						else s = 3;
						if ( s>=0 ) return s;
						break;

					case 1 : 
						int LA18_2 = input.LA(1);
						s = -1;
						if ( (LA18_2=='\\') ) {s = 6;}
						else if ( (LA18_2=='\'') ) {s = 7;}
						else if ( ((LA18_2 >= '\u0000' && LA18_2 <= '&')||(LA18_2 >= '(' && LA18_2 <= '[')||(LA18_2 >= ']' && LA18_2 <= '\uFFFF')) ) {s = 3;}
						if ( s>=0 ) return s;
						break;

					case 2 : 
						int LA18_10 = input.LA(1);
						s = -1;
						if ( (LA18_10=='\'') ) {s = 8;}
						else if ( ((LA18_10 >= '\u0000' && LA18_10 <= '&')||(LA18_10 >= '(' && LA18_10 <= '\uFFFF')) ) {s = 9;}
						else s = 3;
						if ( s>=0 ) return s;
						break;

					case 3 : 
						int LA18_8 = input.LA(1);
						s = -1;
						if ( (LA18_8=='\'') ) {s = 10;}
						else if ( ((LA18_8 >= '\u0000' && LA18_8 <= '&')||(LA18_8 >= '(' && LA18_8 <= '\uFFFF')) ) {s = 3;}
						else s = 9;
						if ( s>=0 ) return s;
						break;

					case 4 : 
						int LA18_0 = input.LA(1);
						s = -1;
						if ( (LA18_0=='\'') ) {s = 1;}
						else if ( (LA18_0=='\\') ) {s = 2;}
						else if ( ((LA18_0 >= '\u0000' && LA18_0 <= '&')||(LA18_0 >= '(' && LA18_0 <= '[')||(LA18_0 >= ']' && LA18_0 <= '\uFFFF')) ) {s = 3;}
						if ( s>=0 ) return s;
						break;
			}
			NoViableAltException nvae =
				new NoViableAltException(getDescription(), 18, _s, input);
			error(nvae);
			throw nvae;
		}
	}

	static final String DFA19_eotS =
		"\1\uffff\1\5\5\uffff\1\3\1\11\1\uffff\1\3";
	static final String DFA19_eofS =
		"\13\uffff";
	static final String DFA19_minS =
		"\1\0\1\42\1\0\4\uffff\2\0\1\uffff\1\0";
	static final String DFA19_maxS =
		"\1\uffff\1\42\1\uffff\4\uffff\2\uffff\1\uffff\1\uffff";
	static final String DFA19_acceptS =
		"\3\uffff\1\4\1\2\1\5\1\1\2\uffff\1\3\1\uffff";
	static final String DFA19_specialS =
		"\1\0\1\uffff\1\2\4\uffff\1\4\1\1\1\uffff\1\3}>";
	static final String[] DFA19_transitionS = {
			"\42\3\1\1\71\3\1\2\uffa3\3",
			"\1\4",
			"\42\3\1\7\71\3\1\6\uffa3\3",
			"",
			"",
			"",
			"",
			"\42\11\1\10\uffdd\11",
			"\42\3\1\12\uffdd\3",
			"",
			"\42\11\1\10\uffdd\11"
	};

	static final short[] DFA19_eot = DFA.unpackEncodedString(DFA19_eotS);
	static final short[] DFA19_eof = DFA.unpackEncodedString(DFA19_eofS);
	static final char[] DFA19_min = DFA.unpackEncodedStringToUnsignedChars(DFA19_minS);
	static final char[] DFA19_max = DFA.unpackEncodedStringToUnsignedChars(DFA19_maxS);
	static final short[] DFA19_accept = DFA.unpackEncodedString(DFA19_acceptS);
	static final short[] DFA19_special = DFA.unpackEncodedString(DFA19_specialS);
	static final short[][] DFA19_transition;

	static {
		int numStates = DFA19_transitionS.length;
		DFA19_transition = new short[numStates][];
		for (int i=0; i<numStates; i++) {
			DFA19_transition[i] = DFA.unpackEncodedString(DFA19_transitionS[i]);
		}
	}

	protected class DFA19 extends DFA {

		public DFA19(BaseRecognizer recognizer) {
			this.recognizer = recognizer;
			this.decisionNumber = 19;
			this.eot = DFA19_eot;
			this.eof = DFA19_eof;
			this.min = DFA19_min;
			this.max = DFA19_max;
			this.accept = DFA19_accept;
			this.special = DFA19_special;
			this.transition = DFA19_transition;
		}
		@Override
		public String getDescription() {
			return "()* loopback of 146:11: ( ( '\\\\' '\\\\' ) | ( '\\\"' '\\\"' ) | ( '\\\\' '\\\"' ) |~ ( '\\\"' ) )*";
		}
		@Override
		public int specialStateTransition(int s, IntStream _input) throws NoViableAltException {
			IntStream input = _input;
			int _s = s;
			switch ( s ) {
					case 0 : 
						int LA19_0 = input.LA(1);
						s = -1;
						if ( (LA19_0=='\"') ) {s = 1;}
						else if ( (LA19_0=='\\') ) {s = 2;}
						else if ( ((LA19_0 >= '\u0000' && LA19_0 <= '!')||(LA19_0 >= '#' && LA19_0 <= '[')||(LA19_0 >= ']' && LA19_0 <= '\uFFFF')) ) {s = 3;}
						if ( s>=0 ) return s;
						break;

					case 1 : 
						int LA19_8 = input.LA(1);
						s = -1;
						if ( (LA19_8=='\"') ) {s = 10;}
						else if ( ((LA19_8 >= '\u0000' && LA19_8 <= '!')||(LA19_8 >= '#' && LA19_8 <= '\uFFFF')) ) {s = 3;}
						else s = 9;
						if ( s>=0 ) return s;
						break;

					case 2 : 
						int LA19_2 = input.LA(1);
						s = -1;
						if ( (LA19_2=='\\') ) {s = 6;}
						else if ( (LA19_2=='\"') ) {s = 7;}
						else if ( ((LA19_2 >= '\u0000' && LA19_2 <= '!')||(LA19_2 >= '#' && LA19_2 <= '[')||(LA19_2 >= ']' && LA19_2 <= '\uFFFF')) ) {s = 3;}
						if ( s>=0 ) return s;
						break;

					case 3 : 
						int LA19_10 = input.LA(1);
						s = -1;
						if ( (LA19_10=='\"') ) {s = 8;}
						else if ( ((LA19_10 >= '\u0000' && LA19_10 <= '!')||(LA19_10 >= '#' && LA19_10 <= '\uFFFF')) ) {s = 9;}
						else s = 3;
						if ( s>=0 ) return s;
						break;

					case 4 : 
						int LA19_7 = input.LA(1);
						s = -1;
						if ( (LA19_7=='\"') ) {s = 8;}
						else if ( ((LA19_7 >= '\u0000' && LA19_7 <= '!')||(LA19_7 >= '#' && LA19_7 <= '\uFFFF')) ) {s = 9;}
						else s = 3;
						if ( s>=0 ) return s;
						break;
			}
			NoViableAltException nvae =
				new NoViableAltException(getDescription(), 19, _s, input);
			error(nvae);
			throw nvae;
		}
	}

	static final String DFA22_eotS =
		"\1\uffff\3\43\1\uffff\1\43\1\uffff\1\43\1\53\1\43\1\56\1\60\1\63\1\uffff"+
		"\1\64\1\65\1\70\1\72\1\74\11\uffff\2\100\4\43\3\uffff\4\43\1\52\2\uffff"+
		"\1\43\4\uffff\1\107\17\uffff\3\43\1\4\1\6\1\55\1\uffff\1\43\1\114\2\43"+
		"\1\uffff\2\43\1\120\1\uffff";
	static final String DFA22_eofS =
		"\121\uffff";
	static final String DFA22_minS =
		"\1\40\1\110\1\101\1\111\1\uffff\1\117\1\uffff\1\122\1\174\1\116\1\46\1"+
		"\76\1\74\1\uffff\4\75\1\52\11\uffff\2\56\2\47\1\42\1\125\3\uffff\1\117"+
		"\1\102\1\126\1\104\1\44\2\uffff\1\104\4\uffff\1\76\17\uffff\1\124\1\127"+
		"\1\114\3\44\1\uffff\1\106\1\44\1\105\1\70\1\uffff\1\123\1\42\1\44\1\uffff";
	static final String DFA22_maxS =
		"\1\176\1\150\1\141\1\151\1\uffff\1\157\1\uffff\1\162\1\174\1\156\1\46"+
		"\2\76\1\uffff\2\75\1\76\1\75\1\71\11\uffff\1\170\1\145\3\47\1\165\3\uffff"+
		"\1\157\1\142\1\166\1\144\1\172\2\uffff\1\144\4\uffff\1\76\17\uffff\1\164"+
		"\1\167\1\154\3\172\1\uffff\1\146\1\172\1\145\1\70\1\uffff\1\163\1\47\1"+
		"\172\1\uffff";
	static final String DFA22_acceptS =
		"\4\uffff\1\3\1\uffff\1\4\6\uffff\1\11\5\uffff\1\20\1\23\1\24\1\25\1\26"+
		"\1\27\1\30\1\31\1\32\6\uffff\1\45\1\46\1\47\5\uffff\1\5\1\34\1\uffff\1"+
		"\6\1\35\1\7\1\10\1\uffff\1\15\1\40\1\33\1\36\1\13\1\16\1\37\1\14\1\21"+
		"\1\17\1\22\1\44\1\42\1\43\1\41\6\uffff\1\12\4\uffff\1\1\3\uffff\1\2";
	static final String DFA22_specialS =
		"\121\uffff}>";
	static final String[] DFA22_transitionS = {
			"\1\44\1\15\1\42\1\uffff\1\43\1\6\1\12\1\42\1\27\1\26\1\25\1\32\1\24\1"+
			"\33\1\22\1\4\1\34\11\35\1\21\1\23\1\14\1\13\1\20\2\uffff\1\11\1\37\1"+
			"\43\1\3\10\43\1\5\1\40\1\7\3\43\1\1\1\2\3\43\1\36\2\43\1\31\1\uffff\1"+
			"\30\1\17\1\41\1\uffff\1\11\1\37\1\43\1\3\10\43\1\5\1\40\1\7\3\43\1\1"+
			"\1\2\6\43\1\uffff\1\10\1\uffff\1\16",
			"\1\45\37\uffff\1\45",
			"\1\46\37\uffff\1\46",
			"\1\47\37\uffff\1\47",
			"",
			"\1\50\37\uffff\1\50",
			"",
			"\1\51\37\uffff\1\51",
			"\1\52",
			"\1\54\37\uffff\1\54",
			"\1\55",
			"\1\57",
			"\1\62\1\61\1\15",
			"",
			"\1\15",
			"\1\15",
			"\1\66\1\67",
			"\1\71",
			"\1\73\5\uffff\12\75",
			"",
			"",
			"",
			"",
			"",
			"",
			"",
			"",
			"",
			"\1\75\1\uffff\12\35\13\uffff\1\75\34\uffff\1\77\2\uffff\1\75\22\uffff"+
			"\1\76",
			"\1\75\1\uffff\12\35\13\uffff\1\75\37\uffff\1\75",
			"\1\76",
			"\1\77",
			"\1\42\4\uffff\1\42",
			"\1\101\37\uffff\1\101",
			"",
			"",
			"",
			"\1\102\37\uffff\1\102",
			"\1\103\37\uffff\1\103",
			"\1\104\37\uffff\1\104",
			"\1\105\37\uffff\1\105",
			"\1\43\13\uffff\12\43\7\uffff\32\43\4\uffff\1\43\1\uffff\32\43",
			"",
			"",
			"\1\106\37\uffff\1\106",
			"",
			"",
			"",
			"",
			"\1\60",
			"",
			"",
			"",
			"",
			"",
			"",
			"",
			"",
			"",
			"",
			"",
			"",
			"",
			"",
			"",
			"\1\110\37\uffff\1\110",
			"\1\111\37\uffff\1\111",
			"\1\112\37\uffff\1\112",
			"\1\43\13\uffff\12\43\7\uffff\32\43\4\uffff\1\43\1\uffff\32\43",
			"\1\43\13\uffff\12\43\7\uffff\32\43\4\uffff\1\43\1\uffff\32\43",
			"\1\43\13\uffff\12\43\7\uffff\32\43\4\uffff\1\43\1\uffff\32\43",
			"",
			"\1\113\37\uffff\1\113",
			"\1\43\13\uffff\12\43\7\uffff\32\43\4\uffff\1\43\1\uffff\32\43",
			"\1\115\37\uffff\1\115",
			"\1\116",
			"",
			"\1\117\37\uffff\1\117",
			"\1\42\4\uffff\1\42",
			"\1\43\13\uffff\12\43\7\uffff\32\43\4\uffff\1\43\1\uffff\32\43",
			""
	};

	static final short[] DFA22_eot = DFA.unpackEncodedString(DFA22_eotS);
	static final short[] DFA22_eof = DFA.unpackEncodedString(DFA22_eofS);
	static final char[] DFA22_min = DFA.unpackEncodedStringToUnsignedChars(DFA22_minS);
	static final char[] DFA22_max = DFA.unpackEncodedStringToUnsignedChars(DFA22_maxS);
	static final short[] DFA22_accept = DFA.unpackEncodedString(DFA22_acceptS);
	static final short[] DFA22_special = DFA.unpackEncodedString(DFA22_specialS);
	static final short[][] DFA22_transition;

	static {
		int numStates = DFA22_transitionS.length;
		DFA22_transition = new short[numStates][];
		for (int i=0; i<numStates; i++) {
			DFA22_transition[i] = DFA.unpackEncodedString(DFA22_transitionS[i]);
		}
	}

	protected class DFA22 extends DFA {

		public DFA22(BaseRecognizer recognizer) {
			this.recognizer = recognizer;
			this.decisionNumber = 22;
			this.eot = DFA22_eot;
			this.eof = DFA22_eof;
			this.min = DFA22_min;
			this.max = DFA22_max;
			this.accept = DFA22_accept;
			this.special = DFA22_special;
			this.transition = DFA22_transition;
		}
		@Override
		public String getDescription() {
			return "1:1: Tokens : ( SHOW | TABLES | DIVIDE | MOD_SYM | OR_SYM | AND_SYM | ARROW | EQ_SYM | NOT_EQ | LET | GET | SET_VAR | SHIFT_LEFT | SHIFT_RIGHT | ALL_FIELDS | SEMI | COLON | DOT | COMMA | ASTERISK | RPAREN | LPAREN | RBRACK | LBRACK | PLUS | MINUS | NEGATION | VERTBAR | BITAND | POWER_OP | GTH | LTH | INTEGER_NUM | HEX_DIGIT | BIT_NUM | REAL_NUMBER | TEXT_STRING | ID | Space );";
		}
	}

}
