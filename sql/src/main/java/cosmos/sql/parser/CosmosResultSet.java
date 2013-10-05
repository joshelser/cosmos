// $ANTLR 3.5.1 CosmosResultSet.g 2013-10-04 21:12:39

package cosmos.sql.parser;
import net.hydromatic.optiq.jdbc.ShowTableExp;

import org.antlr.runtime.*;
import org.antlr.runtime.tree.*;

import cosmos.sql.rules.ResultSetRule;


import java.util.Stack;
import java.util.List;
import java.util.ArrayList;

@SuppressWarnings("all")
public class CosmosResultSet extends TreeParser {
	public static final String[] tokenNames = new String[] {
		"<invalid>", "<EOR>", "<DOWN>", "<UP>", "ALL_FIELDS", "AND_SYM", "ARROW", 
		"ASTERISK", "A_", "BITAND", "BIT_NUM", "B_", "COLON", "COMMA", "C_", "DIVIDE", 
		"DOT", "D_", "EQ_SYM", "E_", "F_", "GET", "GTH", "G_", "HEX_DIGIT", "HEX_DIGIT_FRAGMENT", 
		"H_", "ID", "INTEGER_NUM", "I_", "J_", "K_", "LBRACK", "LET", "LPAREN", 
		"LTH", "L_", "MINUS", "MOD_SYM", "M_", "NEGATION", "NOT_EQ", "N_", "OR_SYM", 
		"O_", "PLUS", "POWER_OP", "P_", "Q_", "RBRACK", "REAL_NUMBER", "RPAREN", 
		"R_", "SEMI", "SET_VAR", "SHIFT_LEFT", "SHIFT_RIGHT", "SHOW", "S_", "Space", 
		"TABLES", "TEXT_STRING", "T_", "U_", "VERTBAR", "V_", "W_", "X_", "Y_", 
		"Z_"
	};
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

	// delegates
	public TreeParser[] getDelegates() {
		return new TreeParser[] {};
	}

	// delegators


	public CosmosResultSet(TreeNodeStream input) {
		this(input, new RecognizerSharedState());
	}
	public CosmosResultSet(TreeNodeStream input, RecognizerSharedState state) {
		super(input, state);
	}

	@Override public String[] getTokenNames() { return CosmosResultSet.tokenNames; }
	@Override public String getGrammarFileName() { return "CosmosResultSet.g"; }



	// $ANTLR start "cosmos_specific"
	// CosmosResultSet.g:9:1: cosmos_specific returns [ResultSetNode e] : supported_statements ;
	public final ResultSetRule cosmos_specific() throws RecognitionException {
		ResultSetRule e = null;


		ResultSetRule supported_statements1 =null;

		try {
			// CosmosResultSet.g:10:4: ( supported_statements )
			// CosmosResultSet.g:10:6: supported_statements
			{
			pushFollow(FOLLOW_supported_statements_in_cosmos_specific42);
			supported_statements1=supported_statements();
			state._fsp--;

			 e = supported_statements1;
			}

		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
		}
		finally {
			// do for sure before leaving
		}
		return e;
	}
	// $ANTLR end "cosmos_specific"



	// $ANTLR start "show_tables"
	// CosmosResultSet.g:13:1: show_tables : SHOW TABLES ;
	public final void show_tables() throws RecognitionException {
		try {
			// CosmosResultSet.g:13:12: ( SHOW TABLES )
			// CosmosResultSet.g:14:2: SHOW TABLES
			{
			match(input,SHOW,FOLLOW_SHOW_in_show_tables52); 
			match(input,TABLES,FOLLOW_TABLES_in_show_tables54); 
			}

		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "show_tables"



	// $ANTLR start "supported_statements"
	// CosmosResultSet.g:18:1: supported_statements returns [ResultSetNode e] : show_tables ;
	public final ResultSetRule supported_statements() throws RecognitionException {
		ResultSetRule e = null;


		try {
			// CosmosResultSet.g:19:4: ( show_tables )
			// CosmosResultSet.g:19:6: show_tables
			{
			pushFollow(FOLLOW_show_tables_in_supported_statements71);
			show_tables();
			state._fsp--;

			 e = new ShowTableExp(); 
			}

		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
		}
		finally {
			// do for sure before leaving
		}
		return e;
	}
	// $ANTLR end "supported_statements"

	// Delegated rules



	public static final BitSet FOLLOW_supported_statements_in_cosmos_specific42 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_SHOW_in_show_tables52 = new BitSet(new long[]{0x1000000000000000L});
	public static final BitSet FOLLOW_TABLES_in_show_tables54 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_show_tables_in_supported_statements71 = new BitSet(new long[]{0x0000000000000002L});
}
